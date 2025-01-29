import math
import shutil
import tempfile
from typing import Dict, List, Iterator
import msgpack

import pytest
from pyarrow import RecordBatch

from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.delta import (
    ManifestIO,
    TreeLevel,
    DeltacatManifestIO,
)

from deltacat.storage.rivulet.mvp.Table import MvpTable, MvpRow
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.writer.memtable_dataset_writer import (
    MemtableDatasetWriter,
)

from deltacat.tests.storage.rivulet.test_utils import FIXTURE_ROW_COUNT
from deltacat.tests.storage.rivulet.test_utils import (
    write_mvp_table,
    compare_mvp_table_to_scan_results,
    mvp_table_to_record_batches,
    validate_with_full_scan,
    assert_data_file_extension_set,
    create_dataset_for_method,
)

MemtableDatasetWriter.MAX_ROW_SIZE = 100


class TestBasicEndToEnd:
    temp_dir = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.dataset: Dataset = Dataset(dataset_name="test", metadata_uri=cls.temp_dir)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)
        pass

    @pytest.fixture
    def ds1_schema(self, ds1_schema: Schema, ds1_dataset: MvpTable):
        self.dataset.add_schema(ds1_schema, "ds1_schema")
        with self.dataset.writer("ds1_schema") as writer:
            write_mvp_table(writer, ds1_dataset)
        return ds1_schema

    def test_end_to_end_scan_pydict(self, ds1_schema, ds1_dataset):
        # Read out dataset written to ds1_schema fixture, with full scan
        read_records: List[Dict] = list(
            self.dataset.scan(QueryExpression()).to_pydict()
        )  # compare all_records to ds1
        compare_mvp_table_to_scan_results(
            ds1_dataset, read_records, ds1_schema.get_merge_key()
        )

    def test_end_to_end_scan_key_range(self, ds1_schema, ds1_dataset):
        read_records_range: List[Dict] = list(
            self.dataset.scan(QueryExpression().with_range(100, 500)).to_pydict()
        )
        assert len(read_records_range) == 401

    def test_end_to_end_scan_single_key(self, ds1_schema, ds1_dataset):
        read_records_single_key: List[Dict] = list(
            self.dataset.scan(QueryExpression().with_key(600)).to_pydict()
        )
        assert len(read_records_single_key) == 1
        assert read_records_single_key[0]["id"] == 600

    def test_end_to_end_scan_pyarrow(self, ds1_schema, ds1_dataset):
        batches: Iterator[RecordBatch] = self.dataset.scan(QueryExpression()).to_arrow()
        read_records = [record for batch in batches for record in batch.to_pylist()]
        compare_mvp_table_to_scan_results(
            ds1_dataset, read_records, ds1_schema.get_merge_key()
        )


class TestMultiLayerCompactionEndToEnd:
    """Tests the merge-on-read compaction

    The priority of records with the same primary key should go as follows:

    1. Prioritize higher layers over lower layers
    1. Prioritize newer SSTs over older SSTs (which is really only relevant for L0)

    To this end, we'll create 4 manifests (in order of oldest to newest):
    1. L0 manifest A (oldest perhaps because of compaction) with ids {x}
    1. L1 manifest B with ids {x} and {y}
    1. L1 manifest C with ids {y}
    1. L2 manifest D with ids {y} (technically not required for this demonstration)

    The output dataset should contain:
    - {x} from manifest A (since it's at a higher layer than manifest B)
    - {y} from manifest C (since it's newer than manifest B)
    """

    temp_dir = None
    file_store: FileStore
    manifest_io: ManifestIO

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        path, filesystem = FileStore.filesystem(cls.temp_dir)
        cls.dataset: Dataset = Dataset(dataset_name="test", metadata_uri=path)
        cls.file_store = cls.dataset._file_store
        cls.manifest_io = DeltacatManifestIO(cls.temp_dir, cls.dataset._locator)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    @pytest.fixture
    def l0_overwrite(self, ds1_dataset):
        """Transform the 2nd half of the records"""
        return self._transform_dataset(
            ds1_dataset,
            math.floor(FIXTURE_ROW_COUNT / 2),
            FIXTURE_ROW_COUNT,
            transform_name=lambda x: "overwritten",
            transform_age=lambda x: None,
        )

    @pytest.fixture
    def l1_overwrite(self, ds1_dataset):
        """Transform the 1st half of the records"""
        return self._transform_dataset(
            ds1_dataset,
            0,
            math.floor(FIXTURE_ROW_COUNT / 2),
            transform_name=lambda x: "overwritten",
        )

    @pytest.fixture
    def l2_ignored(self, ds1_dataset):
        """Transform the 1st half of the records"""
        return self._transform_dataset(
            ds1_dataset,
            0,
            math.floor(FIXTURE_ROW_COUNT / 2),
            transform_name=lambda x: "ignored",
        )

    def _transform_dataset(
        self,
        dataset,
        min_index=0,
        max_index=FIXTURE_ROW_COUNT,
        transform_id=lambda x: x,
        transform_name=lambda x: x,
        transform_age=lambda x: x,
    ):
        data = dataset.data
        return MvpTable(
            {
                "id": [transform_id(x) for x in data["id"][min_index:max_index]],
                "name": [transform_name(x) for x in data["name"][min_index:max_index]],
                "age": [transform_age(x) for x in data["age"][min_index:max_index]],
            }
        )

    @pytest.fixture
    def expected_dataset(self, l1_overwrite, l0_overwrite):
        return MvpTable(
            {
                "id": l1_overwrite.data["id"] + l0_overwrite.data["id"],
                "name": l1_overwrite.data["name"] + l0_overwrite.data["name"],
                "age": l1_overwrite.data["age"] + l0_overwrite.data["age"],
            }
        )

    @pytest.fixture
    def ds1_written_uri(
        self, ds1_schema, ds1_dataset, l2_ignored, l0_overwrite, l1_overwrite
    ):
        print(f"Writing test data to directory {self.temp_dir}")
        self.dataset.add_schema(ds1_schema, "ds1_schema")
        # oldest at L0 (should take precedence)
        self.write_dataset("ds1_schema", l0_overwrite)
        # original dataset (at L1)
        uri = self.write_dataset("ds1_schema", ds1_dataset)
        self.rewrite_at_level(uri, 1)
        # newer dataset at L1 (should take precedence)
        uri = self.write_dataset("ds1_schema", l1_overwrite)
        self.rewrite_at_level(uri, 1)
        # newer at L2 (loses out to L0 data)
        uri = self.write_dataset("ds1_schema", l2_ignored)
        self.rewrite_at_level(uri, 2)

    def test_end_to_end_scan(self, ds1_written_uri, ds1_schema, expected_dataset):
        """Rewrite entire dataset into 2nd manifest with same primary keys but "redacted" name."""
        read_records: List[Dict] = list(
            self.dataset.scan(QueryExpression()).to_pydict()
        )
        key = ds1_schema.get_merge_key()
        rows_by_key: Dict[str, MvpRow] = expected_dataset.to_rows_by_key(key)
        assert len(read_records) == len(rows_by_key)
        for record in read_records:
            pk_val = record[key]
            assert record == rows_by_key[pk_val].data

        # Test scan primary key range
        read_records_range: List[Dict] = list(
            self.dataset.scan(QueryExpression().with_range(100, 500)).to_pydict()
        )
        assert len(read_records_range) == 401

        # Test scan single primary key
        read_records_single_key: List[Dict] = list(
            self.dataset.scan(QueryExpression().with_key(600)).to_pydict()
        )
        assert len(read_records_single_key) == 1
        assert read_records_single_key[0]["id"] == 600

    def write_dataset(self, schema_name: str, dataset) -> str:
        ds1_writer = self.dataset.writer(schema_name)
        write_mvp_table(ds1_writer, dataset)
        return ds1_writer.flush()

    def rewrite_at_level(self, uri: str, level: TreeLevel):
        """Rewrite the given manifest with the new tree level

        TODO: replace this with a compaction operation
        """
        with open(uri, "rb") as f:
            data = msgpack.unpack(f)
            data["level"] = level

        with open(uri, "wb") as f:
            msgpack.pack(data, f)


class TestZipperMergeEndToEnd:
    temp_dir = None
    file_store: FileStore

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        path, filesystem = FileStore.filesystem(cls.temp_dir)
        cls.dataset: Dataset = Dataset(dataset_name="test", metadata_uri=cls.temp_dir)
        cls.file_store = FileStore(path, filesystem=filesystem)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    @pytest.fixture
    def schema1(self, ds1_dataset: MvpTable, ds1_schema: Schema):
        self.dataset.add_schema(ds1_schema, "ds1_schema")
        with self.dataset.writer("ds1_schema") as writer:
            write_mvp_table(writer, ds1_dataset)
        return ds1_schema

    @pytest.fixture
    def schema2(self, ds2_dataset: MvpTable, ds2_schema: Schema):
        self.dataset.add_schema(ds2_schema, "ds2_schema")
        with self.dataset.writer("ds2_schema") as writer:
            write_mvp_table(writer, ds2_dataset)
        return ds2_schema

    def test_end_to_end_scan(
        self,
        schema1,
        schema2,
        ds1_schema,
        ds1_dataset,
        ds2_dataset,
        ds2_schema,
        combined_schema,
    ):
        read_records: List[Dict] = list(
            self.dataset.scan(QueryExpression()).to_pydict()
        )

        merge_key = ds1_schema.get_merge_key()
        ds1_rows_by_pk: Dict[str, MvpRow] = ds1_dataset.to_rows_by_key(merge_key)
        ds2_rows_by_pk: Dict[str, MvpRow] = ds2_dataset.to_rows_by_key(merge_key)

        assert len(read_records) == len(ds1_rows_by_pk)
        for record in read_records:
            pk_val = record[merge_key]
            ds1_row = ds1_rows_by_pk[pk_val]
            ds2_row = ds2_rows_by_pk[pk_val]
            expected_merged_record = ds1_row.data | ds2_row.data
            assert expected_merged_record == record

        # Test scan primary key range
        read_records_range: List[Dict] = list(
            self.dataset.scan(QueryExpression().with_range(100, 500)).to_pydict()
        )
        assert len(read_records_range) == 401

        # Test scan single primary key
        read_records_single_key: List[Dict] = list(
            self.dataset.scan(QueryExpression().with_key(600)).to_pydict()
        )
        assert len(read_records_single_key) == 1
        assert read_records_single_key[0]["id"] == 600


class TestDataFormatSupport:
    temp_dir = None
    file_store: FileStore

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        path, filesystem = FileStore.filesystem(cls.temp_dir)
        cls.file_store = FileStore(path, filesystem=filesystem)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)
        pass

    # TODO expand coverage - below test is more like smoke test since dataset rows the same across types
    def test_mixed_content_dataset(self, dataset_images_with_label):
        dataset = create_dataset_for_method(self.temp_dir)
        table, schema = dataset_images_with_label
        dataset.add_schema(schema, "schema")
        with dataset.writer("schema", "feather") as writer:
            record_batch = mvp_table_to_record_batches(table, schema)
            writer.write([record_batch])

        with dataset.writer("schema", "parquet") as writer:
            record_batch = mvp_table_to_record_batches(table, schema)
            writer.write([record_batch])

        validate_with_full_scan(dataset, table, schema)
        assert_data_file_extension_set(dataset, {".feather", ".parquet"})
