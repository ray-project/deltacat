import pytest
import shutil
import tempfile

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.writer.memtable_dataset_writer import (
    MemtableDatasetWriter,
)
from ..test_utils import (
    write_mvp_table,
    mvp_table_to_record_batches,
    validate_with_full_scan,
    create_dataset_for_method,
    assert_data_file_extension,
)

MemtableDatasetWriter.MAX_ROW_SIZE = 100


class TestWriter:
    temp_dir = None
    file_store: FileStore

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        path, filesystem = FileStore.filesystem(cls.temp_dir)
        cls.file_store = FileStore(path, filesystem)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)
        pass

    def test_write_unsupported_data_type(self, ds1_dataset, ds1_schema):
        dataset = create_dataset_for_method(self.temp_dir)
        dataset.add_schema(ds1_schema, "ds1_schema")
        with dataset.writer("ds1_schema") as writer:
            with pytest.raises(ValueError):
                writer.write("a string")

    def test_write_pydict(self, ds1_dataset, ds1_schema):
        dataset = create_dataset_for_method(self.temp_dir)
        dataset.add_schema(ds1_schema, "ds1_schema")
        with dataset.writer("ds1_schema") as writer:
            write_mvp_table(writer, ds1_dataset)

        validate_with_full_scan(dataset, ds1_dataset, ds1_schema)

    def test_write_record_batch(self, ds1_dataset, ds1_schema):
        dataset = create_dataset_for_method(self.temp_dir)
        dataset.add_schema(ds1_schema, "ds1_schema")
        with dataset.writer("ds1_schema") as writer:
            record_batch = mvp_table_to_record_batches(ds1_dataset, ds1_schema)
            writer.write(record_batch)

        validate_with_full_scan(dataset, ds1_dataset, ds1_schema)

    def test_write_list_of_record_batch(self, ds1_dataset, ds1_schema):
        dataset = create_dataset_for_method(self.temp_dir)
        dataset.add_schema(ds1_schema, "ds1_schema")
        with dataset.writer("ds1_schema", "feather") as writer:
            record_batch = mvp_table_to_record_batches(ds1_dataset, ds1_schema)
            writer.write([record_batch])

        validate_with_full_scan(dataset, ds1_dataset, ds1_schema)
        assert_data_file_extension(dataset, ".feather")

    def test_write_feather(self, dataset_images_with_label):
        dataset = create_dataset_for_method(self.temp_dir)

        table, schema = dataset_images_with_label
        dataset.add_schema(schema, "schema")
        with dataset.writer("schema", "feather") as writer:
            record_batch = mvp_table_to_record_batches(table, schema)
            writer.write([record_batch])

        validate_with_full_scan(dataset, table, schema)
        assert_data_file_extension(dataset, "feather")
