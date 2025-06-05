import ray
from typing import Dict, Any
from deltacat.types.media import ContentType
import pyarrow as pa

import pytest
import deltacat.tests.local_deltacat_storage as ds
import os
from deltacat.tests.test_utils.pyarrow import (
    stage_partition_from_file_paths,
    commit_delta_to_staged_partition,
)
from deltacat.utils.pyarrow import (
    ReadKwargsProviderPyArrowCsvPureUtf8,
    ReadKwargsProviderPyArrowSchemaOverride,
)

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)


class TestContentTypeParams:
    TEST_NAMESPACE = "test_content_type_params"
    TEST_ENTRY_INDEX = 0
    DEDUPE_BASE_COMPACTED_TABLE_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_base_compacted_table_string_pk.csv"
    DEDUPE_NO_DUPLICATION_STRING_PK = "deltacat/tests/compute/compactor_v2/steps/data/dedupe_table_no_duplication_string_pk.csv"

    @pytest.fixture(scope="module", autouse=True)
    def setup_ray_cluster(self):
        ray.init(local_mode=True, ignore_reinit_error=True)
        yield
        ray.shutdown()

    @pytest.fixture(scope="function")
    def local_deltacat_storage_kwargs(self, request: pytest.FixtureRequest):
        # see deltacat/tests/local_deltacat_storage/README.md for documentation
        kwargs_for_local_deltacat_storage: Dict[str, Any] = {
            DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
        }
        yield kwargs_for_local_deltacat_storage
        if os.path.exists(DATABASE_FILE_PATH_VALUE):
            os.remove(DATABASE_FILE_PATH_VALUE)

    def test__download_parquet_metadata_for_manifest_entry_sanity(
        self, local_deltacat_storage_kwargs
    ):
        from deltacat.compute.compactor_v2.utils.content_type_params import (
            _download_parquet_metadata_for_manifest_entry,
        )
        from deltacat.types.partial_download import PartialParquetParameters

        partition = stage_partition_from_file_paths(
            self.TEST_NAMESPACE,
            [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK],
            **local_deltacat_storage_kwargs,
        )
        test_delta = commit_delta_to_staged_partition(
            partition,
            [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK],
            **local_deltacat_storage_kwargs,
        )
        test_entry_index = 0
        obj_ref = _download_parquet_metadata_for_manifest_entry.remote(
            test_delta, test_entry_index, ds, local_deltacat_storage_kwargs
        )
        parquet_metadata = ray.get(obj_ref)
        partial_parquet_params = parquet_metadata["partial_parquet_params"]

        # validate
        assert isinstance(parquet_metadata, dict)
        assert "entry_index" in parquet_metadata
        assert "partial_parquet_params" in parquet_metadata
        assert parquet_metadata["entry_index"] == test_entry_index
        assert isinstance(partial_parquet_params, PartialParquetParameters)

        assert partial_parquet_params.row_groups_to_download == [0]
        assert partial_parquet_params.num_row_groups == 1
        assert partial_parquet_params.num_rows == 8
        assert isinstance(partial_parquet_params.in_memory_size_bytes, float)
        assert partial_parquet_params.in_memory_size_bytes > 0

        pq_metadata = partial_parquet_params.pq_metadata
        assert pq_metadata.num_columns == 2
        assert pq_metadata.num_rows == 8
        assert pq_metadata.num_row_groups == 1
        assert pq_metadata.format_version == "2.6"

        assert (
            test_delta.manifest.entries[self.TEST_ENTRY_INDEX].meta.content_type
            == ContentType.PARQUET.value
        )

    @pytest.mark.parametrize(
        "read_kwargs_provider,expected_values",
        [
            (
                ReadKwargsProviderPyArrowCsvPureUtf8(),
                {
                    "num_rows": 6,
                    "num_columns": 2,
                    "num_row_groups": 1,
                    "format_version": "2.6",
                    "column_types": [pa.string(), pa.string()],
                },
            ),
            (
                ReadKwargsProviderPyArrowSchemaOverride(
                    schema=pa.schema(
                        [
                            ("id", pa.string()),
                            ("value", pa.int64()),
                        ]
                    )
                ),
                {
                    "num_rows": 6,
                    "num_columns": 2,
                    "num_row_groups": 1,
                    "format_version": "2.6",
                    "column_types": [pa.string(), pa.int64()],
                },
            ),
            (
                ReadKwargsProviderPyArrowSchemaOverride(
                    schema=None,
                    pq_coerce_int96_timestamp_unit="ms",
                    parquet_reader_type="daft",
                ),
                {
                    "num_rows": 6,
                    "num_columns": 2,
                    "num_row_groups": 1,
                    "format_version": "2.6",
                    "column_types": None,  # Will use default type inference
                },
            ),
        ],
    )
    def test__download_parquet_metadata_for_manifest_entry_with_read_kwargs_provider(
        self, read_kwargs_provider, expected_values, local_deltacat_storage_kwargs
    ):
        from deltacat.compute.compactor_v2.utils.content_type_params import (
            _download_parquet_metadata_for_manifest_entry,
        )

        partition = stage_partition_from_file_paths(
            self.TEST_NAMESPACE,
            [self.DEDUPE_NO_DUPLICATION_STRING_PK],
            **local_deltacat_storage_kwargs,
        )
        test_delta = commit_delta_to_staged_partition(
            partition,
            [self.DEDUPE_NO_DUPLICATION_STRING_PK],
            **local_deltacat_storage_kwargs,
        )
        test_entry_index = 0
        read_kwargs_provider = ReadKwargsProviderPyArrowCsvPureUtf8
        obj_ref = _download_parquet_metadata_for_manifest_entry.remote(
            test_delta,
            test_entry_index,
            ds,
            local_deltacat_storage_kwargs,
            read_kwargs_provider,
        )
        parquet_metadata = ray.get(obj_ref)
        partial_parquet_params = parquet_metadata["partial_parquet_params"]

        # validate
        assert isinstance(parquet_metadata, dict)
        assert "entry_index" in parquet_metadata
        assert "partial_parquet_params" in parquet_metadata
        assert parquet_metadata["entry_index"] == self.TEST_ENTRY_INDEX

        assert partial_parquet_params.row_groups_to_download == [0]
        assert (
            partial_parquet_params.num_row_groups == expected_values["num_row_groups"]
        )
        assert partial_parquet_params.num_rows == expected_values["num_rows"]
        assert isinstance(partial_parquet_params.in_memory_size_bytes, float)
        assert partial_parquet_params.in_memory_size_bytes > 0

        pq_metadata = partial_parquet_params.pq_metadata
        assert pq_metadata.num_columns == expected_values["num_columns"]
        assert pq_metadata.num_rows == expected_values["num_rows"]
        assert pq_metadata.num_row_groups == expected_values["num_row_groups"]
        assert pq_metadata.format_version == expected_values["format_version"]

        assert (
            test_delta.manifest.entries[self.TEST_ENTRY_INDEX].meta.content_type
            == ContentType.PARQUET.value
        )

    def test_download_parquet_metadata_for_manifest_entry_file_reader_kwargs_present_top_level_and_deltacat_storage_kwarg(
        self, local_deltacat_storage_kwargs, caplog
    ):
        from deltacat.compute.compactor_v2.utils.content_type_params import (
            _download_parquet_metadata_for_manifest_entry,
        )
        from deltacat.types.partial_download import PartialParquetParameters

        test_file_reader_kwargs_provider = ReadKwargsProviderPyArrowCsvPureUtf8()

        local_deltacat_storage_kwargs[
            "file_reader_kwargs_provider"
        ] = ReadKwargsProviderPyArrowCsvPureUtf8()

        partition = stage_partition_from_file_paths(
            self.TEST_NAMESPACE,
            [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK],
            **local_deltacat_storage_kwargs,
        )
        test_delta = commit_delta_to_staged_partition(
            partition,
            [self.DEDUPE_BASE_COMPACTED_TABLE_STRING_PK],
            **local_deltacat_storage_kwargs,
        )

        test_entry_index = 0
        obj_ref = _download_parquet_metadata_for_manifest_entry.remote(
            test_delta,
            test_entry_index,
            ds,
            local_deltacat_storage_kwargs,
            test_file_reader_kwargs_provider,
        )
        parquet_metadata = ray.get(obj_ref)
        partial_parquet_params = parquet_metadata["partial_parquet_params"]

        # validate
        assert isinstance(parquet_metadata, dict)
        assert "entry_index" in parquet_metadata
        assert "partial_parquet_params" in parquet_metadata
        assert parquet_metadata["entry_index"] == test_entry_index
        assert isinstance(partial_parquet_params, PartialParquetParameters)

        assert partial_parquet_params.row_groups_to_download == [0]
        assert partial_parquet_params.num_row_groups == 1
        assert partial_parquet_params.num_rows == 8
        assert isinstance(partial_parquet_params.in_memory_size_bytes, float)
        assert partial_parquet_params.in_memory_size_bytes > 0

        pq_metadata = partial_parquet_params.pq_metadata
        assert pq_metadata.num_columns == 2
        assert pq_metadata.num_rows == 8
        assert pq_metadata.num_row_groups == 1
        assert pq_metadata.format_version == "2.6"

        assert (
            test_delta.manifest.entries[self.TEST_ENTRY_INDEX].meta.content_type
            == ContentType.PARQUET.value
        )
