import unittest
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.utils.daft import (
    daft_file_to_pyarrow_table,
    files_to_dataframe,
)
from deltacat.utils.pyarrow import ReadKwargsProviderPyArrowSchemaOverride
from deltacat.types.partial_download import PartialParquetParameters
import pyarrow as pa

from pyarrow import parquet as pq


class TestDaftFileToPyarrowTable(unittest.TestCase):
    MVP_PATH = "deltacat/tests/utils/data/mvp.parquet"

    def test_read_from_local_all_columns(self):
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
        )
        self.assertEqual(table.schema.names, ["a", "b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_via_include_columns(self):
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["b"],
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_via_column_names(self):
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            column_names=["b"],
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_with_schema(self):
        schema = pa.schema([("a", pa.int8()), ("b", pa.string())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["a"],
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(table.schema.names, ["a"])
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_with_schema_reverse_order(self):
        schema = pa.schema([("b", pa.string()), ("a", pa.int8())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(table.schema.names, ["b", "a"])
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_with_schema_subset_cols(self):
        schema = pa.schema([("a", pa.int8())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(table.schema.names, ["a"])
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_with_schema_extra_cols(self):
        schema = pa.schema([("a", pa.int8()), ("MISSING", pa.string())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(
            table.schema.names, ["a", "MISSING"]
        )  # NOTE: "MISSING" is padded as a null array
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.schema.field("MISSING").type, pa.string())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_with_schema_extra_cols_column_names(self):
        schema = pa.schema([("a", pa.int8()), ("MISSING", pa.string())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            column_names=["a", "MISSING"],
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(
            table.schema.names, ["a", "MISSING"]
        )  # NOTE: "MISSING" is padded as a null array
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.schema.field("MISSING").type, pa.string())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_local_single_column_with_schema_only_missing_col(self):
        schema = pa.schema([("a", pa.int8()), ("MISSING", pa.string())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["MISSING"],
            column_names=["a", "MISSING"],
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(
            table.schema.names, ["MISSING"]
        )  # NOTE: "MISSING" is padded as a null array
        self.assertEqual(table.schema.field("MISSING").type, pa.string())
        self.assertEqual(table.num_rows, 0)

    def test_read_from_local_single_column_with_row_groups(self):

        metadata = pq.read_metadata(self.MVP_PATH)
        ppp = PartialParquetParameters.of(pq_metadata=metadata)
        ppp["row_groups_to_download"] = ppp.row_groups_to_download[1:2]
        table = daft_file_to_pyarrow_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            column_names=["b"],
            partial_file_download_params=ppp,
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 10)


class TestFilesToDataFrame(unittest.TestCase):
    MVP_PATH = "deltacat/tests/utils/data/mvp.parquet"

    def test_read_local_files_all_columns(self):
        df = files_to_dataframe(
            uris=[self.MVP_PATH],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            ray_init_options={"local_mode": True, "ignore_reinit_error": True},
        )

        table = df.to_arrow()
        self.assertEqual(table.schema.names, ["a", "b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_local_files_with_column_selection(self):
        df = files_to_dataframe(
            uris=[self.MVP_PATH],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["b"],
            ray_init_options={"local_mode": True, "ignore_reinit_error": True},
        )

        table = df.to_arrow()
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_local_files_does_not_materialize_by_default(self):
        df = files_to_dataframe(
            uris=[self.MVP_PATH],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            ray_init_options={"local_mode": True, "ignore_reinit_error": True},
        )

        # Should raise RuntimeError because df is not materialized yet
        self.assertRaises(RuntimeError, lambda: len(df))

        # After collecting, it should work
        df.collect()
        self.assertEqual(len(df), 100)

    def test_supports_unescaped_tsv_content_type(self):
        # Test that UNESCAPED_TSV is now supported (was previously unsupported)
        # Use a CSV file since we're testing TSV reader functionality
        csv_path = "deltacat/tests/utils/data/non_empty_valid.csv"
        df = files_to_dataframe(
            uris=[csv_path],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.UNESCAPED_TSV.value,
            ray_init_options={"local_mode": True, "ignore_reinit_error": True},
        )
        # Should succeed without raising an exception - this tests that UNESCAPED_TSV is supported
        table = df.to_arrow()
        # Just verify we got some data back, don't assert specific schema since we're reading CSV as TSV
        self.assertGreater(table.num_rows, 0)
        self.assertGreater(len(table.schema.names), 0)

    def test_supports_gzip_content_encoding(self):
        # Test that GZIP encoding is now supported (was previously unsupported)
        df = files_to_dataframe(
            uris=[self.MVP_PATH],
            content_encoding=ContentEncoding.GZIP.value,
            content_type=ContentType.PARQUET.value,
            ray_init_options={"local_mode": True, "ignore_reinit_error": True},
        )
        # Should succeed without raising an exception
        table = df.to_arrow()
        self.assertEqual(table.schema.names, ["a", "b"])
        self.assertEqual(table.num_rows, 100)

    def test_raises_error_if_not_supported_content_type(self):
        # Test that truly unsupported content types raise NotImplementedError
        self.assertRaises(
            NotImplementedError,
            lambda: files_to_dataframe(
                uris=[self.MVP_PATH],
                content_encoding=ContentEncoding.IDENTITY.value,
                content_type=ContentType.AVRO.value,  # AVRO is actually unsupported
                ray_init_options={"local_mode": True, "ignore_reinit_error": True},
            ),
        )

    def test_raises_error_if_not_supported_content_encoding(self):
        # Test that truly unsupported content encodings raise NotImplementedError
        self.assertRaises(
            NotImplementedError,
            lambda: files_to_dataframe(
                uris=[self.MVP_PATH],
                content_encoding=ContentEncoding.ZSTD.value,  # ZSTD is actually unsupported
                content_type=ContentType.PARQUET.value,
                ray_init_options={"local_mode": True, "ignore_reinit_error": True},
            ),
        )

    def test_accepts_custom_kwargs(self):
        # Test that custom kwargs are passed through to daft.read_parquet
        df = files_to_dataframe(
            uris=[self.MVP_PATH],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            ray_init_options={"local_mode": True, "ignore_reinit_error": True},
            # Custom kwarg that should be passed to daft.read_parquet
            coerce_int96_timestamp_unit="ns",
        )

        table = df.to_arrow()
        self.assertEqual(table.schema.names, ["a", "b"])
        self.assertEqual(table.num_rows, 100)

    def test_accepts_io_config(self):
        # Test that io_config parameter is accepted and passed correctly
        df = files_to_dataframe(
            uris=[self.MVP_PATH],
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            ray_init_options={"local_mode": True, "ignore_reinit_error": True},
            # io_config=None should work fine for local files
            io_config=None,
        )

        table = df.to_arrow()
        self.assertEqual(table.schema.names, ["a", "b"])
        self.assertEqual(table.num_rows, 100)


if __name__ == "__main__":
    unittest.main()
