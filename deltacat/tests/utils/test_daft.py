import unittest
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.utils.daft import daft_s3_file_to_table

from deltacat.utils.pyarrow import ReadKwargsProviderPyArrowSchemaOverride
from deltacat.types.partial_download import PartialParquetParameters
import pyarrow as pa

from pyarrow import parquet as pq


class TestDaftParquetReader(unittest.TestCase):
    MVP_PATH = "deltacat/tests/utils/data/mvp.parquet"

    def test_read_from_s3_all_columns(self):
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
        )
        self.assertEqual(table.schema.names, ["a", "b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column_via_include_columns(self):
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["b"],
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column_via_column_names(self):
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            column_names=["b"],
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column_with_schema(self):
        schema = pa.schema([("a", pa.int8()), ("b", pa.string())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["a"],
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(table.schema.names, ["a"])
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column_with_schema_reverse_order(self):
        schema = pa.schema([("b", pa.string()), ("a", pa.int8())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(table.schema.names, ["b", "a"])
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column_with_schema_subset_cols(self):
        schema = pa.schema([("a", pa.int8())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(table.schema.names, ["a"])
        self.assertEqual(table.schema.field("a").type, pa.int8())
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column_with_schema_extra_cols(self):
        schema = pa.schema([("a", pa.int8()), ("MISSING", pa.string())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_s3_file_to_table(
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

    def test_read_from_s3_single_column_with_row_groups(self):

        metadata = pq.read_metadata(self.MVP_PATH)
        ppp = PartialParquetParameters.of(pq_metadata=metadata)
        ppp["row_groups_to_download"] = ppp.row_groups_to_download[1:2]
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            column_names=["b"],
            partial_file_download_params=ppp,
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 10)


if __name__ == "__main__":
    unittest.main()
