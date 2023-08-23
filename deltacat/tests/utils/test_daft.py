import unittest
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.utils.daft import daft_s3_file_to_table

from deltacat.utils.pyarrow import ReadKwargsProviderPyArrowSchemaOverride

import pyarrow as pa


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

    def test_read_from_s3_single_column(self):
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["b"],
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column_with_schema(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
        pa_read_func_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
            schema=schema
        )
        table = daft_s3_file_to_table(
            self.MVP_PATH,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["b"],
            pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)


if __name__ == "__main__":
    unittest.main()
