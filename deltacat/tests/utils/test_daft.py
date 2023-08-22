import unittest
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.utils.daft import daft_s3_file_to_table


class TestDaftParquetReader(unittest.TestCase):
    URL = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet"  # Maybe change this to something deltacat hosts

    def test_read_from_s3_all_columns(self):
        table = daft_s3_file_to_table(
            self.URL,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
        )
        self.assertEqual(table.schema.names, ["a", "b"])
        self.assertEqual(table.num_rows, 100)

    def test_read_from_s3_single_column(self):
        table = daft_s3_file_to_table(
            self.URL,
            content_encoding=ContentEncoding.IDENTITY.value,
            content_type=ContentType.PARQUET.value,
            include_columns=["b"],
        )
        self.assertEqual(table.schema.names, ["b"])
        self.assertEqual(table.num_rows, 100)


if __name__ == "__main__":
    unittest.main()
