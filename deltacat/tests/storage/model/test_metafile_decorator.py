import unittest
import tempfile

from deltacat.storage import (
    NamespaceLocator,
    Table,
    TableLocator,
)


class TestMetafileDecorator(unittest.TestCase):
    def test_table(self):
        with tempfile.gettempdir() as temp_dir:
            namespace_locator = NamespaceLocator.of("test_namespace")
            table_locator = TableLocator.of(namespace_locator, "test_table")
            table = Table.of(table_locator, "test table description")
            temp_file_path = temp_dir / "table.txt"
            print(f"Writing table to: {temp_file_path}")
            table.write(temp_file_path)
            print(f"Reading table from: {temp_file_path}")
            table.read(temp_file_path)
