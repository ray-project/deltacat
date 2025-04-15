import unittest
from unittest.mock import MagicMock

from deltacat import TableDefinition

class TestTableDefinition(unittest.TestCase):

    def test_create_scan_plan_not_initialized(self):
        mock_table = MagicMock()
        mock_table.table_name = 'mock_table_name'
        mock_table.namespace = 'mock_namespace'

        table_definition = TableDefinition({
            "table": mock_table
        })
        with self.assertRaises(RuntimeError) as context:
            table_definition.create_scan_plan()
        self.assertIn("ScanPlanner is not initialized", str(context.exception))
