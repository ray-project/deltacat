"""Unit tests for DeltaCAT-Daft catalog integration."""

import daft
from unittest.mock import patch, MagicMock

from deltacat.catalog import Catalog as DeltaCATCatalog
from deltacat.catalog import CatalogProperties
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.dev.daft_integration.io._deltacat import (
    DeltaCATCatalog as DaftDeltaCATCatalog,
)


class TestDaftCatalogIntegration:
    """Test cases for DeltaCAT-Daft catalog integration."""

    def setup_method(self):
        """Set up test fixtures."""
        # Import the module to apply monkey patching
        pass

    @patch.object(DeltaCATCatalog, "default")
    def test_from_deltacat_catalog_conversion(self, mock_default):
        """Test conversion from DeltaCAT catalog to Daft catalog."""
        # Create a mock DeltaCAT catalog
        catalog_props = CatalogProperties(
            metastore_uri="s3://test-bucket/test-metastore"
        )
        mock_catalog = MagicMock()
        mock_catalog.inner = catalog_props
        mock_default.return_value = mock_catalog

        dc_catalog = DeltaCATCatalog.default(catalog_props)

        # Convert to Daft catalog
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Verify that the inner catalog is the same
        assert daft_catalog._inner == mock_catalog
        assert isinstance(daft_catalog, DaftDeltaCATCatalog)

    @patch.object(DeltaCATCatalog, "default")
    def test_create_namespace(self, mock_default):
        """Test creating a namespace in the DeltaCAT catalog via Daft."""
        # Set up mock catalog
        mock_catalog = MagicMock()
        mock_default.return_value = mock_catalog

        dc_catalog = DeltaCATCatalog.default(MagicMock())
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Test creating a namespace
        namespace_name = "test_namespace"
        daft_catalog.create_namespace(namespace_name)

        # Verify the DeltaCAT catalog method was called with correct args
        dc_catalog.impl.create_namespace.assert_called_once_with(
            namespace=namespace_name, inner=dc_catalog.inner
        )

    @patch.object(DeltaCATCatalog, "default")
    def test_list_namespaces(self, mock_default):
        """Test listing namespaces from the DeltaCAT catalog via Daft."""
        # Set up mock catalog with mock namespaces
        mock_catalog = MagicMock()
        mock_namespace1 = MagicMock()
        mock_namespace1.name = "namespace1"
        mock_namespace2 = MagicMock()
        mock_namespace2.name = "namespace2"

        # Set up mocked list_namespaces to return ListResult with our namespaces
        mock_list_result = MagicMock()
        mock_list_result.all_items.return_value = [mock_namespace1, mock_namespace2]
        mock_catalog.impl.list_namespaces.return_value = mock_list_result

        mock_default.return_value = mock_catalog

        dc_catalog = DeltaCATCatalog.default(MagicMock())
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Get namespaces
        namespaces = daft_catalog.list_namespaces()

        # Verify results
        assert len(namespaces) == 2
        assert str(namespaces[0]) == "namespace1"
        assert str(namespaces[1]) == "namespace2"
        dc_catalog.impl.list_namespaces.assert_called_once()

    @patch.object(DeltaCATCatalog, "default")
    def test_get_table(self, mock_default):
        """Test getting a table from the DeltaCAT catalog via Daft."""
        # Set up mock catalog with mock table
        mock_catalog = MagicMock()
        mock_table_def = MagicMock(spec=TableDefinition)
        mock_table = MagicMock()
        mock_table.name = "test_table"
        mock_table_def.table = mock_table

        mock_catalog.impl.get_table.return_value = mock_table_def
        mock_default.return_value = mock_catalog

        dc_catalog = DeltaCATCatalog.default(MagicMock())
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Get table
        table = daft_catalog.get_table("namespace.test_table")

        # Verify results
        assert table.name == "test_table"
        dc_catalog.impl.get_table.assert_called_once_with(
            name="test_table", namespace="namespace", inner=dc_catalog.inner
        )

    @patch.object(DeltaCATCatalog, "default")
    def test_drop_table(self, mock_default):
        """Test dropping a table from the DeltaCAT catalog via Daft."""
        # Set up mock catalog
        mock_catalog = MagicMock()
        mock_default.return_value = mock_catalog

        dc_catalog = DeltaCATCatalog.default(MagicMock())
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Drop table
        daft_catalog.drop_table("namespace.test_table")

        # Verify the DeltaCAT catalog method was called with correct args
        dc_catalog.impl.drop_table.assert_called_once_with(
            name="test_table", namespace="namespace", inner=dc_catalog.inner
        )

    @patch.object(DeltaCATCatalog, "default")
    def test_list_tables(self, mock_default):
        """Test listing tables from the DeltaCAT catalog via Daft."""
        # Set up mock catalog with mock tables
        mock_catalog = MagicMock()

        # Mock tables to return
        mock_table_def1 = MagicMock(spec=TableDefinition)
        mock_table1 = MagicMock()
        mock_table1.name = "table1"
        mock_table_def1.table = mock_table1

        mock_table_def2 = MagicMock(spec=TableDefinition)
        mock_table2 = MagicMock()
        mock_table2.name = "table2"
        mock_table_def2.table = mock_table2

        # Set up mocked list_tables to return ListResult with our tables
        mock_list_result = MagicMock()
        mock_list_result.all_items.return_value = [mock_table_def1, mock_table_def2]
        mock_catalog.impl.list_tables.return_value = mock_list_result

        mock_default.return_value = mock_catalog

        dc_catalog = DeltaCATCatalog.default(MagicMock())
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Get tables
        tables = daft_catalog.list_tables("namespace.*")

        # Verify results
        assert len(tables) == 2
        assert tables[0] == "table1"
        assert tables[1] == "table2"
        dc_catalog.impl.list_tables.assert_called_once_with(
            namespace="namespace", inner=dc_catalog.inner
        )
