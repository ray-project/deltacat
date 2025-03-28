"""Example of using DeltaCAT with Daft catalogs.

This example demonstrates how to:
1. Create a DeltaCAT catalog
2. Convert it to a Daft catalog
3. Register it with Daft
4. Perform operations like creating and reading tables
"""

import daft
from daft import Table
from deltacat.catalog import Catalog as DeltaCATCatalog
from deltacat.catalog import CatalogProperties
import shutil
import tempfile

# Import the integration module to apply monkey patching
import deltacat.experimental.daft.deltacat_daft_patch
from deltacat.catalog.iceberg import IcebergCatalogConfig
from pyiceberg.catalog import CatalogType, load_catalog
from pyiceberg.catalog import Catalog as PyIcebergCatalog



class TestCatalogIntegration:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_catalog_table_creation(self):
        """Demonstrate DeltaCAT-Daft integration."""
        # Create a DeltaCAT catalog
        catalog_props = CatalogProperties(root=self.tmpdir)
        dc_catalog = DeltaCATCatalog.default(catalog_props)

        # Convert the DeltaCAT catalog to a Daft catalog
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Register the catalog with Daft's catalog system
        daft.attach_catalog(daft_catalog, "deltacat")

        # Create a sample DataFrame
        df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        # TODO data write not working. Still keeping df here for schema
        daft_catalog.create_table("example_table", df)
        table: Table = daft_catalog.get_table("example_table")

        #

    class TestIcebergCatalogIntegration:
        @classmethod
        def setup_method(cls):
            cls.tmpdir = tempfile.mkdtemp()

        @classmethod
        def teardown_method(cls):
            shutil.rmtree(cls.tmpdir)

    def test_iceberg_catalog_integration(self):

        config = IcebergCatalogConfig(
            type=CatalogType.IN_MEMORY, properties={"warehouse": self.tmpdir}
        )
        dc_catalog = DeltaCATCatalog.iceberg(config)

        # Convert the DeltaCAT catalog to a Daft catalog
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Register the catalog with Daft's catalog system
        daft.attach_catalog(daft_catalog, "deltacat")

        # Create a sample DataFrame
        df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        # TODO data write not working. Still keeping df here for schema
        daft_catalog.create_table("example_table", df)

        # Query that Iceberg table exists using PyIceberg
        catalog = load_catalog()

        #


if __name__ == "__main__":
    main()