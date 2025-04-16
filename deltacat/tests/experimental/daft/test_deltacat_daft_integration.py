import daft
from daft import Table, Identifier
import pytest
import uuid

from deltacat.catalog import Catalog as DeltaCATCatalog
from deltacat.catalog import CatalogProperties
from deltacat.catalog.model.catalog import Catalog, init, get_catalog
from deltacat.experimental.daft.daft_catalog import (
    DaftCatalog
)
import shutil
import tempfile

from deltacat.catalog.iceberg import IcebergCatalogConfig

from pyiceberg.catalog import CatalogType
from deltacat.catalog.main import impl as CatalogImpl


class TestDeltaCATDaftCatalogInit:
    """Test suite for DeltaCATCatalog initialization logic."""

    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_init_with_existing_catalog(self, isolated_ray_env):
        """Test initializing with a name that corresponds to an existing catalog."""
        # Create a catalog and register it
        catalog_name = "test_catalog"
        dc_catalog = Catalog(impl=CatalogImpl, root=self.tmpdir)

        # Initialize deltacat with this catalog
        init(
            {catalog_name: dc_catalog},
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        # Now create a DeltaCATCatalog with just the name
        daft_catalog = DaftCatalog(
            name=catalog_name, namespace=isolated_ray_env
        )

        assert daft_catalog._name == "test_catalog"

    def test_init_with_nonexistent_catalog_name(self, isolated_ray_env):
        """Test initializing with a name that doesn't exist raises ValueError."""
        # Make sure the catalog doesn't exist in this namespace
        catalog_name = f"nonexistent_catalog_{uuid.uuid4().hex}"

        # Try to create a DeltaCATCatalog with a name that doesn't exist
        with pytest.raises(
            ValueError, match=f"No catalog with name '{catalog_name}' found in DeltaCAT"
        ):
            DaftDeltaCATCatalog(name=catalog_name, namespace=isolated_ray_env)

    def test_init_with_new_catalog(self, isolated_ray_env):
        """Test initializing with a new catalog registers it."""
        # Create a catalog but don't register it
        catalog_name = f"new_catalog_{uuid.uuid4().hex}"
        dc_catalog = Catalog(impl=CatalogImpl, root=self.tmpdir)

        # Create a DeltaCATCatalog with the new catalog
        DaftDeltaCATCatalog(
            name=catalog_name, catalog=dc_catalog, namespace=isolated_ray_env
        )

        # Verify the catalog was registered and is retrievable
        retrieved_catalog = get_catalog(catalog_name, namespace=isolated_ray_env)
        assert retrieved_catalog is not None

    def test_init_with_different_existing(self, isolated_ray_env):
        """Test initializing with a catalog that matches an existing one uses the existing one."""
        # Create catalogs with same implementation
        catalog_name = f"same_impl_catalog_{uuid.uuid4().hex}"

        # Create a catalog with a unique root to identify it
        with tempfile.TemporaryDirectory() as root1:
            root1 = tempfile.mkdtemp()
            existing_catalog = Catalog(impl=CatalogImpl, root=root1)

            # Initialize DeltaCAT with this catalog
            init(
                {catalog_name: existing_catalog},
                ray_init_args={
                    "namespace": isolated_ray_env,
                    "ignore_reinit_error": True,
                },
                **{"force_reinitialize": True},
            )

            # Create a new catalog with the same implementation but different root
            with tempfile.TemporaryDirectory() as root2:
                new_catalog = Catalog(impl=CatalogImpl, root=root2)

                with pytest.raises(
                    ValueError,
                    match=f"A catalog with name '{catalog_name}' already exists in DeltaCAT but has a different implementation",
                ):
                    DaftDeltaCATCatalog(
                        name=catalog_name,
                        catalog=new_catalog,
                        namespace=isolated_ray_env,
                    )


class TestCatalogIntegration:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_create_table(self):
        """Demonstrate DeltaCAT-Daft integration."""
        # Create a DeltaCAT catalog
        catalog_props = CatalogProperties(root=self.tmpdir)
        dc_catalog = DeltaCATCatalog.default(catalog_props)

        # Use a random catalog name to prevent namespacing conflicts with other tests
        # Convert the DeltaCAT catalog to a Daft catalog
        catalog_name = f"deltacat_{uuid.uuid4().hex[:8]}"


        daft_catalog = DaftCatalog(
            catalog=dc_catalog, name=catalog_name
        )

        # Register the catalog with Daft's catalog system
        daft.attach_catalog(daft_catalog, catalog_name)

        # Create a sample DataFrame
        df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        # Create then get table
        daft_catalog.create_table(Identifier("example_table"), df)
        table: Table = daft_catalog.get_table(Identifier("example_table"))
        assert table.name == "example_table"

    def test_get_table(self):
        """Test getting a table from the DeltaCAT-Daft catalog."""
        # Create a DeltaCAT catalog using the existing tmpdir
        catalog_props = CatalogProperties(root=self.tmpdir)
        dc_catalog = DeltaCATCatalog.default(catalog_props)

        # Convert to DaftCatalog and attach to Daft
        catalog_name = f"deltacat_{uuid.uuid4().hex[:8]}"
        daft_catalog = daft.catalog.Catalog.from_deltacat(
            catalog=dc_catalog, name=catalog_name
        )
        daft.attach_catalog(daft_catalog, catalog_name)

        # Create a sample DataFrame and table
        df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        table_name = "test_get_table"
        daft_catalog.create_table(Identifier(table_name), df)

        # Get the table using different forms of identifiers
        table2 = daft_catalog.get_table(Identifier(table_name))
        assert table2 is not None
        assert table2.name == table_name

        # 3. With namespace. DeltaCAT used the default namespace since it was not provided
        table3 = daft_catalog.get_table(Identifier("DEFAULT", table_name))
        assert table3 is not None
        assert table3.name == table_name

        # Test non-existent table raises an appropriate error
        with pytest.raises(ValueError, match="Table nonexistent_table not found"):
            daft_catalog.get_table(Identifier("nonexistent_table"))


class TestIcebergCatalogIntegration:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_iceberg_catalog_integration(self):
        # Create a unique warehouse path for this test
        warehouse_path = self.tmpdir

        # Configure an Iceberg catalog with the warehouse path
        config = IcebergCatalogConfig(
            type=CatalogType.SQL,
            properties={
                "warehouse": warehouse_path,
                "uri": f"sqlite:////{warehouse_path}/sql-catalog.db",
            },
        )
        dc_catalog = DeltaCATCatalog.iceberg(config)

        # Convert the DeltaCAT catalog to a Daft catalog
        catalog_name = f"deltacat_iceberg_{uuid.uuid4().hex[:8]}"
        daft_catalog = daft.catalog.Catalog.from_deltacat(
            catalog=dc_catalog, name=catalog_name
        )
        daft.attach_catalog(daft_catalog, catalog_name)

        # Create a sample DataFrame
        df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        # Create a table with the Daft catalog
        table_name = "example_table"
        namespace = "example_namespace"
        daft_catalog.create_table(Identifier(namespace, table_name), df)

        # Query that Iceberg table exists using PyIceberg
        iceberg_catalog = dc_catalog.inner

        # Verify the table exists in the Iceberg catalog
        tables = iceberg_catalog.list_tables(namespace)

        assert any(
            t[0] == namespace and t[1] == table_name for t in tables
        ), f"Table {table_name} not found in Iceberg catalog"

        # Load the table from Iceberg catalog and verify its properties
        iceberg_table = iceberg_catalog.load_table(f"{namespace}.{table_name}")

        # Check that the schema matches our DataFrame
        schema = iceberg_table.schema()
        assert (
            schema.find_field("id") is not None
        ), "Field 'id' not fcound in table schema"
        assert (
            schema.find_field("value") is not None
        ), "Field 'value' not found in table schema"
