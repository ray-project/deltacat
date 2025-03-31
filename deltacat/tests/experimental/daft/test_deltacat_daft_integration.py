import daft
from daft import Table, Identifier
from daft.catalog import Catalog as DaftCatalog
import pytest
from unittest import mock
import uuid

from deltacat.catalog import Catalog as DeltaCATCatalog
from deltacat.catalog import CatalogProperties
from deltacat.catalog.model.catalog import Catalog, init, get_catalog, put_catalog
from deltacat.constants import DEFAULT_CATALOG
from deltacat.experimental.daft.deltacat_daft_patch import DeltaCATCatalog as DaftDeltaCATCatalog
import shutil
import tempfile

# Import the integration module to apply monkey patching
from deltacat.catalog.iceberg import IcebergCatalogConfig

from pyiceberg.catalog import CatalogType, load_catalog
from deltacat.catalog.main import impl as CatalogImpl

# Import the isolated_ray_env fixture from the catalog tests
from deltacat.tests.catalog.conftest import isolated_ray_env


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
            **{"force_reinitialize": True}
        )

        # Now create a DeltaCATCatalog with just the name
        daft_catalog = DaftDeltaCATCatalog(name=catalog_name, namespace=isolated_ray_env)

        # Verify it correctly retrieved the catalog
        retrieved_catalog = get_catalog(catalog_name, namespace=isolated_ray_env)
        assert daft_catalog._inner.impl == retrieved_catalog.impl

    def test_init_with_nonexistent_catalog_name(self, isolated_ray_env):
        """Test initializing with a name that doesn't exist raises ValueError."""
        # Make sure the catalog doesn't exist in this namespace
        catalog_name = f"nonexistent_catalog_{uuid.uuid4().hex}"

        # Try to create a DeltaCATCatalog with a name that doesn't exist
        with pytest.raises(ValueError, match=f"No catalog with name '{catalog_name}' found in DeltaCAT"):
            DaftDeltaCATCatalog(name=catalog_name, namespace=isolated_ray_env)

    def test_init_with_new_catalog(self, isolated_ray_env):
        """Test initializing with a new catalog registers it."""
        # Create a catalog but don't register it
        catalog_name = f"new_catalog_{uuid.uuid4().hex}"
        dc_catalog = Catalog(impl=CatalogImpl, root=self.tmpdir)

        # Create a DeltaCATCatalog with the new catalog
        daft_catalog = DaftDeltaCATCatalog(name=catalog_name, catalog=dc_catalog, namespace=isolated_ray_env)

        # Verify the catalog was registered and is retrievable
        retrieved_catalog = get_catalog(catalog_name, namespace=isolated_ray_env)
        assert retrieved_catalog is not None
        assert daft_catalog._inner == dc_catalog

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
                ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
                **{"force_reinitialize": True}
            )

            # Create a new catalog with the same implementation but different root
            with tempfile.TemporaryDirectory() as root2:
                new_catalog = Catalog(impl=CatalogImpl, root=root2)

                with pytest.raises(ValueError,
                                   match=f"A catalog with name '{catalog_name}' already exists in DeltaCAT but has a different implementation"):
                    DaftDeltaCATCatalog(name=catalog_name, catalog=new_catalog, namespace=isolated_ray_env)


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
        catalog_name = f"deltacat_{uuid.uuid4().hex[:8]}"
        daft.attach_catalog(daft_catalog, catalog_name)

        # Create a sample DataFrame
        df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        # TODO data write not working. Currently create_table will only create an empty table
        daft_catalog.create_table("example_table", df)
        table: Table = daft_catalog.get_table("example_table")

    def test_from_deltacat_conversion(self):
        """Test the from_deltacat conversion method."""
        # Create a DeltaCAT catalog
        catalog_props = CatalogProperties(root=self.tmpdir)
        dc_catalog = DeltaCATCatalog.default(catalog_props)

        # Convert to DaftCatalog
        daft_catalog = DaftCatalog.from_deltacat(dc_catalog)

        # Verify it's the right type and has the inner property set
        assert isinstance(daft_catalog, DaftDeltaCATCatalog)
        assert daft_catalog._inner == dc_catalog


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
            type=CatalogType.IN_MEMORY,
            properties={"warehouse": warehouse_path}
        )
        dc_catalog = DeltaCATCatalog.iceberg(config)

        # Convert the DeltaCAT catalog to a Daft catalog
        daft_catalog = daft.catalog.Catalog.from_deltacat(dc_catalog)

        # Register the catalog with Daft's catalog system
        catalog_name = f"deltacat_iceberg_{uuid.uuid4().hex[:8]}"
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

        assert any(t[0]==namespace and t[1] == table_name for t in tables), f"Table {table_name} not found in Iceberg catalog"

        # Load the table from Iceberg catalog and verify its properties
        iceberg_table = iceberg_catalog.load_table(f"{namespace}.{table_name}")

        # Check that the schema matches our DataFrame
        schema = iceberg_table.schema()
        assert schema.find_field("id") is not None, "Field 'id' not found in table schema"
        assert schema.find_field("value") is not None, "Field 'value' not found in table schema"
