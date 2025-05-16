import unittest
import pytest
import tempfile
import shutil
import uuid
from unittest import mock
import os

from deltacat.catalog import (
    CatalogProperties,
    Catalog,
    clear_catalogs,
    get_catalog,
    init,
    is_initialized,
    put_catalog,
)
from deltacat.experimental.catalog.iceberg import impl as IcebergCatalog
from pyiceberg.catalog import Catalog as PyIcebergCatalog

from deltacat.experimental.catalog.iceberg import IcebergCatalogConfig

from pyiceberg.catalog import CatalogType


# Test module to mock a catalog implementation
class MockCatalogImpl:
    @staticmethod
    def initialize(config, *args, **kwargs):
        # Return some state that the catalog would normally maintain
        return {
            "initialized": True,
            "config": config,
            "args": args,
            "kwargs": kwargs,
        }


@pytest.fixture(scope="function")
def reset_catalogs():
    clear_catalogs()


class TestCatalog(unittest.TestCase):
    """Tests for the Catalog class itself, without Ray initialization."""

    def test_catalog_constructor(self):
        """Test that the Catalog constructor correctly initializes with the given implementation."""
        catalog = Catalog(impl=MockCatalogImpl)

        self.assertEqual(catalog.impl, MockCatalogImpl)

        # Check that inner state was correctly initialized
        # This just asserts that kwargs were plumbed through from Catalog constructor
        self.assertTrue(catalog.inner["initialized"])
        self.assertIsNone(catalog.inner["config"])
        self.assertEqual(catalog.inner["args"], ())
        self.assertEqual(catalog.inner["kwargs"], {})

    def test_iceberg_factory_method(self):
        """Test the iceberg factory method correctly creates an Iceberg catalog."""
        # Create a mock for the Iceberg catalog module
        with mock.patch(
            "deltacat.experimental.catalog.iceberg.impl.IcebergCatalog"
        ) as mock_iceberg_catalog:
            # Configure the mock to return a known value when initialize is called
            mock_iceberg_catalog.initialize.return_value = {"iceberg": True}

            # Create an Iceberg catalog config and invoke iceberg factory method
            config = IcebergCatalogConfig(type=CatalogType.IN_MEMORY, properties={})
            catalog = IcebergCatalog.from_config(config)

            # Check that the implementation is set to iceberg_catalog
            print(f"catalog.impl: {catalog.impl}")
            print(f"mock_iceberg_catalog: {mock_iceberg_catalog}")
            self.assertEqual(catalog.impl, mock_iceberg_catalog)
            # Check that the inner state is set to the output of initialize
            self.assertEqual(catalog.inner, {"iceberg": True})


class TestCatalogsIntegration:
    """Integration tests for Default catalog functionality."""

    temp_dir = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        # Other tests are going to have initialized ray catalog. Initialize here to ensure
        # that when this test class is run individuall it mimicks running with other tests
        catalog = Catalog(impl=MockCatalogImpl)
        init(
            catalog,
            force=True,
        )

    @classmethod
    def teardown_class(cls):
        if cls.temp_dir and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def test_init_single_catalog(self, reset_catalogs):
        """Test initializing a single catalog."""

        catalog = Catalog(impl=MockCatalogImpl)

        # Initialize with a single catalog and Ray init args including the namespace
        init(catalog, force=True)

        assert is_initialized()

        # Get the default catalog and check it's the same one we initialized with
        retrieved_catalog = get_catalog()
        assert retrieved_catalog.impl == MockCatalogImpl
        assert retrieved_catalog.inner["initialized"]

    def test_init_multiple_catalogs(self, reset_catalogs):
        """Test initializing multiple catalogs."""
        # Create catalogs
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)

        # Initialize with multiple catalogs and Ray init args including the namespace
        catalogs_dict = {"catalog1": catalog1, "catalog2": catalog2}
        init(catalogs_dict, force=True)

        assert is_initialized()

        # Get catalogs by name and check they're the same ones we initialized with
        retrieved_catalog1 = get_catalog("catalog1")
        assert retrieved_catalog1.impl == MockCatalogImpl
        assert retrieved_catalog1.inner["kwargs"]["id"] == 1

        retrieved_catalog2 = get_catalog("catalog2")
        assert retrieved_catalog2.impl == MockCatalogImpl
        assert retrieved_catalog2.inner["kwargs"]["id"] == 2

    def test_init_with_default_catalog_name(self, reset_catalogs):
        """Test initializing with a specified default catalog name."""
        # Create catalogs
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)

        # Initialize with multiple catalogs and specify a default
        catalogs_dict = {"catalog1": catalog1, "catalog2": catalog2}
        init(
            catalogs_dict,
            default="catalog2",
            force=True,
        )

        # Get the default catalog and check it's catalog2
        default_catalog = get_catalog()
        assert default_catalog.impl == MockCatalogImpl
        assert default_catalog.inner["kwargs"]["id"] == 2

    def test_put_catalog(self, reset_catalogs):
        """Test adding a catalog after initialization."""
        # Initialize with a single catalog
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)
        init({"catalog1": catalog1}, force=True)

        # Add a second catalog
        put_catalog("catalog2", catalog2)

        # Check both catalogs are available
        retrieved_catalog1 = get_catalog("catalog1")
        assert retrieved_catalog1.inner["kwargs"]["id"] == 1

        retrieved_catalog2 = get_catalog("catalog2")
        assert retrieved_catalog2.inner["kwargs"]["id"] == 2

    def test_put_catalog_that_already_exists(self, reset_catalogs):
        catalog = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)
        put_catalog(
            "test_catalog",
            catalog,
            id=1,
        )

        # Try to add another catalog with the same name. Should not error
        put_catalog(
            "test_catalog",
            catalog2,
        )

        retrieved_catalog = get_catalog("test_catalog")
        assert retrieved_catalog.inner["kwargs"]["id"] == 2

        # If fail_if_exists, put call should fail
        with pytest.raises(ValueError):
            put_catalog(
                "test_catalog",
                catalog,
                fail_if_exists=True,
            )

    def test_get_catalog_nonexistent(self, reset_catalogs):
        """Test that trying to get a nonexistent catalog raises an error."""
        # Initialize with a catalog
        catalog = Catalog(impl=MockCatalogImpl)
        init({"test_catalog": catalog}, force=True)

        # Try to get a nonexistent catalog
        with pytest.raises(ValueError):
            get_catalog("nonexistent")

    def test_get_catalog_no_default(self, reset_catalogs):
        """Test that trying to get the default catalog when none is set raises an error."""
        # Initialize with multiple catalogs but no default
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)
        init({"catalog1": catalog1, "catalog2": catalog2}, force=True)

        # Try to get the default catalog
        with pytest.raises(ValueError):
            get_catalog()

    def test_default_catalog_initialization(self, reset_catalogs):
        """Test that a Default catalog can be initialized and accessed using the factory method."""
        from deltacat.catalog.model.properties import CatalogProperties

        catalog_name = str(uuid.uuid4())

        # Create the catalog properties
        config = CatalogProperties(root=self.temp_dir)

        # Create the catalog
        catalog = Catalog(config)

        # Initialize DeltaCAT with this catalog
        init({catalog_name: catalog}, force=True)

        # Retrieve the catalog and verify it's the same one
        retrieved_catalog = get_catalog(catalog_name)
        assert retrieved_catalog.impl.__name__ == "deltacat.catalog.main.impl"
        assert isinstance(retrieved_catalog.inner, CatalogProperties)
        assert retrieved_catalog.inner.root == self.temp_dir

    def test_default_catalog_initialization_from_kwargs(self, reset_catalogs):

        catalog_name = str(uuid.uuid4())

        # Initialize DeltaCAT with this catalog
        put_catalog(
            catalog_name,
            Catalog(root="test_root"),
        )

        # Retrieve the catalog and verify it's the same one
        retrieved_catalog = get_catalog(catalog_name)
        assert retrieved_catalog.impl.__name__ == "deltacat.catalog.main.impl"
        assert isinstance(retrieved_catalog.inner, CatalogProperties)
        assert retrieved_catalog.inner.root == "test_root"

    def test_iceberg_catalog_initialization(self, reset_catalogs):
        """Test that an Iceberg catalog can be initialized and accessed."""
        catalog_name = str(uuid.uuid4())

        # Create the Iceberg catalog config
        config = IcebergCatalogConfig(
            type=CatalogType.IN_MEMORY, properties={"warehouse": self.temp_dir}
        )

        # Create the catalog using the factory method
        catalog = IcebergCatalog.from_config(config)

        put_catalog(catalog_name, catalog)

        # Retrieve the catalog and verify it's the same one
        retrieved_catalog = get_catalog(catalog_name)
        assert (
            retrieved_catalog.impl.__name__
            == "deltacat.experimental.catalog.iceberg.impl"
        )
        assert isinstance(retrieved_catalog.inner, PyIcebergCatalog)
