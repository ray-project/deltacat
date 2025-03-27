import unittest
import pytest
import ray
import tempfile
import shutil
import uuid
from unittest import mock
import os

from deltacat.catalog import CatalogProperties
from pyiceberg.catalog import Catalog as IcebergCatalog

from deltacat.catalog.model.catalog import (
    Catalog,
    init,
    get_catalog,
    put_catalog,
    is_initialized,
)
from deltacat.catalog.iceberg.iceberg_catalog_config import IcebergCatalogConfig

from pyiceberg.catalog import CatalogType


# Test module to mock a catalog implementation
class MockCatalogImpl:
    @staticmethod
    def initialize(*args, **kwargs):
        # Return some state that the catalog would normally maintain
        return {"initialized": True, "args": args, "kwargs": kwargs}


class TestCatalog(unittest.TestCase):
    """Tests for the Catalog class itself, without Ray initialization."""

    def test_catalog_constructor(self):
        """Test that the Catalog constructor correctly initializes with the given implementation."""
        catalog = Catalog(impl=MockCatalogImpl)

        self.assertEqual(catalog.impl, MockCatalogImpl)

        # Check that inner state was correctly initialized
        # This just asserts that kwargs were plumbed through from Catalog constructor
        self.assertTrue(catalog.inner["initialized"])
        self.assertEqual(catalog.inner["args"], ())
        self.assertEqual(catalog.inner["kwargs"], {})

    def test_iceberg_factory_method(self):
        """Test the iceberg factory method correctly creates an Iceberg catalog."""
        # Create a mock for the Iceberg catalog module
        with mock.patch(
            "deltacat.catalog.model.catalog.iceberg_catalog"
        ) as mock_iceberg_catalog:
            # Configure the mock to return a known value when initialize is called
            mock_iceberg_catalog.initialize.return_value = {"iceberg": True}

            # Create an Iceberg catalog config and invoke iceberg factory method
            config = IcebergCatalogConfig(type=CatalogType.IN_MEMORY, properties={})
            catalog = Catalog.iceberg(config)

            # Check that the implementation is set to iceberg_catalog
            self.assertEqual(catalog.impl, mock_iceberg_catalog)
            # Check that the inner state is set to the output of initialize
            self.assertEqual(catalog.inner, {"iceberg": True})


@pytest.fixture(scope="function")
def isolated_ray_env(request):
    """
    Setup and teardown an isolated Ray environment for tests.

    In order to use this fixture, Ray must be re-initialize providing the namespace and ignoring re-initialization errors

    Re-initialize ray like:
    init(catalog,
        ray_init_args={"namespace": namespace, "ignore_reinit_error": True},
        **{"force_reinitialize": True})
    """

    namespace = f"test_catalogs_{uuid.uuid4().hex}"

    # Reset the global all_catalogs state before each test
    global all_catalogs
    all_catalogs = None

    yield namespace

    # Clean up the actor in this namespace if it exists
    if all_catalogs is not None and isinstance(all_catalogs, ray.actor.ActorHandle):
        try:
            ray.kill(all_catalogs)
        except Exception:
            pass

    # Reset all_catalogs to None
    all_catalogs = None


class TestCatalogsWithRay:
    """
    Tests for Catalogs and global catalog state which require Ray initialization.
    """

    def test_init_single_catalog(self, isolated_ray_env):
        """Test initializing a single catalog."""
        # Create a catalog
        catalog = Catalog(impl=MockCatalogImpl)

        # Initialize with a single catalog and Ray init args including the namespace
        init(
            catalog,
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        assert is_initialized()

        # Get the default catalog and check it's the same one we initialized with
        retrieved_catalog = get_catalog()
        assert retrieved_catalog.impl == MockCatalogImpl
        assert retrieved_catalog.inner["initialized"]

    def test_init_multiple_catalogs(self, isolated_ray_env):
        """Test initializing multiple catalogs."""
        # Create catalogs
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)

        # Initialize with multiple catalogs and Ray init args including the namespace
        catalogs_dict = {"catalog1": catalog1, "catalog2": catalog2}
        init(
            catalogs_dict,
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        assert is_initialized()

        # Get catalogs by name and check they're the same ones we initialized with
        retrieved_catalog1 = get_catalog("catalog1")
        assert retrieved_catalog1.impl == MockCatalogImpl
        assert retrieved_catalog1.inner["kwargs"]["id"] == 1

        retrieved_catalog2 = get_catalog("catalog2")
        assert retrieved_catalog2.impl == MockCatalogImpl
        assert retrieved_catalog2.inner["kwargs"]["id"] == 2

    def test_init_with_default_catalog_name(self, isolated_ray_env):
        """Test initializing with a specified default catalog name."""
        # Create catalogs
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)

        # Initialize with multiple catalogs and specify a default
        catalogs_dict = {"catalog1": catalog1, "catalog2": catalog2}
        init(
            catalogs_dict,
            default_catalog_name="catalog2",
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        # Get the default catalog and check it's catalog2
        default_catalog = get_catalog()
        assert default_catalog.impl == MockCatalogImpl
        assert default_catalog.inner["kwargs"]["id"] == 2

    def test_put_catalog(self, isolated_ray_env):
        """Test adding a catalog after initialization."""
        # Initialize with a single catalog
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        init(
            {"catalog1": catalog1},
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        # Add a second catalog
        put_catalog("catalog2", impl=MockCatalogImpl, id=2)

        # Check both catalogs are available
        retrieved_catalog1 = get_catalog("catalog1")
        assert retrieved_catalog1.inner["kwargs"]["id"] == 1

        retrieved_catalog2 = get_catalog("catalog2")
        assert retrieved_catalog2.inner["kwargs"]["id"] == 2

    def test_put_catalog_that_already_exists(self, isolated_ray_env):
        """Test that trying to add a catalog with a name that already exists raises an error."""
        # Initialize with a catalog
        catalog = Catalog(impl=MockCatalogImpl, id=1)
        init(
            {"test_catalog": catalog},
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        # Try to add another catalog with the same name
        with pytest.raises(ValueError, match="Catalog test_catalog already exists."):
            put_catalog("test_catalog", impl=MockCatalogImpl, id=2)

    def test_get_catalog_nonexistent(self, isolated_ray_env):
        """Test that trying to get a nonexistent catalog raises an error."""
        # Initialize with a catalog
        catalog = Catalog(impl=MockCatalogImpl)
        init(
            {"test_catalog": catalog},
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        # Try to get a nonexistent catalog
        with pytest.raises(KeyError, match="Catalog 'nonexistent' not found"):
            get_catalog("nonexistent")

    def test_get_catalog_no_default(self, isolated_ray_env):
        """Test that trying to get the default catalog when none is set raises an error."""
        # Initialize with multiple catalogs but no default
        catalog1 = Catalog(impl=MockCatalogImpl, id=1)
        catalog2 = Catalog(impl=MockCatalogImpl, id=2)
        init(
            {"catalog1": catalog1, "catalog2": catalog2},
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        # Try to get the default catalog
        with pytest.raises(KeyError):
            get_catalog()


class TestIcebergCatalogIntegration:
    """Integration tests for Iceberg catalog functionality."""

    temp_dir = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def teardown_class(cls):
        if cls.temp_dir and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def test_iceberg_catalog_initialization(self, isolated_ray_env):
        """Test that an Iceberg catalog can be initialized and accessed."""
        catalog_name = str(uuid.uuid4())

        # Create the Iceberg catalog config
        config = IcebergCatalogConfig(
            type=CatalogType.IN_MEMORY, properties={"warehouse": self.temp_dir}
        )

        # Create the catalog using the factory method
        catalog = Catalog.iceberg(config)

        # Initialize DeltaCAT with this catalog
        init({catalog_name: catalog}, ray_init_args={"namespace": isolated_ray_env})

        # Retrieve the catalog and verify it's the same one
        retrieved_catalog = get_catalog(catalog_name)
        assert retrieved_catalog.impl.__name__ == "deltacat.catalog.iceberg"
        assert isinstance(retrieved_catalog.inner, IcebergCatalog)


class TestDefaultCatalogIntegration:
    """Integration tests for Default catalog functionality."""

    temp_dir = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def teardown_class(cls):
        if cls.temp_dir and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def test_default_catalog_initialization(self, isolated_ray_env):
        """Test that a Default catalog can be initialized and accessed using the factory method."""
        from deltacat.catalog.model.properties import CatalogProperties

        catalog_name = str(uuid.uuid4())

        # Create the catalog properties
        config = CatalogProperties(root=self.temp_dir)

        # Create the catalog using the factory method
        catalog = Catalog.default(config)

        # Initialize DeltaCAT with this catalog
        init(
            {catalog_name: catalog},
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialize": True},
        )

        # Retrieve the catalog and verify it's the same one
        retrieved_catalog = get_catalog(catalog_name)
        assert retrieved_catalog.impl.__name__ == "deltacat.catalog.main"
        assert isinstance(retrieved_catalog.inner, CatalogProperties)
        assert retrieved_catalog.inner.root == self.temp_dir

    def test_default_catalog_initialization_from_kwargs(self, isolated_ray_env):

        catalog_name = str(uuid.uuid4())
        # Initialize DeltaCAT with this catalog
        from deltacat.catalog.main import DeltacatCatalog

        init(
            {catalog_name: Catalog(DeltacatCatalog, **{"root": "test_root"})},
            ray_init_args={"namespace": isolated_ray_env, "ignore_reinit_error": True},
            **{"force_reinitialization": True},
        )

        # Retrieve the catalog and verify it's the same one
        retrieved_catalog = get_catalog(catalog_name)
        assert retrieved_catalog.impl.__name__ == "deltacat.catalog.main"
        assert isinstance(retrieved_catalog.inner, CatalogProperties)
        assert retrieved_catalog.inner.root == "test_root"
