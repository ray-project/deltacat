import pytest
import tempfile
import shutil
import uuid
from unittest import mock
import os
import yaml

from deltacat.catalog import (
    CatalogProperties,
    Catalog,
    clear_catalogs,
    get_catalog,
    init,
    init_local,
    is_initialized,
    put_catalog,
    pop_catalog,
)
import deltacat.catalog as catalogs
from deltacat.experimental.catalog.iceberg import impl as IcebergCatalog
from pyiceberg.catalog import Catalog as PyIcebergCatalog

from deltacat.experimental.catalog.iceberg import IcebergCatalogConfig

from pyiceberg.catalog import CatalogType


# Test module to mock a catalog implementation
class MockCatalogImpl:
    @staticmethod
    def initialize(config, *args, **kwargs):
        # Return some state that the catalog would normally maintain
        return CatalogProperties(root=kwargs.get("root", "/tmp/test"))


@pytest.fixture(scope="function")
def reset_catalogs():
    clear_catalogs()
    catalogs.all_catalogs = None  # reset the global actor reference


class TestCatalog:
    """Tests for the Catalog class itself, without Ray initialization."""

    def test_catalog_constructor(self):
        """Test that the Catalog constructor correctly initializes with the given implementation."""
        # Construct a catalog with our MockCatalogImpl
        catalog = Catalog(impl=MockCatalogImpl, root="/tmp/test")

        # The catalog should store the impl we passed
        assert catalog.impl is MockCatalogImpl

        # The inner state should be a CatalogProperties instance
        assert isinstance(catalog.inner, CatalogProperties)
        assert catalog.inner.root == "/tmp/test"

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
            assert catalog.impl == mock_iceberg_catalog
            # Check that the inner state is set to the output of initialize
            assert catalog.inner == {"iceberg": True}


class TestCatalogsIntegration:
    """Integration tests for Default catalog functionality."""

    temp_dir = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        # Initialize a catalog so tests start with Ray running
        catalog = Catalog(impl=MockCatalogImpl, root=cls.temp_dir)
        init(catalog, force=True)

    @classmethod
    def teardown_class(cls):
        if cls.temp_dir and os.path.exists(cls.temp_dir):
            shutil.rmtree(cls.temp_dir)

    def test_init_single_catalog(self, reset_catalogs):
        catalog = Catalog(impl=MockCatalogImpl, root="/tmp/catalog1")
        init(catalog, force=True)
        assert is_initialized()
        retrieved = get_catalog()
        assert isinstance(retrieved.inner, CatalogProperties)
        assert retrieved.inner.root == "/tmp/catalog1"

    def test_init_multiple_catalogs(self, reset_catalogs):
        catalog1 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog1")
        catalog2 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog2")
        init({"c1": catalog1, "c2": catalog2}, force=True)
        assert is_initialized()
        r1 = get_catalog("c1")
        r2 = get_catalog("c2")
        assert r1.inner.root == "/tmp/catalog1"
        assert r2.inner.root == "/tmp/catalog2"

    def test_init_with_default_catalog_name(self, reset_catalogs):
        catalog1 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog1")
        catalog2 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog2")
        init({"c1": catalog1, "c2": catalog2}, default="c2", force=True)
        default_cat = get_catalog()
        assert default_cat.inner.root == "/tmp/catalog2"

    def test_init_with_config_yaml(self, tmp_path, reset_catalogs):
        config_data = {
            "test-catalog": {
                "root": str(tmp_path),
                "filesystem": None,
                "storage": None,
            }
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(config_data, f)
        init(config_path=str(config_path), force=True)
        catalog_props = get_catalog()
        assert isinstance(catalog_props, CatalogProperties)
        assert catalog_props.root == str(tmp_path)
        import pyarrow.fs

        assert isinstance(catalog_props.filesystem, pyarrow.fs.FileSystem)

    def test_init_with_catalogs_and_config_path_raises(self, reset_catalogs):
        cat = Catalog(impl=MockCatalogImpl, root="/tmp/test")
        with pytest.raises(ValueError):
            init({"c": cat}, config_path="dummy.yml", force=True)

    def test_put_catalog(self, reset_catalogs):
        c1 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog1")
        c2 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog2")
        init({"c1": c1}, force=True)
        put_catalog("c2", c2)
        r1 = get_catalog("c1")
        r2 = get_catalog("c2")
        assert r1.inner.root == "/tmp/catalog1"
        assert r2.inner.root == "/tmp/catalog2"

    def test_put_catalog_that_already_exists(self, reset_catalogs):
        c1 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog1")
        c2 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog2")
        put_catalog("test_catalog", c1)
        put_catalog("test_catalog", c2)  # overwrite allowed
        retrieved = get_catalog("test_catalog")
        assert retrieved.inner.root == "/tmp/catalog2"
        with pytest.raises(ValueError):
            put_catalog("test_catalog", c1, fail_if_exists=True)

    def test_put_catalog_persists_merge_to_yaml(self, reset_catalogs, mocker, tmp_path):
        fake_cat = Catalog(impl=MockCatalogImpl, root="/tmp/fake")
        cfg_path = tmp_path / "deltacat.yml"
        mocker.patch("deltacat.constants.DELTACAT_CONFIG_PATH", str(cfg_path))
        mocker.patch("deltacat.catalog.is_initialized", return_value=True)
        mocker.patch(
            "deltacat.catalog.get_catalog", side_effect=ValueError("not found")
        )
        dump_mock = mocker.patch(
            "deltacat.catalog.model.catalog._dump_catalogs_to_yaml"
        )
        catalogs.all_catalogs = mocker.MagicMock()
        put_catalog("foo", fake_cat)
        dump_mock.assert_called()

    def test_get_catalog_nonexistent(self, reset_catalogs):
        cat = Catalog(impl=MockCatalogImpl, root="/tmp/catalog")
        init({"test_catalog": cat}, force=True)
        with pytest.raises(ValueError):
            get_catalog("nope")

    def test_get_catalog_no_default(self, reset_catalogs):
        c1 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog1")
        c2 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog2")
        init({"c1": c1, "c2": c2}, force=True)
        with pytest.raises(ValueError):
            get_catalog()

    def test_pop_catalog_returns_none_if_not_initialized(self, reset_catalogs):
        catalogs.all_catalogs = None
        assert pop_catalog("whatever") is None

    def test_init_local(self, reset_catalogs):
        """Test that init_local() creates a default local catalog."""
        # Initialize with default local catalog
        init_local(force=True)

        assert is_initialized()

        # Should be able to get the default catalog
        default_catalog = get_catalog()
        assert default_catalog is not None

        # The default catalog should be accessible by name "default"
        named_catalog = get_catalog("default")
        assert named_catalog is not None
        assert named_catalog.impl.__name__ == "deltacat.catalog.main.impl"

    def test_init_local_with_path(self, reset_catalogs):
        """Test that init_local(path) creates a default local catalog with specified path."""
        # Create a temporary directory for the test
        custom_path = tempfile.mkdtemp()

        try:
            # Initialize with custom path
            init_local(path=custom_path, force=True)

            assert is_initialized()

            # Should be able to get the default catalog
            default_catalog = get_catalog()
            assert default_catalog is not None

            # The default catalog should be accessible by name "default"
            named_catalog = get_catalog("default")
            assert named_catalog is not None
            assert named_catalog.impl.__name__ == "deltacat.catalog.main.impl"

            # Verify the catalog is using the custom path
            catalog_properties = named_catalog.inner
            assert catalog_properties.root == custom_path

        finally:
            # Clean up the temporary directory
            if os.path.exists(custom_path):
                shutil.rmtree(custom_path)

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
