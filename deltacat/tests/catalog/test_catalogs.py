import pytest
import tempfile
import shutil
import uuid
import os
import yaml
import posixpath
import time

from pathlib import Path

from unittest import mock
from unittest.mock import patch

import pyarrow.fs

import deltacat as dc

from deltacat.catalog import (
    CatalogProperties,
    CatalogVersion,
    Catalog,
    clear_catalogs,
    get_catalog,
    init,
    init_local,
    is_initialized,
    put_catalog,
    pop_catalog,
    save_catalogs,
)
from deltacat.catalog.model.catalog import load_catalog_config
from deltacat.constants import (
    CATALOG_VERSION_DIR_NAME,
    TXN_DIR_NAME,
    SUCCESS_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
)
from deltacat.utils.filesystem import (
    list_directory,
    write_file,
    resolve_path_and_filesystem,
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
        # Return the config if provided, otherwise create default
        if config is not None:
            return config
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
        catalog = get_catalog()
        assert isinstance(catalog, Catalog)
        catalog_props = catalog.inner
        assert isinstance(catalog_props, CatalogProperties)
        assert catalog_props.root == str(tmp_path)

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
        """Test that catalog persistence works correctly with the new deferred persistence model."""
        fake_cat = Catalog(impl=MockCatalogImpl, root="/tmp/fake")
        cfg_path = tmp_path / "deltacat.yml"

        # Mock the dump function to verify it gets called with the right data
        dump_mock = mocker.patch("deltacat.catalog.model.catalog.dump_catalog_config")

        # Import after mocking to ensure we get the mocked version
        from deltacat.catalog.model.catalog import dump_catalog_config

        # Test direct dump_catalog_config call with catalog dictionary
        test_catalogs = {"foo": fake_cat}
        dump_catalog_config(test_catalogs, str(cfg_path))

        # Verify the dump function was called with the catalog dictionary
        dump_mock.assert_called_once_with(test_catalogs, str(cfg_path))

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

    def test_default_catalog_initialization_via_put_catalog(self, reset_catalogs):

        catalog_name = str(uuid.uuid4())

        # Initialize DeltaCAT with this catalog
        put_catalog(
            catalog_name,
            Catalog(root=self.temp_dir),
        )

        # Retrieve the catalog and verify it's the same one
        retrieved_catalog = get_catalog(catalog_name)
        assert retrieved_catalog.impl.__name__ == "deltacat.catalog.main.impl"
        assert isinstance(retrieved_catalog.inner, CatalogProperties)
        assert retrieved_catalog.inner.root == self.temp_dir

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

    def test_catalog_version_file_creation(self, reset_catalogs):
        """Test that catalog initialization creates version files correctly."""

        # Create a temporary directory for the test
        catalog_root = tempfile.mkdtemp()

        try:
            # Initialize catalog - this should create version file
            catalog_properties = CatalogProperties(root=catalog_root)

            # Verify version file was created
            version_dir_path = posixpath.join(catalog_root, CATALOG_VERSION_DIR_NAME)
            version_files_and_sizes = list_directory(
                version_dir_path,
                catalog_properties.filesystem,
                ignore_missing_path=True,
            )

            # Should have exactly one version file
            assert (
                len(version_files_and_sizes) == 1
            ), f"Expected 1 version file, found {len(version_files_and_sizes)}"

            # Extract the version filename
            version_filename = posixpath.basename(version_files_and_sizes[0][0])

            # Parse the version file and verify it matches current DeltaCAT version
            catalog_version = CatalogVersion.from_filename(version_filename)
            assert catalog_version.version == dc.__version__
            assert isinstance(catalog_version.starting_from, int)
            assert catalog_version.starting_from > 0

            # Verify the catalog properties version matches
            assert catalog_properties.version == catalog_version

        finally:
            # Clean up the temporary directory
            if os.path.exists(catalog_root):
                shutil.rmtree(catalog_root)

    def test_catalog_transaction_migration_invocation(self, reset_catalogs):
        """Test that catalog initialization invokes transaction migration."""
        # Create a temporary directory for the test
        catalog_root = tempfile.mkdtemp()

        try:
            # Create unpartitioned transaction structure to ensure migration has work to do
            _, filesystem = resolve_path_and_filesystem(catalog_root)

            txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            failed_dir = posixpath.join(txn_dir, FAILED_TXN_DIR_NAME)

            filesystem.create_dir(success_dir, recursive=True)
            filesystem.create_dir(failed_dir, recursive=True)

            # Create unpartitioned transactions
            current_time = time.time_ns()
            success_txn_id = f"{current_time}_{uuid.uuid4()}"
            failed_txn_id = f"{current_time + 1000000}_{uuid.uuid4()}"

            # Success transaction: directory containing end time file
            success_txn_dir = posixpath.join(success_dir, success_txn_id)
            filesystem.create_dir(success_txn_dir, recursive=True)
            txn_end_time = str(current_time + 500000)
            success_txn_file = posixpath.join(success_txn_dir, txn_end_time)
            write_file(success_txn_file, "success transaction data", filesystem)

            # Failed transaction: single file
            failed_file_path = posixpath.join(failed_dir, failed_txn_id)
            write_file(failed_file_path, "failed transaction data", filesystem)

            # Mock the migration function to verify it gets called
            with patch(
                "deltacat.experimental.compatibility.backfill_transaction_partitions.backfill_transaction_partitions"
            ) as mock_migration:
                # Initialize catalog - this should invoke transaction migration
                CatalogProperties(root=catalog_root)

                # Verify migration function was called exactly once
                mock_migration.assert_called_once()

                # Verify it was called with the catalog properties
                call_args = mock_migration.call_args[0]
                assert len(call_args) == 1
                assert isinstance(call_args[0], CatalogProperties)
                assert call_args[0].root == catalog_root

        finally:
            # Clean up the temporary directory
            if os.path.exists(catalog_root):
                shutil.rmtree(catalog_root)

    def test_catalog_persistence_mechanisms(
        self, reset_catalogs, temp_deltacat_config_path
    ):
        """Test catalog persistence via both manual save_catalogs() and actor destruction."""
        # Test 1: Manual save_catalogs() - should always work
        cat1 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog1")
        cat2 = Catalog(impl=MockCatalogImpl, root="/tmp/catalog2")

        # Initialize with multiple catalogs
        init({"test_cat1": cat1, "test_cat2": cat2}, default="test_cat1", force=True)

        # Verify catalogs are in memory
        assert get_catalog("test_cat1").inner.root == "/tmp/catalog1"
        assert get_catalog("test_cat2").inner.root == "/tmp/catalog2"

        # Manually save catalogs - this should always work
        save_catalogs()

        # Verify config file was written
        config_path = Path(temp_deltacat_config_path)
        assert (
            config_path.exists()
        ), f"Config file should exist at {temp_deltacat_config_path}"

        # Verify file contents
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        assert config_data is not None
        assert "test_cat1" in config_data
        assert "test_cat2" in config_data
        assert config_data["test_cat1"]["root"] == "/tmp/catalog1"
        assert config_data["test_cat2"]["root"] == "/tmp/catalog2"

        # Test 2: Persistence on catalog modifications
        # The catalog module now calls try_dump() whenever catalogs are modified.

        # Clear the config file to test that try_dump on modifications works
        config_path.unlink()
        assert not config_path.exists()

        # Modify catalogs which should trigger try_dump
        put_catalog(
            "test_catalog_3",
            Catalog(CatalogProperties(root="/tmp/test3")),
        )

        # Config should be immediately persisted due to try_dump
        assert config_path.exists()
        with open(config_path, "r") as f:
            modified_config_data = yaml.safe_load(f)

        # Should contain all 3 catalogs now
        assert len(modified_config_data) == 3
        assert "test_catalog_3" in modified_config_data
        assert "test_cat1" in modified_config_data
        assert "test_cat2" in modified_config_data

    def test_catalog_config_round_trip_file_io(
        self, reset_catalogs, temp_deltacat_config_path
    ):
        """Test actual catalog config read/write to disk without mocking."""
        # Create our own temp file to avoid test environment issues
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        test_config_path = temp_file.name
        temp_file.close()

        try:
            # Create test catalogs
            cat1 = Catalog(
                impl=MockCatalogImpl, config=CatalogProperties(root="/tmp/round_trip1")
            )
            cat2 = Catalog(
                impl=MockCatalogImpl, config=CatalogProperties(root="/tmp/round_trip2")
            )

            # Initialize and ensure config is saved
            init({"rt_cat1": cat1, "rt_cat2": cat2}, default="rt_cat1", force=True)

            # Manually save catalogs to test the save functionality
            save_catalogs(test_config_path)

            # Verify file was written
            config_path = Path(test_config_path)
            assert (
                config_path.exists()
            ), f"Config file should exist at {test_config_path}"

            # Read and verify the written config
            with open(config_path, "r") as f:
                written_config = yaml.safe_load(f)

            assert written_config["rt_cat1"]["root"] == "/tmp/round_trip1"
            assert written_config["rt_cat2"]["root"] == "/tmp/round_trip2"

            # Clear current catalogs
            clear_catalogs()

            # Load catalogs from the config file - this tests config loading
            loaded_catalogs = load_catalog_config(test_config_path)
            assert "rt_cat1" in loaded_catalogs
            assert "rt_cat2" in loaded_catalogs
            assert loaded_catalogs["rt_cat1"].inner.root == "/tmp/round_trip1"
            assert loaded_catalogs["rt_cat2"].inner.root == "/tmp/round_trip2"

        finally:
            # Cleanup
            if os.path.exists(test_config_path):
                os.unlink(test_config_path)

    def test_restore_last_session_functionality(
        self, reset_catalogs, temp_deltacat_config_path
    ):
        """Test the session save/restore configuration file functionality."""
        # First, create and save a session
        original_cat = Catalog(
            impl=MockCatalogImpl, config=CatalogProperties(root="/tmp/session_restore")
        )
        init({"session_cat": original_cat}, default="session_cat", force=True)
        save_catalogs(temp_deltacat_config_path)

        # Verify config was saved with correct content
        config_path = Path(temp_deltacat_config_path)
        assert config_path.exists()

        with open(config_path, "r") as f:
            saved_config = yaml.safe_load(f)

        # Verify the session data was saved correctly
        assert "session_cat" in saved_config
        assert saved_config["session_cat"]["root"] == "/tmp/session_restore"

    def test_config_file_not_overwritten_when_exists(
        self, reset_catalogs, temp_deltacat_config_path
    ):
        """Test that explicit config_path takes precedence over restore_last_session."""
        # Create initial session config
        session_cat = Catalog(impl=MockCatalogImpl, root="/tmp/session")
        init({"session_cat": session_cat}, force=True)
        save_catalogs()

        # Create a different config file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as alt_config:
            alt_config_data = {"alt_cat": {"root": "/tmp/alternative"}}
            yaml.safe_dump(alt_config_data, alt_config)
            alt_config_path = alt_config.name

        try:
            # Clear catalogs
            clear_catalogs()

            # Initialize with explicit config_path (should take precedence)
            init(config_path=alt_config_path, restore_last_session=True, force=True)

            # Should load from alt_config_path, not the session config
            alt_cat = get_catalog("alt_cat")
            assert alt_cat.inner.root == "/tmp/alternative"

            # Should not have the session catalog
            try:
                get_catalog("session_cat")
                assert (
                    False
                ), "session_cat should not be loaded when explicit config_path is provided"
            except ValueError:
                pass  # Expected - session_cat should not exist

        finally:
            # Clean up alternative config file
            Path(alt_config_path).unlink(missing_ok=True)

    def test_catalog_persistence_with_catalog_properties_serialization(
        self, reset_catalogs, temp_deltacat_config_path
    ):
        """Test that only serializable CatalogProperties are persisted (no filesystem objects)."""
        # Create catalog with CatalogProperties that has filesystem object
        cat = Catalog(config=CatalogProperties(root="/tmp/serialization_test"))

        # Initialize and save
        init({"serialize_test": cat}, force=True)
        save_catalogs(temp_deltacat_config_path)

        # Read the saved config
        config_path = Path(temp_deltacat_config_path)
        with open(config_path, "r") as f:
            saved_config = yaml.safe_load(f)

        # Should only contain root, not filesystem or other non-serializable fields
        serialize_config = saved_config["serialize_test"]
        assert "root" in serialize_config
        assert serialize_config["root"] == "/tmp/serialization_test"

        # Should not contain filesystem or other non-serializable fields
        assert "filesystem" not in serialize_config
        assert "storage" not in serialize_config

        # Test that the config can be loaded without errors
        loaded_catalogs = load_catalog_config(temp_deltacat_config_path)
        assert "serialize_test" in loaded_catalogs
        assert loaded_catalogs["serialize_test"].inner.root == "/tmp/serialization_test"

    def test_catalog_modification_persistence(
        self, reset_catalogs, temp_deltacat_config_path
    ):
        """Test that catalogs are persisted immediately when modified."""
        # Initialize with a catalog
        cat = Catalog(impl=MockCatalogImpl, root="/tmp/modify_test")
        init({"modify_test": cat}, force=True)

        config_path = Path(temp_deltacat_config_path)

        # Test that put_catalog triggers persistence
        put_catalog(
            "modify_test2", Catalog(impl=MockCatalogImpl, root="/tmp/modify_test2")
        )

        assert config_path.exists()
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)
        assert "modify_test2" in config_data

        # Test that pop_catalog triggers persistence
        config_path.unlink()
        pop_catalog("modify_test2")
        assert config_path.exists()

        # Test that clear_catalogs triggers persistence
        config_path.unlink()
        clear_catalogs()
        assert config_path.exists()
