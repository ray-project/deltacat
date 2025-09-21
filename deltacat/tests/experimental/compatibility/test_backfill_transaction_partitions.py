"""
Tests for transaction partition backfill functionality.
"""

import posixpath
import tempfile
import time
import uuid
from unittest.mock import MagicMock

import pytest
import pyarrow.fs

from deltacat.experimental.compatibility.backfill_transaction_partitions import (
    backfill_transaction_partitions,
    _migrate_transaction_status_directory,
)
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.storage.model.transaction import Transaction
from deltacat.utils.filesystem import write_file, resolve_path_and_filesystem
from deltacat.constants import (
    TXN_DIR_NAME,
    SUCCESS_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
)


class TestBackfillTransactionPartitions:
    """Test transaction partition migration functionality."""

    def test_backfill_transaction_partitions_with_version_check(self):
        """Test that migration validates catalog version matches current version."""
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_properties = CatalogProperties(root=temp_dir)

            # Mock version check to simulate version mismatch
            mock_version = MagicMock()
            mock_version.version = "0.1.0"  # Different from current
            catalog_properties._version = mock_version

            with pytest.raises(
                ValueError, match="Catalog version .* doesn't match current version"
            ):
                backfill_transaction_partitions(catalog_properties)

    def test_backfill_transaction_partitions_success(self):
        """Test successful migration of transaction files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup catalog properties
            catalog_properties = CatalogProperties(root=temp_dir)
            filesystem = catalog_properties.filesystem

            # Create transaction directories
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            failed_dir = posixpath.join(txn_dir, FAILED_TXN_DIR_NAME)

            filesystem.create_dir(success_dir, recursive=True)
            filesystem.create_dir(failed_dir, recursive=True)

            # Create realistic transaction structure
            current_time = time.time_ns()
            success_txn_id = f"{current_time}_{uuid.uuid4()}"
            failed_txn_id = f"{current_time + 1000000}_{uuid.uuid4()}"

            # Success transactions: directory containing file with end time as filename
            success_txn_dir = posixpath.join(success_dir, success_txn_id)
            filesystem.create_dir(success_txn_dir, recursive=True)

            # Transaction end time file (simplified for test)
            txn_end_time = str(current_time + 500000)
            success_txn_file = posixpath.join(success_txn_dir, txn_end_time)
            write_file(success_txn_file, "success transaction data", filesystem)

            # Failed transactions: single file with txn_id as filename
            failed_file_path = posixpath.join(failed_dir, failed_txn_id)
            write_file(failed_file_path, "failed transaction data", filesystem)

            # Mock version to allow migration
            catalog_properties._version = None
            # Run migration
            backfill_transaction_partitions(catalog_properties)

            # Verify files were moved to partitioned locations
            # Check success transaction - should move entire directory structure
            # Note: success_txn_log_dir_path already includes the txn_id at the end
            expected_success_txn_dir = Transaction.success_txn_log_dir_path(
                success_dir, success_txn_id
            )
            expected_success_file = posixpath.join(
                expected_success_txn_dir, txn_end_time
            )

            # Check failed transaction
            expected_failed_path = Transaction.failed_txn_log_path(
                failed_dir, failed_txn_id
            )

            # Verify partitioned success transaction directory and file exist
            assert (
                filesystem.get_file_info(expected_success_txn_dir).type
                == pyarrow.fs.FileType.Directory
            )
            assert (
                filesystem.get_file_info(expected_success_file).type
                == pyarrow.fs.FileType.File
            )
            assert (
                filesystem.get_file_info(expected_failed_path).type
                == pyarrow.fs.FileType.File
            )

            # Verify original success transaction directory and file are gone
            success_dir_info = filesystem.get_file_info(success_txn_dir)
            assert success_dir_info.type == pyarrow.fs.FileType.NotFound

            failed_file_info = filesystem.get_file_info(failed_file_path)
            assert failed_file_info.type == pyarrow.fs.FileType.NotFound

    def test_migrate_transaction_status_directory_empty(self):
        """Test migration when directory is empty."""
        with tempfile.TemporaryDirectory() as temp_dir:
            _, filesystem = resolve_path_and_filesystem(temp_dir)

            # Test with non-existent directory
            migrated_count = _migrate_transaction_status_directory(
                catalog_root=temp_dir,
                filesystem=filesystem,
                status_dir_name=SUCCESS_TXN_DIR_NAME,
                is_success=True,
            )

            assert migrated_count == 0

    def test_migrate_transaction_status_directory_success(self):
        """Test migration of success transaction directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            _, filesystem = resolve_path_and_filesystem(temp_dir)

            # Create success transaction directory with realistic structure
            success_dir = posixpath.join(temp_dir, TXN_DIR_NAME, SUCCESS_TXN_DIR_NAME)
            filesystem.create_dir(success_dir, recursive=True)

            # Create multiple transaction directories with end time files
            current_time = time.time_ns()
            txn_data = [
                (f"{current_time}_{uuid.uuid4()}", str(current_time + 500000)),
                (
                    f"{current_time + 1000000}_{uuid.uuid4()}",
                    str(current_time + 1500000),
                ),
                (
                    f"{current_time + 2000000}_{uuid.uuid4()}",
                    str(current_time + 2500000),
                ),
            ]

            for txn_id, end_time in txn_data:
                # Create transaction directory
                txn_dir = posixpath.join(success_dir, txn_id)
                filesystem.create_dir(txn_dir, recursive=True)

                # Create end time file inside transaction directory
                end_time_file = posixpath.join(txn_dir, end_time)
                write_file(end_time_file, f"transaction data for {txn_id}", filesystem)

            # Run migration
            migrated_count = _migrate_transaction_status_directory(
                catalog_root=temp_dir,
                filesystem=filesystem,
                status_dir_name=SUCCESS_TXN_DIR_NAME,
                is_success=True,
            )

            # Verify all transaction directories were migrated
            assert migrated_count == len(txn_data)

            # Verify transaction directories exist in partitioned locations
            for txn_id, end_time in txn_data:
                # Note: success_txn_log_dir_path already includes the txn_id at the end
                expected_txn_dir = Transaction.success_txn_log_dir_path(
                    success_dir, txn_id
                )
                expected_end_time_file = posixpath.join(expected_txn_dir, end_time)

                # Verify directory structure
                dir_info = filesystem.get_file_info(expected_txn_dir)
                assert dir_info.type == pyarrow.fs.FileType.Directory

                # Verify end time file
                file_info = filesystem.get_file_info(expected_end_time_file)
                assert file_info.type == pyarrow.fs.FileType.File

    def test_migrate_transaction_status_directory_failed(self):
        """Test migration of failed transaction directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            _, filesystem = resolve_path_and_filesystem(temp_dir)

            # Create failed transaction directory with files
            failed_dir = posixpath.join(temp_dir, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
            filesystem.create_dir(failed_dir, recursive=True)

            # Create transaction file
            current_time = time.time_ns()
            txn_id = f"{current_time}_{uuid.uuid4()}"
            file_path = posixpath.join(failed_dir, txn_id)
            write_file(file_path, f"failed transaction data for {txn_id}", filesystem)

            # Run migration
            migrated_count = _migrate_transaction_status_directory(
                catalog_root=temp_dir,
                filesystem=filesystem,
                status_dir_name=FAILED_TXN_DIR_NAME,
                is_success=False,
            )

            # Verify file was migrated
            assert migrated_count == 1

            # Verify file exists in partitioned location
            expected_path = Transaction.failed_txn_log_path(failed_dir, txn_id)
            file_info = filesystem.get_file_info(expected_path)
            assert file_info.type == pyarrow.fs.FileType.File

            # Verify original file is gone
            original_file_info = filesystem.get_file_info(file_path)
            assert original_file_info.type == pyarrow.fs.FileType.NotFound

    def test_migration_invalid_files_skipped(self):
        """Test that invalid transaction files are skipped with warnings."""
        with tempfile.TemporaryDirectory() as temp_dir:
            _, filesystem = resolve_path_and_filesystem(temp_dir)

            # Create success directory with files
            success_dir = posixpath.join(temp_dir, TXN_DIR_NAME, SUCCESS_TXN_DIR_NAME)
            filesystem.create_dir(success_dir, recursive=True)

            # Create a valid transaction directory structure
            current_time = time.time_ns()
            valid_txn_id = f"{current_time}_{uuid.uuid4()}"
            valid_txn_dir = posixpath.join(success_dir, valid_txn_id)
            filesystem.create_dir(valid_txn_dir, recursive=True)

            # Create end time file inside valid transaction directory
            txn_end_time = str(current_time + 500000)
            valid_end_time_file = posixpath.join(valid_txn_dir, txn_end_time)
            write_file(valid_end_time_file, "valid transaction", filesystem)

            # Create a directory with invalid transaction ID format
            invalid_txn_dir = posixpath.join(success_dir, "invalid_txn_id")
            filesystem.create_dir(invalid_txn_dir, recursive=True)
            invalid_file = posixpath.join(invalid_txn_dir, "some_file")
            write_file(invalid_file, "invalid transaction", filesystem)

            # Migration should succeed, skipping invalid directory with warning
            migrated_count = _migrate_transaction_status_directory(
                catalog_root=temp_dir,
                filesystem=filesystem,
                status_dir_name=SUCCESS_TXN_DIR_NAME,
                is_success=True,
            )

            # Should have migrated only the valid transaction directory
            assert migrated_count == 1

            # Verify valid transaction directory was moved to partitioned location
            # Note: success_txn_log_dir_path already includes the txn_id at the end
            expected_txn_dir = Transaction.success_txn_log_dir_path(
                success_dir, valid_txn_id
            )
            expected_end_time_file = posixpath.join(expected_txn_dir, txn_end_time)

            # Verify directory structure
            dir_info = filesystem.get_file_info(expected_txn_dir)
            assert dir_info.type == pyarrow.fs.FileType.Directory

            # Verify end time file
            file_info = filesystem.get_file_info(expected_end_time_file)
            assert file_info.type == pyarrow.fs.FileType.File

            # Verify invalid directory still exists in original location
            invalid_dir_info = filesystem.get_file_info(invalid_txn_dir)
            assert invalid_dir_info.type == pyarrow.fs.FileType.Directory

    def test_no_migration_needed(self):
        """Test when no transaction files need migration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_properties = CatalogProperties(root=temp_dir)

            # Mock version to allow migration
            catalog_properties._version = None
            # Run migration on empty catalog
            backfill_transaction_partitions(catalog_properties)

            # Should complete without error even with no files to migrate

    def test_no_double_nesting_success_transactions(self):
        """Test that success transactions are not double-nested (regression protection)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_properties = CatalogProperties(root=temp_dir)
            filesystem = catalog_properties.filesystem

            # Create transaction directories
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            filesystem.create_dir(success_dir, recursive=True)

            # Create a success transaction with realistic structure
            current_time = time.time_ns()
            success_txn_id = f"{current_time}_{uuid.uuid4()}"

            # Success transaction: directory containing file with end time as filename
            success_txn_dir = posixpath.join(success_dir, success_txn_id)
            filesystem.create_dir(success_txn_dir, recursive=True)

            txn_end_time = str(current_time + 500000)
            success_txn_file = posixpath.join(success_txn_dir, txn_end_time)
            write_file(success_txn_file, "success transaction data", filesystem)

            # Mock version to allow migration
            catalog_properties._version = None

            # Run migration
            backfill_transaction_partitions(catalog_properties)

            # Verify correct directory structure (no double nesting)
            expected_txn_dir = Transaction.success_txn_log_dir_path(
                success_dir, success_txn_id
            )
            expected_file_path = posixpath.join(expected_txn_dir, txn_end_time)

            # Verify the transaction directory exists at the correct location
            assert (
                filesystem.get_file_info(expected_txn_dir).type
                == pyarrow.fs.FileType.Directory
            ), f"Expected transaction directory not found at: {expected_txn_dir}"

            # Verify the end time file exists at the correct location
            assert (
                filesystem.get_file_info(expected_file_path).type
                == pyarrow.fs.FileType.File
            ), f"Expected transaction file not found at: {expected_file_path}"

            # Verify there's NO double-nested directory (this was the bug)
            # The bug would create: {partition_dirs}/{txn_id}/{txn_id}/{end_time}
            # Instead of correct: {partition_dirs}/{txn_id}/{end_time}
            double_nested_dir = posixpath.join(expected_txn_dir, success_txn_id)
            double_nested_file = posixpath.join(double_nested_dir, txn_end_time)

            assert (
                filesystem.get_file_info(double_nested_dir).type
                == pyarrow.fs.FileType.NotFound
            ), f"Found incorrect double-nested directory at: {double_nested_dir}"

            assert (
                filesystem.get_file_info(double_nested_file).type
                == pyarrow.fs.FileType.NotFound
            ), f"Found incorrect double-nested file at: {double_nested_file}"

            # Verify original transaction directory is gone
            assert (
                filesystem.get_file_info(success_txn_dir).type
                == pyarrow.fs.FileType.NotFound
            ), f"Original transaction directory still exists at: {success_txn_dir}"

    def test_read_only_filesystem_safety_no_transactions(self):
        """Test that migration is safe for read-only filesystems when no transactions exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_properties = CatalogProperties(root=temp_dir)
            filesystem = catalog_properties.filesystem

            # Create empty transaction directories
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            failed_dir = posixpath.join(txn_dir, FAILED_TXN_DIR_NAME)

            filesystem.create_dir(success_dir, recursive=True)
            filesystem.create_dir(failed_dir, recursive=True)

            # Mock version to allow migration
            catalog_properties._version = None

            # Mock a filesystem error that would occur on read-only systems
            from unittest.mock import patch

            def mock_migrate_status_dir(*args, **kwargs):
                raise PermissionError("Read-only filesystem")

            with patch(
                "deltacat.experimental.compatibility.backfill_transaction_partitions._migrate_transaction_status_directory",
                side_effect=mock_migrate_status_dir,
            ):
                # This should NOT raise an exception when no transactions exist
                try:
                    backfill_transaction_partitions(
                        catalog_properties, show_progress=False
                    )
                    # Success - migration handled read-only filesystem gracefully
                except Exception as e:
                    pytest.fail(
                        f"Migration should not fail on read-only filesystem when no transactions exist: {e}"
                    )

    def test_read_only_filesystem_failure_with_transactions(self):
        """Test that migration fails appropriately when transactions exist but filesystem is read-only."""
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_properties = CatalogProperties(root=temp_dir)
            filesystem = catalog_properties.filesystem

            # Create transaction directories with a transaction
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            filesystem.create_dir(success_dir, recursive=True)

            # Create a transaction that needs migration
            current_time = time.time_ns()
            success_txn_id = f"{current_time}_{uuid.uuid4()}"
            success_txn_dir = posixpath.join(success_dir, success_txn_id)
            filesystem.create_dir(success_txn_dir, recursive=True)

            txn_end_time = str(current_time + 500000)
            success_txn_file = posixpath.join(success_txn_dir, txn_end_time)
            write_file(success_txn_file, "transaction data", filesystem)

            # Mock version to allow migration
            catalog_properties._version = None

            # Mock a filesystem error that would occur on read-only systems
            from unittest.mock import patch

            def mock_migrate_status_dir(*args, **kwargs):
                raise PermissionError("Read-only filesystem")

            with patch(
                "deltacat.experimental.compatibility.backfill_transaction_partitions._migrate_transaction_status_directory",
                side_effect=mock_migrate_status_dir,
            ):
                # This SHOULD raise an exception when transactions exist but migration fails
                with pytest.raises(
                    RuntimeError,
                    match="Failed to migrate .* transactions",
                ):
                    backfill_transaction_partitions(
                        catalog_properties, show_progress=False
                    )

    def test_migration_early_exit_no_transactions(self):
        """Test that migration exits early when no transactions are found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_properties = CatalogProperties(root=temp_dir)
            filesystem = catalog_properties.filesystem

            # Create empty transaction directories
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            failed_dir = posixpath.join(txn_dir, FAILED_TXN_DIR_NAME)

            filesystem.create_dir(success_dir, recursive=True)
            filesystem.create_dir(failed_dir, recursive=True)

            # Mock version to allow migration
            catalog_properties._version = None

            # Mock the migration function to ensure it's never called when no transactions exist
            from unittest.mock import patch, MagicMock

            mock_migrate = MagicMock()
            with patch(
                "deltacat.experimental.compatibility.backfill_transaction_partitions._migrate_transaction_status_directory",
                mock_migrate,
            ):
                # Run migration
                backfill_transaction_partitions(catalog_properties, show_progress=False)

                # Verify that _migrate_transaction_status_directory was never called
                # because we exited early when no transactions were found
                mock_migrate.assert_not_called()
