"""
Tests for automatic transaction partition migration during catalog initialization.
"""

import posixpath
import tempfile
import time
import uuid

import pyarrow.fs

from deltacat.catalog.model.properties import CatalogProperties
from deltacat.utils.filesystem import write_file, resolve_path_and_filesystem
from deltacat.storage.model.transaction import Transaction
from deltacat.constants import (
    TXN_DIR_NAME,
    SUCCESS_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
)


class TestCatalogPropertiesTransactionMigration:
    """Test automatic transaction migration during catalog initialization."""

    def test_catalog_initialization_migrates_unpartitioned_transactions(self):
        """Test that catalog initialization automatically migrates unpartitioned transactions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            _, filesystem = resolve_path_and_filesystem(temp_dir)

            # Create unpartitioned transaction structure (old format)
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            failed_dir = posixpath.join(txn_dir, FAILED_TXN_DIR_NAME)

            filesystem.create_dir(success_dir, recursive=True)
            filesystem.create_dir(failed_dir, recursive=True)

            # Create realistic unpartitioned transaction structure
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

            # Initialize catalog - this should trigger automatic migration
            catalog_properties = CatalogProperties(root=temp_dir)
            assert catalog_properties.root == temp_dir

            # Verify transactions were migrated to partitioned structure
            # Check success transaction
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

            # Verify partitioned transactions exist
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

            # Verify original unpartitioned transactions are gone
            assert (
                filesystem.get_file_info(success_txn_dir).type
                == pyarrow.fs.FileType.NotFound
            )
            assert (
                filesystem.get_file_info(failed_file_path).type
                == pyarrow.fs.FileType.NotFound
            )

    def test_catalog_initialization_no_transactions_to_migrate(self):
        """Test that catalog initialization succeeds when no transactions need migration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Initialize catalog on empty directory - should succeed without error
            catalog_properties = CatalogProperties(root=temp_dir)

            # Verify catalog was initialized properly
            assert catalog_properties.root == temp_dir
            assert catalog_properties.filesystem is not None
            assert catalog_properties.version is not None

    def test_catalog_initialization_already_migrated_transactions(self):
        """Test that catalog initialization succeeds when transactions are already partitioned."""
        with tempfile.TemporaryDirectory() as temp_dir:
            _, filesystem = resolve_path_and_filesystem(temp_dir)

            # Create already-partitioned transaction structure
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            filesystem.create_dir(success_dir, recursive=True)

            # Create already partitioned success transaction
            current_time = time.time_ns()
            success_txn_id = f"{current_time}_{uuid.uuid4()}"

            # Use the correct partitioned path structure
            partitioned_txn_dir = Transaction.success_txn_log_dir_path(
                success_dir, success_txn_id
            )
            filesystem.create_dir(partitioned_txn_dir, recursive=True)

            txn_end_time = str(current_time + 500000)
            partitioned_txn_file = posixpath.join(partitioned_txn_dir, txn_end_time)
            write_file(
                partitioned_txn_file, "already partitioned transaction", filesystem
            )

            # Initialize catalog - should succeed without attempting migration
            catalog_properties = CatalogProperties(root=temp_dir)

            # Verify catalog was initialized properly
            assert catalog_properties.root == temp_dir
            assert catalog_properties.filesystem is not None
            assert catalog_properties.version is not None

            # Verify partitioned transaction still exists
            assert (
                filesystem.get_file_info(partitioned_txn_dir).type
                == pyarrow.fs.FileType.Directory
            )
            assert (
                filesystem.get_file_info(partitioned_txn_file).type
                == pyarrow.fs.FileType.File
            )

    def test_catalog_initialization_mixed_partitioned_unpartitioned(self):
        """Test catalog initialization with mixed partitioned and unpartitioned transactions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            _, filesystem = resolve_path_and_filesystem(temp_dir)

            # Create transaction directories
            txn_dir = posixpath.join(temp_dir, TXN_DIR_NAME)
            success_dir = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME)
            failed_dir = posixpath.join(txn_dir, FAILED_TXN_DIR_NAME)

            filesystem.create_dir(success_dir, recursive=True)
            filesystem.create_dir(failed_dir, recursive=True)

            current_time = time.time_ns()

            # Create unpartitioned success transaction
            unpart_txn_id = f"{current_time}_{uuid.uuid4()}"
            unpart_txn_dir = posixpath.join(success_dir, unpart_txn_id)
            filesystem.create_dir(unpart_txn_dir, recursive=True)
            unpart_file = posixpath.join(unpart_txn_dir, str(current_time + 500000))
            write_file(unpart_file, "unpartitioned transaction", filesystem)

            # Create already partitioned success transaction
            part_txn_id = f"{current_time + 1000000}_{uuid.uuid4()}"
            part_txn_dir = Transaction.success_txn_log_dir_path(
                success_dir, part_txn_id
            )
            filesystem.create_dir(part_txn_dir, recursive=True)
            part_file = posixpath.join(part_txn_dir, str(current_time + 1500000))
            write_file(part_file, "already partitioned transaction", filesystem)

            # Create unpartitioned failed transaction
            failed_txn_id = f"{current_time + 2000000}_{uuid.uuid4()}"
            failed_file_path = posixpath.join(failed_dir, failed_txn_id)
            write_file(failed_file_path, "failed transaction", filesystem)

            # Initialize catalog - should migrate only the unpartitioned transactions
            catalog_properties = CatalogProperties(root=temp_dir)
            assert catalog_properties.root == temp_dir

            # Verify unpartitioned transaction was migrated
            expected_unpart_txn_dir = Transaction.success_txn_log_dir_path(
                success_dir, unpart_txn_id
            )
            expected_unpart_file = posixpath.join(
                expected_unpart_txn_dir, str(current_time + 500000)
            )

            assert (
                filesystem.get_file_info(expected_unpart_txn_dir).type
                == pyarrow.fs.FileType.Directory
            )
            assert (
                filesystem.get_file_info(expected_unpart_file).type
                == pyarrow.fs.FileType.File
            )

            # Verify already partitioned transaction remains untouched
            assert (
                filesystem.get_file_info(part_txn_dir).type
                == pyarrow.fs.FileType.Directory
            )
            assert filesystem.get_file_info(part_file).type == pyarrow.fs.FileType.File

            # Verify failed transaction was migrated
            expected_failed_path = Transaction.failed_txn_log_path(
                failed_dir, failed_txn_id
            )
            assert (
                filesystem.get_file_info(expected_failed_path).type
                == pyarrow.fs.FileType.File
            )

            # Verify original unpartitioned files are gone
            assert (
                filesystem.get_file_info(unpart_txn_dir).type
                == pyarrow.fs.FileType.NotFound
            )
            assert (
                filesystem.get_file_info(failed_file_path).type
                == pyarrow.fs.FileType.NotFound
            )
