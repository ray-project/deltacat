import os
import time
import posixpath
import pytest
import pyarrow as pa

from deltacat.storage import (
    Transaction,
    TransactionOperation,
    TransactionOperationType,
    TransactionType,
)
from deltacat.constants import (
    TXN_DIR_NAME, 
    RUNNING_TXN_DIR_NAME, 
    FAILED_TXN_DIR_NAME,
    TXN_PART_SEPARATOR,
)
from deltacat.utils.filesystem import resolve_path_and_filesystem
from deltacat.tests.test_utils.storage import (
    create_test_namespace,
    create_test_table,
)
from deltacat.compute.janitor import (
    janitor_delete_timed_out_transaction,
    janitor_remove_files_in_failed,
)


class TestJanitorJob:
    def test_janitor_delete_timed_out_transaction(self, temp_dir):
        # Set up test data
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        running_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, RUNNING_TXN_DIR_NAME)
        failed_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
        
        # Create required directories
        os.makedirs(running_txn_dir, exist_ok=True)
        os.makedirs(failed_txn_dir, exist_ok=True)
        
        # Create a test transaction that should time out
        start_time = time.time() - 100  # 100 seconds ago (older than default threshold)
        txn_id = "test_transaction_id"
        txn_filename = f"{start_time}{TXN_PART_SEPARATOR}{txn_id}"
        txn_path = posixpath.join(running_txn_dir, txn_filename)
        
        # Create a mock transaction file
        namespace = create_test_namespace()
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=namespace,
                )
            ],
        )
        
        # Save transaction to the running directory
        write_paths, txn_log_path = transaction.commit(temp_dir)
        
        # Mock the transaction file in the running directory
        with open(txn_path, 'w') as f:
            f.write("mock transaction content")
        
        # Create a test metafile that contains the transaction id to test brute force search
        test_metafile_path = posixpath.join(catalog_root, f"test_metafile_{txn_id}.json")
        with open(test_metafile_path, 'w') as f:
            f.write("mock metafile content")
        
        # Run the function with a 30-second threshold
        janitor_delete_timed_out_transaction(temp_dir, threshold_seconds=30)
        
        # Check that the transaction is both in the failed directory and no also not in running anymore
        assert not os.path.exists(txn_path)
        assert os.path.exists(posixpath.join(failed_txn_dir, txn_filename))
        
        # Check that the metafile was deleted by the brute force search
        assert not os.path.exists(test_metafile_path)

    def test_janitor_remove_files_in_failed(self, temp_dir):
        # Set up test data
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        failed_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
        
        # Create required directories
        os.makedirs(failed_txn_dir, exist_ok=True)
        
        # Create a test transaction with known write paths
        namespace = create_test_namespace()
        table = create_test_table()
        
        meta_to_create = [namespace, table]
        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=meta,
            )
            for meta in meta_to_create
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=txn_operations,
        )
        
        # Commit transaction to get write paths
        write_paths, txn_log_path = transaction.commit(temp_dir)
        
        # Move transaction log to failed directory
        failed_txn_path = posixpath.join(failed_txn_dir, os.path.basename(txn_log_path))
        os.rename(txn_log_path, failed_txn_path)
        
        # Verify files exist before cleanup
        for path in write_paths:
            assert os.path.exists(path)
        
        # Run the cleanup function
        janitor_remove_files_in_failed(temp_dir, filesystem)
        
        # Check that all write paths have been deleted
        for path in write_paths:
            assert not os.path.exists(path)