import os
import time
import posixpath
import pyarrow as pa

from deltacat.storage import (
    Transaction,
    TransactionOperation,
    TransactionOperationType,
    TransactionType,
)
from deltacat.constants import (
    SUCCESSFULLY_CLEANED,
    TXN_DIR_NAME, 
    RUNNING_TXN_DIR_NAME, 
    FAILED_TXN_DIR_NAME,
    TXN_PART_SEPARATOR,
    CURRENTLY_CLEANING
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
        # Set up test directories and filesystem
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        running_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, RUNNING_TXN_DIR_NAME)
        failed_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
        os.makedirs(running_txn_dir, exist_ok=True)
        os.makedirs(failed_txn_dir, exist_ok=True)

        # Create a test transaction log that is already timed out.
        # Note: The janitor expects the first token to be the intended end time.
        start_time = time.time_ns() - 1_000_000_000  # 1 second in the past
        txn_id = "test_transaction_id"
        # The file name uses past_end_time as the first token so that it qualifies as timed out.
        txn_filename = f"{start_time}{txn_id}{TXN_PART_SEPARATOR}{TXN_PART_SEPARATOR}{time.time_ns()}"
        txn_path = posixpath.join(running_txn_dir, txn_filename)
        
        # Create a mock transaction file in the running directory.
        with open(txn_path, 'w') as f:
            f.write("mock transaction content")
        
        # Create a test metafile that contains the transaction id to trigger the renaming logic.
        test_metafile_path = posixpath.join(catalog_root, f"test_metafile_{txn_id}.json")
        with open(test_metafile_path, 'w') as f:
            f.write("mock metafile content")
        
        # Run the janitor job that should:
        # 1. Move the running txn file to the failed directory with TIMEOUT_TXN appended.
        # 2. Invoke brute force search which deletes the metafile and renames the txn log file to use SUCCESSFULLY_CLEANED.
        janitor_delete_timed_out_transaction(temp_dir)
        
        # Expected name: original txn_filename with TIMEOUT_TXN replaced by SUCCESSFULLY_CLEANED.
        new_txn_file_name = f"{txn_filename}{TXN_PART_SEPARATOR}{SUCCESSFULLY_CLEANED}"
        new_failed_txn_path = posixpath.join(failed_txn_dir, new_txn_file_name)
        
        # Verify that the renamed file exists in the failed directory.
        assert os.path.exists(new_failed_txn_path), f"Expected {new_failed_txn_path} to exist."
        # Verify that the transaction file is no longer in the running directory.
        assert not os.path.exists(txn_path), "Transaction file still exists in running directory."
        # Verify that the metafile was deleted.
        assert not os.path.exists(test_metafile_path), "Test metafile was not deleted."

    def test_janitor_remove_files_in_failed(self, temp_dir):
        # Set up test directories and filesystem
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        failed_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
        os.makedirs(failed_txn_dir, exist_ok=True)
        
        # Create a test transaction with known write paths.
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
        # Commit transaction to obtain write paths and a txn log file.
        write_paths, txn_log_path = transaction.commit(temp_dir)
        
        # Simulate a failed txn log file with a filename that includes CURRENTLY_CLEANING.
        new_failed_txn_filename = f"{int(time.time_ns())}{TXN_PART_SEPARATOR}{transaction.id}{TXN_PART_SEPARATOR}{CURRENTLY_CLEANING}"
        failed_txn_path = posixpath.join(failed_txn_dir, new_failed_txn_filename)
        os.rename(txn_log_path, failed_txn_path)
        
        # Verify that the write path files exist before cleanup.
        for path in write_paths:
            assert os.path.exists(path), f"Expected write path {path} to exist before cleanup."
        
        # Run the cleanup function.
        janitor_remove_files_in_failed(temp_dir, filesystem)
        
        # Check that all write paths have been deleted.
        for path in write_paths:
            assert not os.path.exists(path), f"Write path {path} should have been deleted after cleanup."