import os
import uuid
import posixpath

from deltacat.storage import (
    Transaction,
    TransactionOperation,
    TransactionOperationType,
)

from deltacat.constants import (
    TXN_DIR_NAME,
    RUNNING_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
    TXN_PART_SEPARATOR,
)
from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    epoch_timestamp_partition_transform,
    write_file_partitioned,
)
from deltacat.tests.test_utils.storage import (
    create_test_namespace,
    create_test_table,
)
from deltacat.compute.janitor import (
    janitor_delete_timed_out_transaction,
    janitor_remove_files_in_failed,
)

JAN_1ST_2025_EPOCH_NS = 1_735_689_600_000_000_000


class TestJanitorJob:
    def test_janitor_delete_timed_out_transaction(self, temp_dir):
        # Set up test directories and filesystem
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        running_txn_dir = posixpath.join(
            catalog_root, TXN_DIR_NAME, RUNNING_TXN_DIR_NAME
        )
        failed_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
        os.makedirs(running_txn_dir, exist_ok=True)
        os.makedirs(failed_txn_dir, exist_ok=True)

        # Create a test transaction log that is already timed out.
        start_time = JAN_1ST_2025_EPOCH_NS

        txn_uuid = str(uuid.uuid4())
        # Create transaction ID in format {start_time}_{uuid}
        txn_id = f"{start_time}{TXN_PART_SEPARATOR}{txn_uuid}"
        # The filename includes end_time to indicate when transaction should be considered timed out
        end_time = start_time + 1000 + 500_000_000  # Already timed out (500ms ago)
        txn_filename = f"{txn_id}{TXN_PART_SEPARATOR}{end_time}"
        txn_path = posixpath.join(running_txn_dir, txn_filename)

        # Create a mock transaction file in the running directory.
        with open(txn_path, "w") as f:
            f.write("mock transaction content")

        # Create a test metafile that contains the transaction uuid to trigger the renaming logic.
        test_metafile_path = posixpath.join(
            catalog_root, f"test_metafile_{txn_uuid}.json"
        )
        with open(test_metafile_path, "w") as f:
            f.write("mock metafile content")

        assert os.path.exists(test_metafile_path), "Test metafile was not deleted."

        assert os.path.exists(
            txn_path
        ), "Transaction file still exists in running directory."

        # Run the janitor job that should:
        # 1. Move the running txn file to the failed directory with TIMEOUT_TXN appended.
        # 2. Invoke brute force search to deletes the metafiles and cleans up txn log files.
        janitor_delete_timed_out_transaction(temp_dir)

        # Calculate the partitioned path where the file should be moved
        # The transaction ID is already in the correct format
        new_failed_txn_path = Transaction.failed_txn_log_path(failed_txn_dir, txn_id)

        # Verify that the renamed file exists in the partitioned failed directory.
        assert os.path.exists(
            new_failed_txn_path
        ), f"Expected {new_failed_txn_path} to exist."
        # Verify that the transaction file is no longer in the running directory.
        assert not os.path.exists(
            txn_path
        ), "Transaction file still exists in running directory."
        # Verify that the metafile was deleted.
        assert not os.path.exists(test_metafile_path), "Test metafile was not deleted."

    def test_janitor_handles_empty_directories(self, temp_dir):
        # Set up test directories and filesystem
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        running_txn_dir = posixpath.join(
            catalog_root, TXN_DIR_NAME, RUNNING_TXN_DIR_NAME
        )
        failed_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)

        # Ensure directories exist but are empty
        os.makedirs(running_txn_dir, exist_ok=True)
        os.makedirs(failed_txn_dir, exist_ok=True)

        # Ensure directories are empty
        assert not os.listdir(
            running_txn_dir
        ), "Running transaction directory is not empty."
        assert not os.listdir(
            failed_txn_dir
        ), "Failed transaction directory is not empty."

        # Run the janitor functions on the empty directories
        try:
            janitor_delete_timed_out_transaction(temp_dir)
            janitor_remove_files_in_failed(temp_dir, filesystem)
        except Exception as e:
            assert (
                False
            ), f"Janitor functions should not fail on empty directories, but got exception: {e}"

        # Verify that directories are still empty after running janitor functions
        assert not os.listdir(
            running_txn_dir
        ), "Running transaction directory should still be empty."
        assert not os.listdir(
            failed_txn_dir
        ), "Failed transaction directory should still be empty."

    def test_janitor_handles_multiple_timed_out_transactions(self, temp_dir):
        # Set up test directories and filesystem
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        running_txn_dir = posixpath.join(
            catalog_root, TXN_DIR_NAME, RUNNING_TXN_DIR_NAME
        )
        failed_txn_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
        os.makedirs(running_txn_dir, exist_ok=True)
        os.makedirs(failed_txn_dir, exist_ok=True)

        # Create multiple timed-out transaction logs
        start_time = JAN_1ST_2025_EPOCH_NS

        txn_filenames = []
        txn_data = []  # Store transaction IDs and UUIDs
        for i in range(3):
            txn_uuid = str(uuid.uuid4())
            # Create transaction ID in format {start_time}_{uuid}
            txn_id = f"{start_time + i * 1000}{TXN_PART_SEPARATOR}{txn_uuid}"  # Slightly different start times
            # The filename includes end_time to indicate when transaction should be considered timed out
            end_time = (
                start_time + i * 1000 + 500_000_000
            )  # Already timed out (500ms ago)
            txn_filename = f"{txn_id}{TXN_PART_SEPARATOR}{end_time}"
            txn_data.append((txn_id, txn_uuid))
            txn_path = posixpath.join(running_txn_dir, txn_filename)

            # Create mock transaction files in the running directory
            with open(txn_path, "w") as f:
                f.write("mock transaction content")

            # Create a metafile for each transaction using the UUID
            test_metafile_path = posixpath.join(
                catalog_root, f"test_metafile_{txn_uuid}.json"
            )
            with open(test_metafile_path, "w") as f:
                f.write("mock metafile content")

            txn_filenames.append((txn_filename, txn_path, test_metafile_path))

        # Run the janitor function to move timed-out transactions to the failed directory
        janitor_delete_timed_out_transaction(temp_dir)

        # Verify that all transactions were moved to the failed directory
        for i, (txn_filename, txn_path, test_metafile_path) in enumerate(txn_filenames):
            # Get the transaction ID for this transaction
            txn_id, txn_uuid = txn_data[i]

            # Calculate the partitioned path where the file should be moved
            new_failed_txn_path = Transaction.failed_txn_log_path(
                failed_txn_dir, txn_id
            )

            # Check if the renamed transaction file exists in the partitioned failed directory
            assert os.path.exists(
                new_failed_txn_path
            ), f"Expected {new_failed_txn_path} to exist."

            # Check if the transaction file is no longer in the running directory
            assert not os.path.exists(
                txn_path
            ), f"Transaction file {txn_path} still exists in running directory."

            # Check if the corresponding metafile was deleted (if applicable)
            assert not os.path.exists(
                test_metafile_path
            ), f"Metafile {test_metafile_path} was not deleted."

    def test_janitor_remove_files_failed(self, temp_dir):
        # Set up test directories and filesystem
        catalog_root, filesystem = resolve_path_and_filesystem(temp_dir)
        txn_log_dir = posixpath.join(catalog_root, TXN_DIR_NAME)
        failed_txn_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)
        running_txn_dir = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME)

        # Ensure all necessary directories exist
        for dir_path in [failed_txn_dir, running_txn_dir]:
            os.makedirs(dir_path, exist_ok=True)

        # Create metadata for the transaction
        meta_to_create = [
            create_test_namespace(),
            create_test_table(),
        ]

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=meta,
            )
            for meta in meta_to_create
        ]

        transaction = Transaction.of(txn_operations)
        write_paths, success_txn_log_file_path = transaction.commit(temp_dir)

        # Get filename of committed transaction (from success directory)
        filename = posixpath.basename(posixpath.dirname(success_txn_log_file_path))

        # Create empty partitioned failed transaction log file.
        failed_txn_path = posixpath.join(failed_txn_dir, filename)
        failed_txn_path = write_file_partitioned(
            path=failed_txn_path,
            data=b"",
            partition_value=Transaction.parse_transaction_id(filename)[0],
            partition_transform=epoch_timestamp_partition_transform,
            filesystem=filesystem,
        )

        # Move the file from success to failed and running to
        # simulate a failed transaction that hasn't been cleaned up.
        running_txn_path = posixpath.join(running_txn_dir, filename)
        filesystem.copy_file(success_txn_log_file_path, failed_txn_path)
        filesystem.copy_file(success_txn_log_file_path, running_txn_path)

        # Verify that the write path files exist before cleanup.
        for path in write_paths:
            assert os.path.exists(
                path
            ), f"Expected write path {path} to exist before cleanup."

        # Run the cleanup function.
        janitor_remove_files_in_failed(temp_dir, filesystem)

        # Check that all write paths have been deleted.
        for path in write_paths:
            assert not os.path.exists(
                path
            ), f"Write path {path} should have been deleted after cleanup."
