import os
import time
import shutil
import posixpath
import logging
import msgpack

from itertools import chain


from deltacat.constants import TXN_DIR_NAME, RUNNING_TXN_DIR_NAME, FAILED_TXN_DIR_NAME, SUCCESS_TXN_DIR_NAME, TXN_PART_SEPARATOR
from deltacat.utils import pyarrow

# TODO: We need to implement functionality that adds the heartbeat time to the transaction id, this will scale with the number of operations that the transaction has
def janitor_move_old_running_transactions(catalog_root: str, threshold_seconds: int = 30) -> None:
    """
    Traverse the running transactions directory and move transactions that have been
    running longer than threshold_seconds into the failed transactions directory.

    Directory structure expected:
        <catalog_root>/<TXN_DIR_NAME>/<RUNNING_TXN_DIR_NAME>
        <catalog_root>/<TXN_DIR_NAME>/<FAILED_TXN_DIR_NAME>

    Each file in the running directory is expected to be named as:
        "<start_time><TXN_PART_SEPARATOR><transaction_id>"
    """
    # Build paths using posixpath.join
    running_dir = posixpath.join(catalog_root, TXN_DIR_NAME, RUNNING_TXN_DIR_NAME)
    failed_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)

    # Ensure the running directory exists; if not, raise an error.
    if not os.path.exists(running_dir):
        raise FileNotFoundError(f"Running transactions directory does not exist: {running_dir}")


    current_time = time.time()

    # Iterate over files in the running directory.
    for filename in os.listdir(running_dir):
        file_path = posixpath.join(running_dir, filename)

        # Skip if not a file.
        if not os.path.isfile(file_path):
            continue

        # Expected file name format: "<start_time><TXN_PART_SEPARATOR><transaction_id>"
        parts = filename.split(TXN_PART_SEPARATOR)
        
        start_time_str = parts[0]
        start_time = float(start_time_str)

        elapsed = current_time - start_time
        if elapsed >= threshold_seconds:
            destination_path = posixpath.join(failed_dir, filename)
            try:
                shutil.move(file_path, destination_path)
            except Exception as e:
                raise RuntimeError(f"Failed to move '{filename}' to failed directory: {e}") from e
            

# TODO: delete every operation of a transaction, reference line 628 of transactions.py
            

def janitor_remove_files_in_failed(txn_dir: str, failed_txn_log_dir: str, filesystem: pyarrow.fs.FileSystem):
    """
    Cleans up metafiles and locator files associated with failed transactions.
    Only processes transactions that have not been successfully committed or are still running.
    txn_dir: Path to directory with all transactions
    failed_txn_log_dir: Path to failed transaction log, which should only contain ids of transactions
    """
    try:
        all_txn_files = filesystem.get_file_info(txn_dir)
    except Exception as e:
        print(f"Failed to list all transaction logs: {e}")
        return

    try:
        failed_txn_files = filesystem.get_file_info(failed_txn_log_dir)
        failed_txn_ids = {posixpath.basename(txn_log_path) for txn_log_path in failed_txn_files}  # Set of failed txn IDs
    except Exception as e:
        print(f"Failed to list failed transaction logs: {e}")
        return

    for txn_log in all_txn_files:
        txn_id = posixpath.basename(txn_log.path)  # Extract transaction ID from the log filename
        txn_log_path = posixpath.join(txn_dir, txn_id)  # Full path to the txn log

        # Check if the transaction is already successfully committed or still running
        success_txn_log_path = posixpath.join(txn_dir, SUCCESS_TXN_DIR_NAME, txn_id)
        running_txn_log_path = posixpath.join(txn_dir, RUNNING_TXN_DIR_NAME, txn_id)

        if filesystem.exists(success_txn_log_path):
            continue  # Skip if already successfully committed

        if filesystem.exists(running_txn_log_path):
            continue  # Skip if the transaction is still running, we know it hasn't failed because first call to 
                        #janitor_move_old_running_transactions will handle this

        # If the transaction is not in the success or running logs, it's a failed transaction
        if txn_id in failed_txn_ids:
            try:
                # this was pretty much taken directly out of _commit_write in transactions.py
                with filesystem.open_input_stream(txn_log_path) as file:
                    txn_data = msgpack.loads(file.read())  

                # changed logic a little bit from _commit_write but same idea
                metafile_paths = list(
                    chain.from_iterable(op["metafile_write_paths"] for op in txn_data["operations"])
                )
                locator_paths = list(
                    chain.from_iterable(op["locator_write_paths"] for op in txn_data["operations"])
                )

                for path in metafile_paths + locator_paths:
                    try:
                        filesystem.delete_file(path)
                    except Exception as e:
                        print(f"Failed to delete file `{path}`: {e}")

                filesystem.delete_file(txn_log_path)

            except Exception as e:
                print(f"Error processing failed transaction `{txn_id}`: {e}")


def janitor_job(catalog_root_dir: str) -> None:
    # TODO: Implement proper heartbeat mechanics
    janitor_move_old_running_transactions(catalog_root_dir, threshold_seconds=30)
    janitor_remove_files_in_failed(catalog_root_dir)
