import os
import time
import shutil
import posixpath
import logging
import msgpack
import pyarrow.fs
from deltacat.storage.model.locator import Locator 
from deltacat.storage.model.transaction import read

from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    list_directory,
    get_file_info,
)

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
            

# TODO: I think to implement the iteration of each transaction in a directory we can use the children function in 
# metafile.py on line 697 once we have established an absolute path from catalog to the directory
            

def janitor_remove_files_in_failed(catalog_root: str, filesystem: pyarrow.fs.FileSystem):
    """
    Cleans up metafiles and locator files associated with failed transactions.
    Only processes transactions that have not been successfully committed or are still running.
    txn_dir: Path to directory with all transactions
    failed_txn_log_dir: Path to failed transaction log, which should only contain ids of transactions
    """
    catalog_root_normalized, filesystem = resolve_path_and_filesystem(
        catalog_root,
        filesystem,
    )

    txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)

    failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)
    filesystem.create_dir(failed_txn_log_dir, recursive=False)

    failed_txn_file_selector = filesystem.FileSelector(failed_txn_log_dir)

    # Get file information for all files in the directory
    failed_txn_info_list = filesystem.get_file_info(failed_txn_file_selector)  

    for failed_txn_info in failed_txn_info_list:
        try:
            # Read the transaction, class method from transaction.py line 349
            txn = read(failed_txn_info.path, filesystem)

            known_write_paths = chain.from_iterable(
                [
                    operation.metafile_write_paths + operation.locator_write_paths
                    for operation in txn.operations
                ]
            )

            for write_path in known_write_paths:
                filesystem.delete_file(write_path)

            # Delete the failed transaction log file
            filesystem.delete_file(failed_txn_info.path)

            print(f"Cleaned up failed transaction: {failed_txn_info.base_name}")

        except Exception as e:
            print(f"Error cleaning failed transaction '{failed_txn_info.base_name}': {e}")

def janitor_job(catalog_root_dir: str) -> None:
    # TODO: Implement proper heartbeat mechanics
    janitor_move_old_running_transactions(catalog_root_dir, threshold_seconds=30)
    janitor_remove_files_in_failed(catalog_root_dir)
