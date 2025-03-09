import time
import posixpath

import pyarrow.fs
from deltacat.storage.model.transaction import Transaction

from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    list_directory,
    get_file_info,
)

from itertools import chain

from deltacat.constants import TXN_DIR_NAME, RUNNING_TXN_DIR_NAME, FAILED_TXN_DIR_NAME, SUCCESS_TXN_DIR_NAME, TXN_PART_SEPARATOR


def brute_force_search_matching_metafiles (transaction_ids, filesystem: pyarrow.fs.FileSystem):
    txn_dir_name = TXN_DIR_NAME
    def recursive_search(path):
        try:
            entries = filesystem.ls(path)
        except Exception as e:
            print(f"Error listing directory '{path}': {e}")
            return

        for entry in entries:
            # If the entry is a file and its base name matches transaction_id, delete it.
            if entry.is_file:
                for transaction_id in transaction_ids:
                    """
                    i think this is not the most ideal approach because instead of looking for
                    specific matches to metafile names we just look if the transaction id is inside the file name
                    we will refactor this once we figure out how to match a transaction id with its associated metafile
                    """
                    if transaction_id in entry.base_name:
                        try:
                            filesystem.delete_file(entry.path)
                            print(f"Deleted file: {entry.path}")
                        except Exception as e:
                            print(f"Error deleting file '{entry.path}': {e}")

            # If the entry is a directory:
            elif entry.is_dir:
                # Skip the directory if its name is the one we want to ignore.
                if entry.base_name == txn_dir_name:
                    print(f"Skipping directory: {entry.path}")
                    continue
                # Otherwise, recursively search this directory.
                recursive_search(entry.path)
    
    # Start the recursive search from the current directory.
    recursive_search('.')

def janitor_delete_timed_out_transaction(catalog_root: str, threshold_seconds: int = 3600) -> None:
    """
    Traverse the running transactions directory and move transactions that have been
    running longer than threshold_seconds into the failed transactions directory.

    Directory structure expected:
        <catalog_root>/<TXN_DIR_NAME>/<RUNNING_TXN_DIR_NAME>
        <catalog_root>/<TXN_DIR_NAME>/<FAILED_TXN_DIR_NAME>

    Each file in the running directory is expected to be named as:
        "<start_time><TXN_PART_SEPARATOR><transaction_id>"
    """

    catalog_root_normalized, filesystem = resolve_path_and_filesystem(
        catalog_root,
        filesystem,
    )

    txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
    running_txn_log_dir = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME)
    failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)

    dirty_files = []

    filesystem.create_dir(running_txn_log_dir, recursive=False)

    running_txn_file_selector = filesystem.FileSelector(running_txn_log_dir)

    # Get file information for all files in the directory
    running_txn_info_list = filesystem.get_file_info(running_txn_file_selector)  

    for running_txn_info in running_txn_info_list:
        try:
            # TODO: NEED TO ACTUALLY MOVE FILE TO FAILED TRANSACTION DIRECTORY REFERENCE LINE 613 OF TRANSACTION.PY

            # Read the transaction, class method from transaction.py line 349
            
            filename = running_txn_info.base_name
            parts = filename.split(TXN_PART_SEPARATOR)

            start_time_str = parts[0]

            start_time = float(start_time_str)
            current_time = time.time()

            elapsed = current_time - start_time
            if elapsed >= threshold_seconds:
                """
                so basically we just need to manually traverse through the file structure. 
                If any of the metafile id match with the transaction id then we can delete
                call helper function with param transaction id
                """
                src_path = posixpath.join(running_txn_log_dir, filename)
                dest_path = posixpath.join(failed_txn_log_dir, filename)

                # Move the file to the failed transactions directory
                filesystem.move(src_path, dest_path)


                dirty_files.append(parts[1])

        except Exception as e:
            print(f"Error cleaning failed transaction '{running_txn_info.base_name}': {e}")
    """right now we're storing the dirty in progress transactions and brute force searching to
    delete the associated metafiles. The problem with this approach is we would not be able to 
    brute force in the same way if there is an error while the function is running. For instance,
    if the function terminates halfway through and one in progress file gets moved to failed
    we have no way of knowing that we need to brute force the file structure to find its associated 
    metafiles bc our current janitor job for the failed directory assumes we have access to the 
    metafile write paths3297"""
    brute_force_search_matching_metafiles(dirty_files)



# TODO: I think to implement the iteration of each transaction in a directory we can use the children function in 
# metafile.py on line 697 once we have established an absolute path from catalog to the directory
            

def janitor_remove_files_in_failed(catalog_root: str, filesystem: pyarrow.fs.FileSystem) -> None:
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

    failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME) # TODO: Lowkey need to clarify 
    filesystem.create_dir(failed_txn_log_dir, recursive=False)

    failed_txn_file_selector = filesystem.FileSelector(failed_txn_log_dir)

    # Get file information for all files in the directory
    failed_txn_info_list = filesystem.get_file_info(failed_txn_file_selector)  


    """
    failed
        -> [start_time]_[txn uuid]
        -> [start_time]_[txn uuid]
    
    """
    for failed_txn_info in failed_txn_info_list:
        try:
            # Read the transaction, class method from transaction.py line 349
            txn = Transaction.read(failed_txn_info.path, filesystem)

            known_write_paths = chain.from_iterable(
                [
                    operation.metafile_write_paths + operation.locator_write_paths
                    for operation in txn.operations
                ]
            )

            for write_path in known_write_paths:
                filesystem.delete_file(write_path)


            print(f"Cleaned up failed transaction: {failed_txn_info.base_name}")

        except Exception as e:
            print(f"Error cleaning failed transaction '{failed_txn_info.base_name}': {e}")

def janitor_job(catalog_root_dir: str) -> None:
    # TODO: Implement proper heartbeat mechanics
    janitor_delete_timed_out_transaction(catalog_root_dir, threshold_seconds=30)
    janitor_remove_files_in_failed(catalog_root_dir)



