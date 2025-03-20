import time
import os
import posixpath
import pyarrow.fs
from pyarrow.fs import FileSelector, FileType
from itertools import chain
from deltacat.storage.model.transaction import Transaction
from deltacat.utils.filesystem import resolve_path_and_filesystem
from deltacat.constants import (
    CURRENTLY_CLEANING,
    SUCCESSFULLY_CLEANED,
    TIMEOUT_TXN,
    TXN_DIR_NAME, 
    RUNNING_TXN_DIR_NAME, 
    FAILED_TXN_DIR_NAME, 
    TXN_PART_SEPARATOR,
)


def brute_force_search_matching_metafiles(dirty_files_names, filesystem: pyarrow.fs.FileSystem, catalog_root):
    txn_dir_name = TXN_DIR_NAME
     #collect transaction ids of the files
    transaction_ids = []
    for dirty_file in dirty_files_names:
        parts = dirty_file.split(TXN_PART_SEPARATOR)
        if len(parts) < 2:
            continue
        transaction_ids.append(parts[1])

    def recursive_search(path):
        try:
            selector = FileSelector(path, recursive=False)
            entries = filesystem.get_file_info(selector)
        except Exception as e:
            print(f"Error listing directory '{path}': {e}")
            return
       
        for entry in entries:
            base_name = posixpath.basename(entry.path)
            if entry.type == FileType.File:
                    for transaction_id in transaction_ids:
                    # Look for transaction_id in the filename
                        if transaction_id in base_name:
                            try:
                                filesystem.delete_file(entry.path)
                                print(f"Deleted file: {entry.path}")
                            except Exception as e:
                                print(f"Error deleting file '{entry.path}': {e}")

            elif entry.type == FileType.Directory:
                # Skip directories that match txn_dir_name
                if posixpath.basename(entry.path) == txn_dir_name:
                    print(f"Skipping directory: {entry.path}")
                    continue
                recursive_search(entry.path)

    # Start recursive search from the catalog root
    recursive_search(catalog_root)

    #renaming to successful completion
    for dirty_file in dirty_files_names:
        failed_txn_log_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)
        old_log_path = posixpath.join(failed_txn_log_dir, dirty_file)
        if dirty_file.endswith(TIMEOUT_TXN):
            new_filename = dirty_file.replace(TIMEOUT_TXN, SUCCESSFULLY_CLEANED)
            new_log_path = posixpath.join(failed_txn_log_dir, new_filename)
            try:
                filesystem.move(old_log_path, new_log_path)
                print(f"Renamed file from {old_log_path} to {new_log_path}")
            except Exception as e:
                print(f"Error renaming file '{old_log_path}': {e}")



def janitor_delete_timed_out_transaction(catalog_root: str) -> None:
    """
    Traverse the running transactions directory and move transactions that have been
    running longer than the threshold into the failed transactions directory.
    """
    catalog_root_normalized, filesystem = resolve_path_and_filesystem(catalog_root)

    txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
    running_txn_log_dir = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME)
    failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)

    dirty_files = []

    running_txn_file_selector = FileSelector(running_txn_log_dir, recursive=False)
    running_txn_info_list = filesystem.get_file_info(running_txn_file_selector)

    for running_txn_info in running_txn_info_list:
        try:
            filename = posixpath.basename(running_txn_info.path)
            parts = filename.split(TXN_PART_SEPARATOR)
            end_time_str = parts[-1]
            end_time = float(end_time_str)
            current_time = time.time_ns()
            print(current_time)
            if end_time <= current_time:
                src_path = running_txn_info.path
                new_filename = f"{filename}{TXN_PART_SEPARATOR}{TIMEOUT_TXN}"
                dest_path = posixpath.join(failed_txn_log_dir, new_filename)

                # Move the file using copy and delete
                with filesystem.open_input_file(src_path) as src_file:
                    contents = src_file.read()
                with filesystem.open_output_stream(dest_path) as dest_file:
                    dest_file.write(contents)
                filesystem.delete_file(src_path)

                dirty_files.append(new_filename)

        except Exception as e:
            print(f"Error cleaning failed transaction '{running_txn_info.path}': {e}")

    # Pass catalog_root to the brute force search so it searches from the right place
    brute_force_search_matching_metafiles(dirty_files, filesystem, catalog_root_normalized)


def janitor_remove_files_in_failed(catalog_root: str, filesystem: pyarrow.fs.FileSystem = None) -> None:
    """
    Cleans up metafiles and locator files associated with failed transactions.
    """
    if filesystem is None:
        catalog_root_normalized, filesystem = resolve_path_and_filesystem(catalog_root)
    else:
        catalog_root_normalized, filesystem = resolve_path_and_filesystem(catalog_root, filesystem)

    txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
    failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)
    filesystem.create_dir(failed_txn_log_dir, recursive=True)

    failed_txn_file_selector = FileSelector(failed_txn_log_dir, recursive=False)
    failed_txn_info_list = filesystem.get_file_info(failed_txn_file_selector)

    for failed_txn_info in failed_txn_info_list:
        try:
            failed_txn_basename = posixpath.basename(failed_txn_info.path)
            parts = failed_txn_basename.split(TXN_PART_SEPARATOR)
            should_process = True
            try: 
                if parts[-1] == SUCCESSFULLY_CLEANED:
                    should_process = False
            except Exception as e:
                print("Could not check attribute")
            if should_process:
                # Process if the file is marked as currently cleaning.
                if parts[-1] == CURRENTLY_CLEANING:
                    txn = Transaction.read(failed_txn_info.path, filesystem)
                    txnid = txn.id

                    operations = txn["operations"]

                    known_write_paths = chain.from_iterable(
                        (
                            op["metafile_write_paths"] + op["locator_write_paths"]
                        )
                        for op in operations
                    )

                    for write_path in known_write_paths:
                        try:
                            filesystem.delete_file(write_path)
                        except Exception as e:
                            print(f"Failed to delete file '{write_path}': {e}")

                    new_filename = f"{txnid}{TXN_PART_SEPARATOR}{SUCCESSFULLY_CLEANED}"
                    new_failed_txn_log_file_path = posixpath.join(failed_txn_log_dir, new_filename)
                    os.rename(failed_txn_info.path, new_failed_txn_log_file_path)
                    print(f"Cleaned up failed transaction: {failed_txn_basename}")
                
                # If the file is marked with TIMEOUT_TXN (third token), call the brute-force search.
                if parts[-1] == TIMEOUT_TXN:
                    brute_force_search_matching_metafiles([failed_txn_basename], filesystem, catalog_root)
        except Exception as e:
            print(f"Could not read transaction '{failed_txn_info.path}', skipping: {e}")


def janitor_job(catalog_root_dir: str) -> None:
    janitor_delete_timed_out_transaction(catalog_root_dir)
    janitor_remove_files_in_failed(catalog_root_dir)
