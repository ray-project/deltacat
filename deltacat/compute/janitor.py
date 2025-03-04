import os
import time
import shutil
import posixpath
import logging

from deltacat.constants import TXN_DIR_NAME, RUNNING_TXN_DIR_NAME, FAILED_TXN_DIR_NAME, TXN_PART_SEPARATOR


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

    # Create failed directory if it doesnt exist
    #lwk is this even needed
    os.makedirs(failed_dir, exist_ok=True)

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
            
def janitor_remove_files_in_failed(catalog_root: str) -> None:
    """
    Traverse the failed transactions directory and remove any file that has not been yet been removed
    """


    failed_dir = posixpath.join(catalog_root, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)

    for filename in os.listdir(running_dir):
        file_path = posixpath.join(running_dir, filename)
        

    

def janitor_job(catalog_root_dir):
    janitor_move_old_running_transactions(catalog_root_dir, threshold_seconds=30)
