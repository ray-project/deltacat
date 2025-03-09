import pytest
from deltacat.storage.model.transaction import *
import time

import posixpath
from deltacat.tests.storage.model.test_metafile_io import _commit_single_delta_table 
from deltacat.constants import (
    RUNNING_TXN_DIR_NAME, FAILED_TXN_DIR_NAME, TXN_DIR_NAME, TXN_PART_SEPARATOR
)
from deltacat.utils.filesystem import resolve_path_and_filesystem
from deltacat.compute.janitor import janitor_job, janitor_remove_files_in_failed, janitor_delete_timed_out_transaction

from unittest.mock import MagicMock

@pytest.fixture
def temp_dir():
    # tmpdir is a pytest fixture that automatically provides a temporary directory
    catalog_root = "/catalog/root"
    return catalog_root  # return the string path of the temporary directory

def test_remove_files_from_failed(mocker, temp_dir: str): 
# Create a dummy transaction operation with absolute paths
    #<start_time><TXN_PART_SEPARATOR><transaction_id>"
     # Create a mock filesystem instance
    mock_fs = MagicMock(spec=pyarrow.fs.FileSystem)
    mock_fs.create_dir.return_value = None  # Simulate a successful directory creation
    
    # Patch the LocalFileSystem class to return the mocked filesystem
    mocker.patch('pyarrow.fs.LocalFileSystem', return_value=mock_fs)
    
    # Initialize filesystem to the mock_fs
    filesystem = mock_fs
    
    # Call the function that needs the filesystem
    catalog_root_normalized, filesystem = resolve_path_and_filesystem(
        temp_dir,
        filesystem,
    )


    txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
    failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)
    filesystem.create_dir(failed_txn_log_dir, recursive=True)

    id = str(time.time()) + TXN_PART_SEPARATOR + "9999999999999_test-txn-id"

    failed_txn_log_file_path = posixpath.join(
                failed_txn_log_dir,
                id,
            )
    
    dest_metafile = Metafile({"id": id}) # do we really need to use this?

    transaction_operation = Transaction.of(
        operation_type=TransactionOperationType.CREATE
    )


    with filesystem.open_output_stream(failed_txn_log_file_path) as file:
            packed = msgpack.dumps(transaction_operation.to_serializable()) 
            file.write(packed)

    transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[transaction_operation]
        )
    
    transaction.id() # when we first instantiate a transaction the id field doesn't get set, this will set it
    
    known_write_paths = chain.from_iterable(
                [
                    operation.metafile_write_paths + operation.locator_write_paths
                    for operation in transaction.operations
                ]
            )
    
    print("known write paths before job", known_write_paths)

    janitor_remove_files_in_failed(temp_dir)

    print("known write paths after job:", known_write_paths)

    #assert( known_write_paths) #known_write paths don't exist and go bye bye?
    
           
def test_heartbeat_moves_file(mocker, temp_dir: str):
     # Create a mock filesystem instance
    mock_fs = MagicMock(spec=pyarrow.fs.FileSystem)
    mock_fs.create_dir.return_value = None  # Simulate a successful directory creation
    
    # Patch the LocalFileSystem class to return the mocked filesystem
    mocker.patch('pyarrow.fs.LocalFileSystem', return_value=mock_fs)
    
    # Initialize filesystem to the mock_fs
    filesystem = mock_fs
    
    # Call the function that needs the filesystem
    catalog_root_normalized, filesystem = resolve_path_and_filesystem(
        temp_dir,
        filesystem,
    )


    txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
    running_txn_log_dir = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME)
    filesystem.create_dir(running_txn_log_dir, recursive=True)

    id = str(time.time() - 4000) + TXN_PART_SEPARATOR + "9999999999999_test-txn-id"

    running_txn_log_file_path = posixpath.join(
                running_txn_log_dir,
                id,
            )
    
    dest_metafile = Metafile({"id": id}) # do we really need to use this?

    transaction_operation = Transaction.of(
        operation_type=TransactionOperationType.CREATE ### I DON'T THINK THIS WORKS BUT FUCK IT WE BALLLLLLL
    )

    transaction_operation.id = id

    with filesystem.open_output_stream(running_txn_log_file_path) as file:
            packed = msgpack.dumps(transaction_operation.to_serializable()) 
            file.write(packed)

    transaction = Transaction.of(
            txn_type=TransactionType.APPEND, txn_operations=[transaction_operation]
        )

    known_write_paths = chain.from_iterable(
                [
                    operation.metafile_write_paths + operation.locator_write_paths
                    for operation in transaction.operations
                ]
            )
    
    known_write_paths = chain.from_iterable(
                [
                    operation.metafile_write_paths + operation.locator_write_paths
                    for operation in transaction.operations
                ]
            )
    
    assert(known_write_paths) #exist?

    janitor_delete_timed_out_transaction(temp_dir)

    assert(not known_write_paths) # does exist and go bye bye?






    