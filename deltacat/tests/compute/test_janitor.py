import pytest
import os
import time
import posixpath
from test_metafile_io import _commit_single_delta_table 
from delta.constants import (
    RUNNING_TXN_DIR_NAME, FAILED_TXN_DIR_NAME, TXN_DIR_NAME
)
from delta.metafile import MetafileRevisionInfo
from delta.transactions import TransactionOperationType
from delta.filesystem import resolve_path_and_filesystem
from janitor import janitor_job

# referenced test_metafile_io.py a lot, mainly looked at def test_txn_bad_end_time_fails(self, temp_dir, mocker):
# also referenced test_txn_conflict_concurrent_complete

def test_janitor_job_running_to_failed(self, temp_dir):
    commit_results = _commit_single_delta_table(temp_dir)
    for expected, actual, _ in commit_results:
        assert expected.equivalent_to(actual)

    # Given an initial metafile revision of a committed delta
    write_paths = [result[2] for result in commit_results]
    orig_delta_write_path = write_paths[5]

    mri = MetafileRevisionInfo.parse(orig_delta_write_path)
    mri.txn_id = "9999999999999_test-txn-id"
    mri.txn_op_type = TransactionOperationType.DELETE
    mri.revision += 1
    conflict_delta_write_path = mri.path

    # Simulate an incomplete transaction
    _, filesystem = resolve_path_and_filesystem(orig_delta_write_path)
    with filesystem.open_output_stream(conflict_delta_write_path):
        pass  # Just create an empty metafile revision

    # Define directories using posixpath
    txn_log_file_dir = os.path.join(temp_dir, TXN_DIR_NAME, RUNNING_TXN_DIR_NAME, mri.txn_id) #reference line 443 of test_metafile_io.py
    failed_txn_log_dir = os.path.join(temp_dir, TXN_DIR_NAME, FAILED_TXN_DIR_NAME)

    txn_log_file_path = os.path.join(txn_log_file_dir, str(time.time_ns() - 60))

    # Create the failed transaction log directory

    filesystem.create_dir(failed_txn_log_dir, recursive=True)

    # Ensure the original transaction log file exists
    assert filesystem.exists(txn_log_file_path)

    janitor_job(temp_dir)
    # Verify the file was moved to the failed directory
    assert not os.path.exists(posixpath.join(txn_log_file_dir, mri.txn_id))
    assert os.path.exists(posixpath.join(failed_txn_log_dir, mri.txn_id))


def test_remove_files_from_failed(self, temp_dir):
    commit_results = _commit_single_delta_table(temp_dir)
    for expected, actual, _ in commit_results:
        assert expected.equivalent_to(actual)

    txn_log_file_dir = create_failed_dummy_transaction(temp_dir)

    assert os.path.exists(txn_log_file_dir)

    janitor_job(temp_dir)

    assert not os.path.exists(txn_log_file_dir), "Failed transaction file should be removed after janitor job"



def create_failed_dummy_transaction(temp_dir):
    """Creates a dummy transaction, simulates a failure, and moves it to the failed transaction directory."""
    
    commit_results = _commit_single_delta_table(temp_dir)
    
    for expected, actual, _ in commit_results:
        assert expected.equivalent_to(actual)

    write_paths = [result[2] for result in commit_results]
    orig_delta_write_path = write_paths[5]  # Arbitrary index for an existing delta

    mri = MetafileRevisionInfo.parse(orig_delta_write_path)
    mri.txn_id = "9999999999999_test-txn-id" 
    mri.txn_op_type = TransactionOperationType.DELETE  
    mri.revision += 1  

    conflict_delta_write_path = mri.path  

    _, filesystem = resolve_path_and_filesystem(orig_delta_write_path)
    
    with filesystem.open_output_stream(conflict_delta_write_path):
        pass  

    return os.path.join(
            temp_dir,
            TXN_DIR_NAME,
            FAILED_TXN_DIR_NAME,
            mri.txn_id,
        )
    
    

    
    

