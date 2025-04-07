import os
import tempfile
import shutil
import pytest


@pytest.fixture
def temp_dir():
    """
    Fixture that creates a temporary directory for tests and cleans it up afterwards.

    Returns:
        str: Path to the temporary directory
    """
    # Create a temporary directory
    dir_path = tempfile.mkdtemp()

    # Provide the directory path to the test
    yield dir_path

    # Cleanup: remove the directory after the test is done
    shutil.rmtree(dir_path)


@pytest.fixture(scope="function")
def local_deltacat_storage_kwargs(temp_dir):
    """
    Fixture that creates a temporary database file for each test function
    and returns storage kwargs dictionary.
    
    Returns:
        dict: A dictionary with db_file_path key pointing to a temporary database file
    """
    # Create a unique database file in the temporary directory
    db_file_path = os.path.join(temp_dir, "db_test.sqlite")
    
    # Return kwargs dictionary ready to use
    kwargs = {"db_file_path": db_file_path}
    yield kwargs
    
    # Cleanup: remove the database file if it exists
    if os.path.exists(db_file_path):
        os.remove(db_file_path)