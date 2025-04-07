import os
import tempfile
import shutil
from typing import Dict

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


def create_local_deltacat_storage_file() -> Dict[str, str]:
    """
    Helper function to create a local deltacat storage file

    Essentially uses the same approach as local_deltacat_storage_kwargs, but more flexible
    if the consumer does not want to use a function scoped fixture

    Returns: kwargs to use for local deltacat storage, i.e. {"db_file_path": $db_file}
    """
    temp_dir = tempfile.mkdtemp()
    db_file_path = os.path.join(temp_dir, "db_test.sqlite")
    return {"db_file_path": db_file_path}


def clean_up_local_deltacat_storage_file(local_storage_kwargs: Dict[str, str]):
    """
    Cleans up local file and directory created by create_local_deltacat_storage_file
    """
    db_file = local_storage_kwargs["db_file_path"]
    dir_path = os.path.dirname(db_file)

    # Remove the database file if it exists
    if os.path.exists(db_file):
        os.remove(db_file)

    # Remove the temporary directory if it exists
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
