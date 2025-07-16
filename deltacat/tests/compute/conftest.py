import tempfile
import shutil

import pytest
from deltacat.catalog.model.properties import CatalogProperties


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
def main_deltacat_storage_kwargs(temp_dir):
    """
    Fixture that creates a CatalogProperties object for each test function
    using the main metastore implementation and cleans up afterwards.

    Returns:
        dict: A dictionary with 'inner' key pointing to CatalogProperties
    """
    catalog = CatalogProperties(root=temp_dir)
    kwargs = {"inner": catalog}
    yield kwargs

    # Cleanup happens automatically via temp_dir fixture
