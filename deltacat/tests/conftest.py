import tempfile

import pytest
from deltacat.catalog import CatalogProperties
from deltacat.tests.test_utils.filesystem import temp_dir_autocleanup


@pytest.fixture
def temp_dir():
    """
    Temp dir which is removed after usage
    note that each method which is injected with temp_dir will get a separate new tmp directory
    """
    with temp_dir_autocleanup() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def keep_temp_dir():
    return tempfile.mkdtemp()


@pytest.fixture
def temp_catalog(temp_dir):
    return CatalogProperties(root=temp_dir)
