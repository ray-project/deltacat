import tempfile

import pytest


from deltacat.tests.test_utils.filesystem import temp_dir_autocleanup

from deltacat.catalog.main.impl import PropertyCatalog


@pytest.fixture
def temp_dir():
    with temp_dir_autocleanup() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def keep_temp_dir():
    return tempfile.mkdtemp()


@contextmanager
@pytest.fixture
def temp_catalog(temp_dir):
    return PropertyCatalog(temp_dir)
