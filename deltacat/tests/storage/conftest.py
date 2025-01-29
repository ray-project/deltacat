import tempfile

import pytest
from deltacat.catalog.main.impl import PropertyCatalog


@pytest.fixture
def keep_temp_dir():
    return tempfile.mkdtemp()


@pytest.fixture
def temp_catalog(temp_dir):
    return PropertyCatalog(temp_dir)
