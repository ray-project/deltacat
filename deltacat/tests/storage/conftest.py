import shutil
import tempfile

import pytest

from contextlib import contextmanager

from deltacat.tests.test_utils.filesystem import temp_dir_autocleanup


@pytest.fixture
def temp_dir():
    with temp_dir_autocleanup() as tmp_dir:
        yield tmp_dir

@pytest.fixture
def keep_temp_dir():
    return tempfile.mkdtemp()
