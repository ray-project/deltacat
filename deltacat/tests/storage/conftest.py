import shutil
import tempfile

import pytest

from contextlib import contextmanager


@contextmanager
@pytest.fixture
def temp_dir():
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)


@pytest.fixture
def keep_temp_dir():
    return tempfile.mkdtemp()
