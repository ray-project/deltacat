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
