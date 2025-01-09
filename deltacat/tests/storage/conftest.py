import os
import shutil
import tempfile
import uuid

import pytest

from contextlib import contextmanager


@contextmanager
@pytest.fixture
def make_tmpdir():
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)


@pytest.fixture
def keep_tmpdir():
    temp_dir = tempfile.gettempdir()
    temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
    yield temp_dir
    f"Skipping Cleanup of Test Directory: {temp_dir}"
