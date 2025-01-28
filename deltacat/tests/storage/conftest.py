import tempfile

import pytest


@pytest.fixture
def keep_temp_dir():
    return tempfile.mkdtemp()
