import tempfile
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from deltacat import constants
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
def temp_catalog_properties(temp_dir):
    return CatalogProperties(root=temp_dir)


@pytest.fixture(autouse=True, scope="session")
def temp_deltacat_config_path():

    """
    Force all tests to write their catalog configs to a temporary YAML file,
    rather than the developer's real DELTACAT_CONFIG_PATH.
    """
    tmpfile = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False)
    print(">> Using temp config path:", tmpfile.name)
    tmpfile.close()

    mp = MonkeyPatch()
    mp.setenv("DELTACAT_CONFIG_PATH", tmpfile.name)

    # Also patch the constants module
    constants.DELTACAT_CONFIG_PATH = tmpfile.name

    yield tmpfile.name

    # Undo patches and cleanup
    mp.undo()
    try:
        Path(tmpfile.name).unlink()
    except FileNotFoundError:
        pass
