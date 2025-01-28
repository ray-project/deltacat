import pytest
import os
import tempfile
from deltacat.storage.rivulet.fs.file_provider import FileProvider
from deltacat.storage.rivulet.fs.file_store import FileStore
from pyarrow.fs import LocalFileSystem

from deltacat.tests.test_utils.filesystem import temp_dir_autocleanup


@pytest.fixture
def temp_dir():
    with temp_dir_autocleanup() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def file_provider(temp_dir):
    filesystem = LocalFileSystem()
    file_store = FileStore(temp_dir, filesystem)
    return FileProvider(uri=temp_dir, file_store=file_store)


def test_provide_data_file(file_provider, temp_dir):
    output_file = file_provider.provide_data_file("parquet")
    assert output_file.location.startswith(os.path.join(temp_dir, "data"))
    assert output_file.location.endswith(".parquet")

    output_file2 = file_provider.provide_data_file("parquet")
    assert output_file2.location.startswith(os.path.join(temp_dir, "data"))
    assert output_file2.location.endswith(".parquet")

    assert (
        output_file.location != output_file2.location
    ), "Two output files should have different locations."


def test_provide_l0_sst_file(file_provider, temp_dir):
    output_file = file_provider.provide_l0_sst_file()
    assert output_file.location.startswith(os.path.join(temp_dir, "metadata/ssts/0"))
    assert output_file.location.endswith(".json")

    output_file2 = file_provider.provide_l0_sst_file()
    assert output_file2.location.startswith(os.path.join(temp_dir, "metadata/ssts/0"))
    assert output_file2.location.endswith(".json")

    assert (
        output_file.location != output_file2.location
    ), "Two output files should have different locations."


def test_provide_manifest_file(file_provider, temp_dir):
    output_file = file_provider.provide_manifest_file()
    assert output_file.location.startswith(os.path.join(temp_dir, "metadata/manifests"))
    assert output_file.location.endswith(".json")

    output_file2 = file_provider.provide_manifest_file()
    assert output_file2.location.startswith(
        os.path.join(temp_dir, "metadata/manifests")
    )
    assert output_file2.location.endswith(".json")

    assert (
        output_file.location != output_file2.location
    ), "Two output files should have different locations."


def test_get_sst_scan_directories(file_provider, temp_dir):
    sst_dirs = file_provider.get_sst_scan_directories()
    expected_dir = os.path.join(temp_dir, "metadata/ssts/0/")
    assert len(sst_dirs) == 1
    assert sst_dirs[0] == expected_dir


def test_get_manifest_scan_directories(file_provider, temp_dir):
    manifest_dirs = file_provider.get_manifest_scan_directories()
    expected_dir = os.path.join(temp_dir, "metadata/manifests/")
    assert len(manifest_dirs) == 1
    assert manifest_dirs[0] == expected_dir


def test_generate_manifest_uris(file_provider, temp_dir):
    manifest_dir = os.path.join(temp_dir, "metadata/manifests")
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_file = os.path.join(manifest_dir, "sample_manifest.json")
    with open(manifest_file, "w") as f:
        f.write("{}")

    manifest_uris = list(file_provider.generate_manifest_uris())
    assert len(manifest_uris) == 1
    assert manifest_uris[0].location == manifest_file


def test_generate_sst_uris(file_provider, temp_dir):
    sst_dir = os.path.join(temp_dir, "metadata/ssts/0")
    os.makedirs(sst_dir, exist_ok=True)
    sst_file = os.path.join(sst_dir, "sample_sst.json")
    with open(sst_file, "w") as f:
        f.write("{}")

    sst_uris = list(file_provider.generate_sst_uris())
    assert len(sst_uris) == 1
    assert sst_uris[0].location == sst_file


def test_provide_input_file(file_provider, temp_dir):
    file_path = os.path.join(temp_dir, "data/sample_file.txt")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_content = "test content"
    with open(file_path, "w") as f:
        f.write(file_content)

    input_file = file_provider.provide_input_file(file_path)
    with input_file.open() as f:
        content = f.read()
        assert content.decode() == file_content


def test_empty_sst_and_manifest_directories(file_provider, temp_dir):
    os.makedirs(os.path.join(temp_dir, "metadata/ssts/0"), exist_ok=True)
    os.makedirs(os.path.join(temp_dir, "metadata/manifests"), exist_ok=True)

    sst_uris = list(file_provider.generate_sst_uris())
    manifest_uris = list(file_provider.generate_manifest_uris())

    assert len(sst_uris) == 0
    assert len(manifest_uris) == 0
