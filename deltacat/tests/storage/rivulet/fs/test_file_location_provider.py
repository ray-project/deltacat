import pytest
import os
import tempfile
from deltacat.storage.rivulet.fs.file_location_provider import FileLocationProvider

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp:
        yield temp

@pytest.fixture
def file_location_provider(temp_dir):
    return FileLocationProvider(uri=temp_dir)

def test_new_data_file_uri(file_location_provider, temp_dir):
    output_file = file_location_provider.new_data_file_uri("parquet")
    assert output_file.location.startswith(os.path.join(temp_dir, "data"))
    assert output_file.location.endswith(".parquet")

    output_file2 = file_location_provider.new_data_file_uri("parquet")
    assert output_file2.location.startswith(os.path.join(temp_dir, "data"))
    assert output_file2.location.endswith(".parquet")

    assert output_file.location != output_file2.location, "Two output files should have different locations."

def test_new_l0_sst_file_uri(file_location_provider, temp_dir):
    output_file = file_location_provider.new_l0_sst_file_uri()
    assert output_file.location.startswith(os.path.join(temp_dir, "metadata/ssts/0"))
    assert output_file.location.endswith(".json")

    output_file2 = file_location_provider.new_l0_sst_file_uri()
    assert output_file2.location.startswith(os.path.join(temp_dir, "metadata/ssts/0"))
    assert output_file2.location.endswith(".json")

    assert output_file.location != output_file2.location, "Two output files should have different locations."

def test_new_manifest_file_uri(file_location_provider, temp_dir):
    output_file = file_location_provider.new_manifest_file_uri()
    assert output_file.location.startswith(os.path.join(temp_dir, "metadata/manifests"))
    assert output_file.location.endswith(".json")

    output_file2 = file_location_provider.new_manifest_file_uri()
    assert output_file2.location.startswith(os.path.join(temp_dir, "metadata/manifests"))
    assert output_file2.location.endswith(".json")

    assert output_file.location != output_file2.location, "Two output files should have different locations."

def test_get_sst_scan_directories(file_location_provider, temp_dir):
    sst_dirs = file_location_provider.get_sst_scan_directories()
    expected_dir = os.path.join(temp_dir, "metadata/ssts/0/")
    assert len(sst_dirs) == 1
    assert sst_dirs[0] == expected_dir

def test_get_manifest_scan_directories(file_location_provider, temp_dir):
    manifest_dirs = file_location_provider.get_manifest_scan_directories()
    expected_dir = os.path.join(temp_dir, "metadata/manifests/")
    assert len(manifest_dirs) == 1
    assert manifest_dirs[0] == expected_dir

def test_generate_manifest_uris(file_location_provider, temp_dir):
    # Create a sample manifest file
    manifest_dir = os.path.join(temp_dir, "metadata/manifests")
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_file = os.path.join(manifest_dir, "sample_manifest.json")
    with open(manifest_file, "w") as f:
        f.write("{}")

    manifest_uris = list(file_location_provider.generate_manifest_uris())
    assert len(manifest_uris) == 1
    assert manifest_uris[0].location == manifest_file

def test_generate_sst_uris(file_location_provider, temp_dir):
    # Create a sample SST file
    sst_dir = os.path.join(temp_dir, "metadata/ssts/0")
    os.makedirs(sst_dir, exist_ok=True)
    sst_file = os.path.join(sst_dir, "sample_sst.json")
    with open(sst_file, "w") as f:
        f.write("{}")

    sst_uris = list(file_location_provider.generate_sst_uris())
    assert len(sst_uris) == 1
    assert sst_uris[0].location == sst_file
