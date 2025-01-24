import shutil
import pytest
import tempfile
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.file_provider import FileProvider


@pytest.fixture(params=["local"])
def filesystem_and_base_uri(request):
    """
    Parameterized fixture for testing with different filesystems.

    Supported filesystems:
    - local: Uses a temporary directory on the local filesystem.
    - s3: Uses an S3 bucket (requires AWS credentials and a valid bucket).

    Yields a tuple of (filesystem, base_uri).
    """
    if request.param == "local":
        temp_dir = tempfile.mkdtemp()
        path, filesystem = FileStore.filesystem(temp_dir)
        yield path, filesystem
        shutil.rmtree(temp_dir)
    elif request.param == "s3":
        # TODO: get this working with test aws account.
        base_uri = "s3://your-test-bucket/prefix"
        path, filesystem = FileStore.filesystem(base_uri)
        yield path, filesystem


@pytest.fixture
def file_provider(filesystem_and_base_uri):
    path, filesystem = filesystem_and_base_uri
    file_store = FileStore(path, filesystem)
    return FileProvider(uri=path, file_store=file_store)


def test_provide_data_file(file_provider, filesystem_and_base_uri):
    path, _ = filesystem_and_base_uri
    output_file = file_provider.provide_data_file("parquet")
    assert output_file.location.startswith(f"{path}/data")
    assert output_file.location.endswith(".parquet")

    output_file2 = file_provider.provide_data_file("parquet")
    assert output_file2.location.startswith(f"{path}/data")
    assert output_file2.location.endswith(".parquet")

    assert (
        output_file.location != output_file2.location
    ), "Two output files should have different locations."


def test_provide_l0_sst_file(file_provider, filesystem_and_base_uri):
    path, _ = filesystem_and_base_uri
    output_file = file_provider.provide_l0_sst_file()
    assert output_file.location.startswith(f"{path}/metadata/ssts/0")
    assert output_file.location.endswith(".json")

    output_file2 = file_provider.provide_l0_sst_file()
    assert output_file2.location.startswith(f"{path}/metadata/ssts/0")
    assert output_file2.location.endswith(".json")

    assert (
        output_file.location != output_file2.location
    ), "Two output files should have different locations."


def test_provide_manifest_file(file_provider, filesystem_and_base_uri):
    path, _ = filesystem_and_base_uri
    output_file = file_provider.provide_manifest_file()
    assert output_file.location.startswith(f"{path}/metadata/manifests")
    assert output_file.location.endswith(".json")

    output_file2 = file_provider.provide_manifest_file()
    assert output_file2.location.startswith(f"{path}/metadata/manifests")
    assert output_file2.location.endswith(".json")

    assert (
        output_file.location != output_file2.location
    ), "Two output files should have different locations."


def test_get_sst_scan_directories(file_provider, filesystem_and_base_uri):
    path, _ = filesystem_and_base_uri
    sst_dirs = file_provider.get_sst_scan_directories()
    expected_dir = f"{path}/metadata/ssts/0/"
    assert len(sst_dirs) == 1
    assert sst_dirs[0] == expected_dir


def test_get_manifest_scan_directories(file_provider, filesystem_and_base_uri):
    path, _ = filesystem_and_base_uri
    manifest_dirs = file_provider.get_manifest_scan_directories()
    expected_dir = f"{path}/metadata/manifests/"
    assert len(manifest_dirs) == 1
    assert manifest_dirs[0] == expected_dir


def test_generate_manifest_uris(file_provider, filesystem_and_base_uri):
    path, filesystem = filesystem_and_base_uri
    # Create a sample manifest file
    manifest_dir = f"{path}/metadata/manifests"
    filesystem.create_dir(manifest_dir)
    manifest_file = f"{manifest_dir}/sample_manifest.json"
    with filesystem.open_output_stream(manifest_file) as f:
        f.write(b"{}")

    manifest_uris = list(file_provider.generate_manifest_uris())
    assert len(manifest_uris) == 1
    assert manifest_uris[0].location == manifest_file


def test_generate_sst_uris(file_provider, filesystem_and_base_uri):
    path, filesystem = filesystem_and_base_uri
    # Create a sample SST file
    sst_dir = f"{path}/metadata/ssts/0"
    filesystem.create_dir(sst_dir)
    sst_file = f"{sst_dir}/sample_sst.json"
    with filesystem.open_output_stream(sst_file) as f:
        f.write(b"{}")

    sst_uris = list(file_provider.generate_sst_uris())
    assert len(sst_uris) == 1
    assert sst_uris[0].location == sst_file


def test_provide_input_file(file_provider, filesystem_and_base_uri):
    path, filesystem = filesystem_and_base_uri
    file_path = f"{path}/data/sample_file.txt"
    filesystem.create_dir(f"{path}/data")
    with filesystem.open_output_stream(file_path) as f:
        f.write(b"test content")

    input_file = file_provider.provide_input_file(file_path)
    with input_file.open() as f:
        content = f.read()
        assert content.decode() == "test content"
