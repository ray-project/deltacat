import pytest
import os

from deltacat.storage.rivulet.fs.file_provider import FileProvider
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.delta import DeltacatManifestIO
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat.tests.test_utils.filesystem import temp_dir_autocleanup


@pytest.fixture
def sample_schema():
    return Schema(
        {("id", Datatype.int32()), ("name", Datatype.string())},
        "id",
    )


@pytest.fixture
def temp_dir_metastore_e2e():
    with temp_dir_autocleanup() as tmp_dir:
        yield tmp_dir


def test_dataset_metastore_e2e(temp_dir_metastore_e2e, sample_schema):
    # Setup
    temp_dir = temp_dir_metastore_e2e
    file_store = FileProvider(temp_dir, FileStore(temp_dir))
    manifest_io = DeltacatManifestIO(temp_dir)

    # Create multiple manifests
    manifests_data = [
        {"sst_files": ["sst1.sst", "sst2.sst"], "level": 1},
        {"sst_files": ["sst3.sst", "sst4.sst"], "level": 2},
    ]

    # Create SST files and manifests
    manifest_paths = []
    for manifest_data in manifests_data:
        sst_files = manifest_data["sst_files"]
        for sst in sst_files:
            with open(os.path.join(temp_dir, sst), "w") as f:
                f.write("test data")

        manifest_path = manifest_io.write(
            sst_files, sample_schema, manifest_data["level"]
        )
        manifest_paths.append(manifest_path)

    # Initialize DatasetMetastore
    metastore = DatasetMetastore(temp_dir, file_store, manifest_io=manifest_io)

    # Test manifest generation
    manifest_accessors = list(metastore.generate_manifests())
    assert len(manifest_accessors) == len(manifests_data)

    # Verify each manifest accessor
    for accessor in manifest_accessors:
        assert accessor.context.schema == sample_schema
        manifests_data_index = 0 if accessor.context.level == 1 else 1
        assert accessor.context.level == manifests_data[manifests_data_index]["level"]
        assert (
            accessor.manifest.sst_files
            == manifests_data[manifests_data_index]["sst_files"]
        )
