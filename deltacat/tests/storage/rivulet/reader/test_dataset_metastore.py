import pytest
import os
from typing import Set

from deltacat.storage.rivulet.metastore.json_sst import JsonSstReader
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.delta import DeltaContext, RivuletDelta, DeltacatManifestIO
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.reader.dataset_metastore import ManifestAccessor, DatasetMetastore


@pytest.fixture
def sample_schema():
    return Schema(
        {("id", Datatype.int32()), ("name", Datatype.string())},
        "id",
    )


def test_dataset_metastore_e2e(temp_dir, sample_schema):
    # Setup
    file_store = FileStore()
    manifest_io = DeltacatManifestIO(temp_dir)

    # Create multiple manifests
    manifests_data = [
        {
            "sst_files": ["sst1.sst", "sst2.sst"],
            "level": 1
        },
        {
            "sst_files": ["sst3.sst", "sst4.sst"],
            "level": 2
        }
    ]

    # Create SST files and manifests
    manifest_paths = []
    for manifest_data in manifests_data:
        sst_files = manifest_data["sst_files"]
        for sst in sst_files:
            with open(os.path.join(temp_dir, sst), 'w') as f:
                f.write('test data')

        manifest_path = manifest_io.write(
            sst_files,
            sample_schema,
            manifest_data["level"]
        )
        manifest_paths.append(manifest_path)

    # Initialize DatasetMetastore
    metastore = DatasetMetastore(
        temp_dir,
        file_store,
        manifest_io=manifest_io
    )

    # Test manifest generation
    manifest_accessors = list(metastore.generate_manifests())
    assert len(manifest_accessors) == len(manifests_data)

    # Verify each manifest accessor
    for accessor, manifest_data in zip(manifest_accessors, manifests_data):
        assert accessor.context.schema == sample_schema
        assert accessor.context.level == manifest_data["level"]
        assert accessor.manifest.sst_files == manifest_data["sst_files"]

