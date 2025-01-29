import pytest
import os

from deltacat.storage.rivulet.metastore.delta import DeltacatManifestIO
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat import Datatype, Dataset
from deltacat.storage.rivulet import Schema


@pytest.fixture
def sample_schema():
    return Schema(
        {("id", Datatype.int32()), ("name", Datatype.string())},
        "id",
    )


@pytest.fixture
def sample_pydict():
    return {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}


def test_dataset_metastore_e2e(sample_schema, tmp_path):
    # Setup
    dataset = Dataset(metadata_uri=tmp_path, dataset_name="dataset")
    file_provider = dataset._file_provider
    manifest_io = DeltacatManifestIO(file_provider.uri, dataset._locator)

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
            with open(os.path.join(file_provider.uri, sst), "w") as f:
                f.write("test data")

        manifest_path = manifest_io.write(
            sst_files, sample_schema, manifest_data["level"]
        )
        manifest_paths.append(manifest_path)

    # Initialize DatasetMetastore
    metastore = DatasetMetastore(
        file_provider.uri,
        file_provider,
        file_provider._locator,
        manifest_io=manifest_io,
    )

    # Test manifest generation
    manifest_accessors = list(metastore.generate_manifests())
    assert len(manifest_accessors) == len(manifests_data)

    # Verify each manifest accessor
    for accessor in manifest_accessors:
        # assert accessor.context.schema == sample_schema
        manifests_data_index = 0 if accessor.context.level == 1 else 1
        assert accessor.context.level == manifests_data[manifests_data_index]["level"]
        assert (
            accessor.manifest.sst_files
            == manifests_data[manifests_data_index]["sst_files"]
        )
