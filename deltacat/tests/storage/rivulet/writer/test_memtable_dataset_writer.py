import pytest

from deltacat.storage.rivulet.fs.file_location_provider import FileLocationProvider
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.delta import DeltacatManifestIO
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.writer.memtable_dataset_writer import MemtableDatasetWriter


@pytest.fixture
def test_schema():
    return Schema(
        fields=[
            ("id", Datatype.int32()),
            ("name", Datatype.string()),
        ],
        merge_keys="id",
    )


@pytest.fixture
def location_provider(tmp_path):
    return FileLocationProvider(str(tmp_path))


@pytest.fixture
def file_store():
    return FileStore()


@pytest.fixture
def writer(location_provider, test_schema):
    return MemtableDatasetWriter(
        location_provider=location_provider,
        schema=test_schema
    )

def test_write_after_flush(writer, file_store):
    writer.write_dict({"id": 100, "name": "alpha"})
    manifest_uri_1 = writer.flush()

    manifest_io = DeltacatManifestIO(writer.location_provider.uri)
    manifest_1 = manifest_io.read(file_store.new_input_file(manifest_uri_1).location)
    sst_files_1 = manifest_1.sst_files

    assert len(sst_files_1) > 0, "First flush: no SST files found."
    assert manifest_1.context.schema == writer.schema, "Schema mismatch in first flush."

    writer.write_dict({"id": 200, "name": "gamma"})
    manifest_uri_2 = writer.flush()

    manifest_2 = manifest_io.read(file_store.new_input_file(manifest_uri_2).location)
    sst_files_2 = manifest_2.sst_files

    assert len(sst_files_2) > 0, "Second flush: no SST files found."

    # ensures data_files and sst_files from first write are not included in second write.
    assert set(sst_files_1).isdisjoint(set(sst_files_2)), \
        "Expected no overlap of SST files between first and second flush."
    assert manifest_2.context.schema == writer.schema, "Schema mismatch in second flush."
