import pytest

from deltacat.storage.rivulet.fs.file_provider import FileProvider
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.manifest import JsonManifestIO
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.writer.memtable_dataset_writer import (
    MemtableDatasetWriter,
)


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
def resolve_path_and_filesystem(tmp_path):
    return FileStore.filesystem(tmp_path)


@pytest.fixture
def file_provider(resolve_path_and_filesystem):
    path, filesystem = resolve_path_and_filesystem
    file_store = FileStore(path, filesystem)
    return FileProvider(path, file_store)


@pytest.fixture
def file_store(resolve_path_and_filesystem):
    path, filesystem = resolve_path_and_filesystem
    return FileStore(path, filesystem=filesystem)


@pytest.fixture
def writer(file_provider, test_schema):
    return MemtableDatasetWriter(file_provider=file_provider, schema=test_schema)


def test_write_after_flush(writer, file_store):
    writer.write_dict({"id": 100, "name": "alpha"})
    manifest_uri_1 = writer.flush()

    manifest_io = JsonManifestIO()
    manifest_1 = manifest_io.read(file_store.create_input_file(manifest_uri_1))
    data_files_1 = manifest_1.data_files
    sst_files_1 = manifest_1.sst_files

    assert len(data_files_1) > 0, "First flush: no data files found."
    assert len(sst_files_1) > 0, "First flush: no SST files found."
    assert manifest_1.context.schema == writer.schema, "Schema mismatch in first flush."

    writer.write_dict({"id": 200, "name": "gamma"})
    manifest_uri_2 = writer.flush()

    manifest_2 = manifest_io.read(file_store.create_input_file(manifest_uri_2))
    data_files_2 = manifest_2.data_files
    sst_files_2 = manifest_2.sst_files

    assert len(data_files_2) > 0, "Second flush: no data files found."
    assert len(sst_files_2) > 0, "Second flush: no SST files found."

    # ensures data_files and sst_files from first write are not included in second write.
    assert data_files_1.isdisjoint(
        data_files_2
    ), "Expected no overlap of data files between first and second flush."
    assert sst_files_1.isdisjoint(
        sst_files_2
    ), "Expected no overlap of SST files between first and second flush."
    assert (
        manifest_2.context.schema == writer.schema
    ), "Schema mismatch in second flush."
