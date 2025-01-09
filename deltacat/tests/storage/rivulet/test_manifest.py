import os
from typing import Set

from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.manifest import (
    JsonManifestIO,
    ManifestContext,
    Manifest,
)
from deltacat.storage.rivulet import Schema


def test_write_manifest_round_trip():
    file_store = FileStore()
    manifest_io = JsonManifestIO()
    data_files = {"file1.parquet", "file2.parquet"}
    sst_files = {"sst1.sst", "sst2.sst"}
    schema = Schema(
        {("id", Datatype.int32()), ("name", Datatype.string())},
        "id",
    )
    level = 2

    with (make_tmpdir() as temp_dir):
        uri = os.path.join(temp_dir, "manifest.json")
        file = file_store.new_output_file(uri)
        manifest_io.write(file, data_files, sst_files, schema, level)
        manifest = manifest_io.read(file.to_input_file())
        assert manifest.context.schema == schema
        assert manifest.context.level == level
        assert manifest.data_files == data_files
        assert manifest.sst_files == sst_files


def test_manifest_hash():
    schema1 = Schema([("id", Datatype.int64()), ("name", Datatype.string())])
    schema2 = Schema([("age", Datatype.int16()), ("zip", Datatype.int32())])

    manifest_context1 = ManifestContext(schema=schema1, stream_position="pos1", level=1)
    manifest_context2 = ManifestContext(schema=schema2, stream_position="pos2", level=2)

    manifest1 = Manifest(
        data_files={"file1", "file2"},
        sst_files={"sst1", "sst2"},
        context=manifest_context1,
    )

    manifest2 = Manifest(
        data_files={"file1", "file2"},
        sst_files={"sst1", "sst2"},
        context=manifest_context1,
    )

    manifest3 = Manifest(
        data_files={"file3", "file4"},
        sst_files={"sst3", "sst4"},
        context=manifest_context2,
    )

    # Test hashes for identical manifests
    assert hash(manifest1) == hash(
        manifest2
    ), "Hashes for identical manifests should match."

    # Test hashes for different manifests
    assert hash(manifest1) != hash(
        manifest3
    ), "Hashes for different manifests should not match."

    # Test using manifests in a set
    manifest_set: Set[Manifest] = {manifest1, manifest2, manifest3}
    assert len(manifest_set) == 2, "Set should deduplicate identical manifests."
