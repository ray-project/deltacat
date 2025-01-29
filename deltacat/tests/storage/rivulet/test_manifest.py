import os

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.metastore.delta import (
    DeltacatManifestIO,
)
from deltacat.storage.rivulet import Schema

def test_write_manifest_round_trip(temp_dir):
    temp_dir = temp_dir
    path, filesystem = FileStore.filesystem(temp_dir)
    file_store = FileStore(path, filesystem=filesystem)
    manifest_io = DeltacatManifestIO(temp_dir)

    sst_files = ["sst1.sst", "sst2.sst"]
    schema = Schema(
        {("id", Datatype.int32()), ("name", Datatype.string())},
        "id",
    )
    level = 2

    uri = os.path.join(temp_dir, "manifest.json")
    file_store.create_output_file(uri)
    written = manifest_io.write(sst_files, schema, level)
    manifest = manifest_io.read(written)
    assert manifest.context.schema == schema
    assert manifest.context.level == level
    assert manifest.sst_files == sst_files
