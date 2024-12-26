import os

from deltacat.tests.storage.rivulet.test_utils import make_tmpdir
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.manifest import JsonManifestIO
from deltacat.storage.rivulet import Schema, Field


def test_write_manifest_round_trip():
    file_store = FileStore()
    manifest_io = JsonManifestIO()
    data_files = {"file1.parquet", "file2.parquet"}
    sst_files = {"sst1.sst", "sst2.sst"}
    schema = Schema(
        {"id": Field("id", Datatype.int32()), "name": Field("name", Datatype.string())},
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
