import os

import pytest

from deltacat import Dataset
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.metastore.delta import DeltacatManifestIO
from deltacat.storage.rivulet import Schema, Field
import pyarrow as pa
import pyarrow.parquet


@pytest.fixture
def sample_schema():
    return Schema(
        fields=[
            Field("id", Datatype.int32(), is_merge_key=True),
            Field("name", Datatype.string()),
            Field("age", Datatype.int32()),
        ]
    )


@pytest.fixture
def sample_pydict():
    return {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}


@pytest.fixture
def path(tmp_path):
    return tmp_path


@pytest.fixture
def sample_parquet_data(path, sample_pydict):
    parquet_path = path / "test.parquet"
    table = pa.Table.from_pydict(sample_pydict)
    pyarrow.parquet.write_table(table, parquet_path)
    return parquet_path


def test_write_manifest_round_trip(sample_parquet_data, sample_schema):
    dataset = Dataset.from_parquet(
        file_uri=sample_parquet_data, name="dataset", merge_keys="id"
    )

    path, filesystem = FileStore.filesystem(dataset._metadata_path)
    file_store = FileStore(path, filesystem=filesystem)
    manifest_io = DeltacatManifestIO(path, dataset._locator)

    sst_files = ["sst1.sst", "sst2.sst"]
    schema = Schema(
        {("id", Datatype.int32()), ("name", Datatype.string())},
        "id",
    )
    level = 2

    uri = os.path.join(path, "manifest.json")

    file_store.create_output_file(uri)
    written = manifest_io.write(sst_files, schema, level)
    manifest = manifest_io.read(written)

    assert manifest.context.schema == schema
    assert manifest.context.level == level
    assert manifest.sst_files == sst_files
