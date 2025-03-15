import pytest
import pyarrow as pa
import json
import tarfile
from deltacat.storage.rivulet import Dataset, Schema, Field, Datatype


def test_schema_values():
    fields = [
        Field("id", Datatype.int64(), is_merge_key=True),
        Field("name", Datatype.string()),
    ]
    schema = Schema(fields)
    values = list(schema.values())
    assert len(values) == 2
    assert all(isinstance(v, Field) for v in values)

def test_t():
    # cwd = pathlib.Path.cwd()
    # csv_file_path = cwd / "data.csv"
    tar_path = "./imagenet1k-train-0000.tar"

    # ds = Dataset.from_csv(
    #     name="chat",
    #     file_uri=tar_path,
    #     metadata_uri=cwd.as_uri(),
    #     merge_keys="msg_id"
    # )

    s = Schema.from_webdataset_schema(tar_path)
    for f in s:
        print(f)
    assert False

