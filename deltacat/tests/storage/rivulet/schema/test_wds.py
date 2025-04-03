import itertools
import pytest
import pyarrow as pa
import json
import tarfile
from deltacat.storage.rivulet import Dataset, Schema, Field, Datatype


def test_schema_field_types():
    fields = [
        Field("id", Datatype.int64(), is_merge_key=True),
        Field("name", Datatype.string()),
    ]
    schema = Schema(fields)
    values = list(schema.values())
    assert len(values) == 2
    assert all(isinstance(v, Field) for v in values)

def test_schema_fields():
    # cwd = pathlib.Path.cwd()
    # csv_file_path = cwd / "data.csv"
    # ds = Dataset.from_csv(
        # name="chat",
        # file_uri=tar_path,
        # metadata_uri=cwd.as_uri(),
        # merge_keys="msg_id"
    # )

    # tar_path = "../../../test_utils/resources/imagenet1k-train-0000.tar"
    tar_path = "../../../test_utils/resources/test_wds.tar"
    dataset = Dataset.from_webdataset(
        name="test",
        file_uri=tar_path,
        merge_keys="filename"
    )
    assert "label" in dataset.fields
    assert "width" in dataset.fields
    assert "height" in dataset.fields
    assert "filename" in dataset.fields
    assert len(dataset.fields) == 4
    # assert False

# TODO: make dummy tar file instead of local file
# test that the data is properly stored in new dataset
def test_schema_data():
    # tar_path = "../../../test_utils/resources/imagenet1k-train-0000.tar"
    tar_path = "../../../test_utils/resources/test_wds.tar"

    dataset = Dataset.from_webdataset(
        name="test",
        file_uri=tar_path,
        merge_keys="filename"
    )
    # dataset.print()
    # print(len(dataset))
    records = dataset.scan().to_pydict()
    for record in itertools.islice(records, 1):
        assert record["label"] == 1
        assert record["width"] == 500
        assert record["height"] == 429
        assert record["filename"] == "n01443537/n01443537_14753.JPEG"

