import pytest

import pyarrow as pa
import pyarrow.parquet as pq
from deltacat import Datatype, Dataset
from deltacat.storage.rivulet import Schema, Field
from deltacat.utils.metafile_locator import _find_partition_path


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
def temp_storage_path(tmp_path):
    return tmp_path


@pytest.fixture
def sample_parquet_data(temp_storage_path, sample_pydict):
    parquet_path = temp_storage_path / "test.parquet"
    table = pa.Table.from_pydict(sample_pydict)
    pq.write_table(table, parquet_path)
    return parquet_path


@pytest.fixture
def dataset(sample_parquet_data):
    return Dataset.from_parquet(
        file_uri=sample_parquet_data, name="dataset", merge_keys="id"
    )


@pytest.fixture
def file_provider(dataset):
    return dataset._file_provider


def test_provide_data_file(file_provider):
    output_file = file_provider.provide_data_file("parquet")
    assert "data" in output_file.location
    assert output_file.location.endswith(".parquet")

    output_file2 = file_provider.provide_data_file("parquet")
    assert "data" in output_file2.location
    assert output_file2.location.endswith(".parquet")

    assert (
        output_file.location != output_file2.location
    ), "Two output files should have different locations."


def test_provide_manifest_file(file_provider):
    output_file = file_provider.provide_manifest_file()
    assert "metadata/manifests" in output_file.location
    assert output_file.location.endswith(".json")


def test_provide_l0_sst_file(file_provider):
    output_file = file_provider.provide_l0_sst_file()
    assert "metadata/ssts/0" in output_file.location
    assert output_file.location.endswith(".json")


def test_provide_input_file(file_provider, sample_parquet_data):
    input_file = file_provider.provide_input_file(str(sample_parquet_data))
    assert input_file.location == str(sample_parquet_data)


def test_generate_sst_uris(file_provider):
    generated_files = list(file_provider.generate_sst_uris())
    for file in generated_files:
        assert "metadata/ssts/0" in file.location
        assert file.location.endswith(".json")


def test_get_scan_directories(file_provider):
    partition_path = _find_partition_path(file_provider.uri, file_provider._locator)
    assert file_provider.get_sst_scan_directories() == [
        f"{partition_path}/metadata/ssts/0/"
    ]
