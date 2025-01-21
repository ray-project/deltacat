import pytest
from deltacat.tests.storage.rivulet.test_utils import verify_pyarrow_scan
import pyarrow as pa
from deltacat.storage.rivulet import Schema, Field, Datatype
from deltacat.storage.rivulet.dataset import Dataset


@pytest.fixture
def combined_schema():
    return Schema(
        fields=[
            Field("id", Datatype.int64(), is_merge_key=True),
            Field("name", Datatype.string()),
            Field("age", Datatype.int32()),
            Field("height", Datatype.int64()),
            Field("gender", Datatype.string()),
        ]
    )


@pytest.fixture
def initial_schema():
    return Schema(
        fields=[
            Field("id", Datatype.int32(), is_merge_key=True),
            Field("name", Datatype.string()),
            Field("age", Datatype.int32()),
        ]
    )


@pytest.fixture
def extended_schema():
    return Schema(
        fields=[
            Field("id", Datatype.int64(), is_merge_key=True),
            Field("height", Datatype.int64()),
            Field("gender", Datatype.string()),
        ]
    )


@pytest.fixture
def sample_data():
    return {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    }


@pytest.fixture
def extended_data():
    return {
        "id": [1, 2, 3],
        "height": [150, 160, 159],
        "gender": ["male", "female", "male"],
    }


@pytest.fixture
def combined_data(sample_data, extended_data):
    data = sample_data.copy()
    data.update(extended_data)
    return data


@pytest.fixture
def parquet_data(tmp_path, sample_data):
    parquet_path = tmp_path / "test.parquet"
    table = pa.Table.from_pydict(sample_data)
    pa.parquet.write_table(table, parquet_path)
    return parquet_path


@pytest.fixture
def sample_dataset(parquet_data, tmp_path):
    return Dataset.from_parquet(
        name="test_dataset",
        file_uri=str(parquet_data),
        metadata_uri=tmp_path,
        merge_keys="id",
    )


def test_end_to_end_scan_with_multiple_schemas(
    sample_dataset,
    initial_schema,
    extended_schema,
    combined_schema,
    sample_data,
    extended_data,
    combined_data,
):
    # Verify initial scan.
    verify_pyarrow_scan(sample_dataset.scan().to_arrow(), initial_schema, sample_data)

    # Add a new schema to the dataset
    sample_dataset.add_schema(schema=extended_schema, schema_name="schema2")
    new_data = [
        {"id": 1, "height": 150, "gender": "male"},
        {"id": 2, "height": 160, "gender": "female"},
        {"id": 3, "height": 159, "gender": "male"},
    ]
    writer = sample_dataset.writer(schema_name="schema2")
    writer.write(new_data)
    writer.flush()

    # Verify scan with the extended schema retrieves only extended datfa
    verify_pyarrow_scan(
        sample_dataset.scan(schema_name="schema2").to_arrow(),
        extended_schema,
        extended_data,
    )

    # Verify a combined scan retrieves data matching the combined schema
    verify_pyarrow_scan(
        sample_dataset.scan().to_arrow(), combined_schema, combined_data
    )
