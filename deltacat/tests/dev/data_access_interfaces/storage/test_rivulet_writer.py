import os
import tempfile
import shutil
import pytest
import pyarrow as pa
from typing import List

from deltacat import Datatype

# Import our RivuletWriter implementation
from deltacat.dev.data_access_layer.storage.rivulet_writer import (
    RivuletWriter,
    RivuletWriteOptions,
    WriteMode,
)
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.dataset import Dataset as RivuletDataset


@pytest.fixture(scope="function")
def rivulet_environment():
    """Create a temporary environment for Rivulet dataset"""
    # Create a temporary directory for the dataset
    temp_dir = tempfile.mkdtemp()
    dataset_path = os.path.join(temp_dir, "rivulet_dataset")
    os.makedirs(dataset_path, exist_ok=True)

    # Define schema for the test table
    schema_dict = {
        "fields": [
            {"name": "id", "datatype": Datatype.int64(), "is_merge_key": True},
            {"name": "name", "datatype": Datatype.string()},
            {"name": "category", "datatype": Datatype.string()},
            {"name": "value", "datatype": Datatype.int32()},
        ],
    }

    schema = Schema.from_dict(schema_dict)
    dataset = RivuletDataset(
        dataset_name="test", metadata_uri=dataset_path, schema=schema
    )

    # Create the environment dictionary to return
    env = {"dataset_path": dataset_path, "dataset": dataset, "schema": schema}

    yield env

    # Remove the temporary directory
    shutil.rmtree(temp_dir)


def generate_test_data(
    schema: Schema,
    num_batches: int = 2,
    rows_per_batch: int = 5,
) -> List[pa.RecordBatch]:
    """Generate test data batches"""
    batches = []

    for batch_idx in range(num_batches):
        start_id = batch_idx * rows_per_batch + 1

        # Create arrays for each column
        ids = pa.array([i for i in range(start_id, start_id + rows_per_batch)])
        names = pa.array(
            [f"name-{i}" for i in range(start_id, start_id + rows_per_batch)]
        )
        categories = pa.array(
            [
                "A" if i % 2 == 0 else "B"
                for i in range(start_id, start_id + rows_per_batch)
            ]
        )
        values = pa.array([i * 10 for i in range(start_id, start_id + rows_per_batch)])

        # Create a record batch
        batch = pa.RecordBatch.from_arrays(
            [ids, names, categories, values], schema=schema.to_pyarrow()
        )

        batches.append(batch)

    return batches


def test_write_batches(rivulet_environment):
    """Test writing batches to a Rivulet dataset"""
    # Get environment variables
    dataset = rivulet_environment["dataset"]

    # Create the writer
    writer = RivuletWriter(dataset)

    # Generate test data
    test_batches = generate_test_data(
        rivulet_environment["schema"], num_batches=2, rows_per_batch=100
    )

    # Write the batches
    write_options = RivuletWriteOptions(
        write_mode=WriteMode.UPSERT, file_format="parquet"
    )
    write_results = list(writer.write_batches(test_batches, write_options))

    # Finalize global commit
    writer.commit(write_results)

    # Read dataset back out
    results = list(dataset.scan().to_pydict())
    assert len(results) == 200
