import os
import tempfile
import shutil
import pytest
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor
from typing import List

from deltacat import Datatype
# Import our RivuletWriter implementation
from deltacat.dev.data_access_layer.storage.rivulet_writer import RivuletWriter, RivuletWriteOptions, WriteMode
from deltacat.storage.rivulet import Schema
from deltacat.storage.model.partition import PartitionLocator
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
            {"name": "value", "datatype": Datatype.int32()}
        ],
    }
    
    schema = Schema.from_dict(schema_dict)
    dataset = RivuletDataset(dataset_name="test", metadata_uri=dataset_path, schema=schema)

    # Create the environment dictionary to return
    env = {
        "dataset_path": dataset_path,
        "dataset": dataset,
        "schema": schema
    }

    yield env

    # Remove the temporary directory
    shutil.rmtree(temp_dir)


def generate_test_data(schema: Schema, num_batches: int = 2, rows_per_batch: int = 5, ) -> List[pa.RecordBatch]:
    """Generate test data batches"""
    batches = []

    for batch_idx in range(num_batches):
        start_id = batch_idx * rows_per_batch + 1

        # Create arrays for each column
        ids = pa.array([i for i in range(start_id, start_id + rows_per_batch)])
        names = pa.array([f"name-{i}" for i in range(start_id, start_id + rows_per_batch)])
        categories = pa.array(["A" if i % 2 == 0 else "B" for i in range(start_id, start_id + rows_per_batch)])
        values = pa.array([i * 10 for i in range(start_id, start_id + rows_per_batch)])

        # Create a record batch
        batch = pa.RecordBatch.from_arrays(
            [ids, names, categories, values],
            schema=schema.to_pyarrow()
        )

        batches.append(batch)

    return batches


def test_write_batches(rivulet_environment):
    """Test writing batches to a Rivulet dataset"""
    # Get environment variables
    dataset = rivulet_environment["dataset"]

    # Create the writer
    writer = RivuletWriter(
        dataset
    )

    # Generate test data
    test_batches = generate_test_data(rivulet_environment["schema"], num_batches=2, rows_per_batch=100)

    # Write the batches
    write_options = RivuletWriteOptions(write_mode=WriteMode.UPSERT, file_format="parquet")
    write_results = list(writer.write_batches(test_batches, write_options))

    # Finalize global commit
    commit_result = writer.commit(write_results)

    # Read dataset back out
    results = list(dataset.scan().to_pydict())
    assert len(results)==200


def test_multi_threaded_writers(rivulet_environment):
    """Test multiple writers on different threads with transaction only committing at finalize"""
    # Get environment variables
    dataset_path = rivulet_environment["dataset_path"]
    schema_dict = rivulet_environment["schema_dict"]

    # List to store results from each writer thread
    local_results = []

    # Function for thread to run a writer
    def worker_thread(worker_id):
        # Create partition locator for this worker
        partition_locator = PartitionLocator(
            namespace="default",
            table_name="test_table",
            partition_values={"worker": f"worker_{worker_id}"}
        )
        
        # Create writer
        writer = RivuletWriter(
            base_path=dataset_path,
            schema=schema_dict,
            merge_key="id",
            partition_locator=partition_locator
        )

        # Generate data with a specific category for this worker
        category = f"worker_{worker_id}"
        test_batches = generate_test_data(
            num_batches=2,
            rows_per_batch=3,
            category=category
        )

        # Write batches
        write_options = RivuletWriteOptions(write_mode=WriteMode.UPSERT)
        write_results = list(writer.write_batches(test_batches, write_options))

        # Finalize local (no commit yet)
        local_metadata = writer.finalize_local(write_results)

        # Store results
        local_results.append(local_metadata)

        # Return the writer for potential global finalization
        return writer

    # Run multiple writer threads
    num_workers = 3
    writers = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all worker tasks
        future_writers = [executor.submit(worker_thread, i) for i in range(num_workers)]

        # Wait for all to complete and collect writers
        for future in future_writers:
            writers.append(future.result())

    # Check that we have metadata from all workers
    assert len(local_results) == num_workers
    
    # Each local result should have SST files in it
    total_sst_files = 0
    for result in local_results:
        assert "sst_files" in result
        assert len(result["sst_files"]) > 0
        total_sst_files += len(result["sst_files"])

    # Use one writer to perform the global finalization with all collected metadata
    global_writer = writers[0]
    commit_result = global_writer.commit(local_results)

    # Verify commit results
    assert commit_result["status"] == "committed"
    assert commit_result["num_files_committed"] == total_sst_files
    assert "manifest_location" in commit_result
    
    # The manifest file should exist
    assert os.path.exists(commit_result["manifest_location"])