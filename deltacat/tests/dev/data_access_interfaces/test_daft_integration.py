"""
Unit tests for Daft-DeltaCAT integration.

These tests verify the correct functionality of the write_deltacat method
added to Daft DataFrames for writing to DeltaCAT storage formats.
"""

import os
import tempfile
import shutil
import pytest
import daft
from deltacat import Datatype

from deltacat.dev.data_access_layer.daft_integration import patch_daft
from deltacat.dev.data_access_layer.storage.writer import Writer, WriteOptions, WriteMode
from deltacat.dev.data_access_layer.storage.rivulet_writer import RivuletWriter, RivuletWriteOptions
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.dataset import Dataset as RivuletDataset

@pytest.fixture
def setup_daft_integration():
    """Setup the Daft-DeltaCAT integration by applying the monkey patch"""
    patch_daft()

@pytest.fixture
def rivulet_environment(scope="function"):
    """Create a temporary environment for Rivulet dataset"""
    # Create a temporary directory for the dataset
    temp_dir = tempfile.mkdtemp()
    dataset_path = os.path.join(temp_dir, "rivulet_dataset")
    os.makedirs(dataset_path, exist_ok=True)

    # Define schema for the test
    schema_dict = {
        "fields": [
            {"name": "id", "datatype": Datatype.int64(), "is_merge_key": True},
            {"name": "name", "datatype": Datatype.string()},
            {"name": "value", "datatype": Datatype.int32()},
        ],
    }
    
    schema = Schema.from_dict(schema_dict)
    dataset = RivuletDataset(
        dataset_name="test", metadata_uri=dataset_path, schema=schema
    )
    # Create the environment dictionary to return
    env = {
        "temp_dir": temp_dir,
        "dataset": dataset,
        "schema": schema,
        "schema_dict": schema_dict
    }

    yield env

    # Remove the temporary directory
    shutil.rmtree(temp_dir)


def test_rivulet_writer_local(setup_daft_integration, rivulet_environment):
    """Test with a real RivuletWriter implementation in local mode"""
    # Create test data
    df = daft.from_pydict({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30]
    })
    
    dataset = rivulet_environment["dataset"]
    schema_dict = rivulet_environment["schema_dict"]
    
    # Create a RivuletWriter
    writer = RivuletWriter(
        dataset=dataset,
    )
    
    # Create write options
    write_options = RivuletWriteOptions(
        write_mode=WriteMode.UPSERT,
        file_format="parquet"
    )
    
    # Write to Rivulet
    result = df.write_deltacat(writer, write_options)

    # Read data
    read_back = list(dataset.scan().to_pydict())
    assert len(read_back) == 3

    # Verify result
    assert "status" in result.to_pydict(), "Result should contain status"
    assert result.to_pydict()["status"][0] == "committed", "Status should be 'committed'"
    
    # Verify manifest file was created
    assert "manifest_location" in result.to_pydict(), "Result should contain manifest_location"
    manifest_file = result.to_pydict()["manifest_location"][0]
    assert os.path.exists(manifest_file), "Manifest file should exist"
    
    # Verify the correct number of files was committed
    assert "num_files_committed" in result.to_pydict(), "Result should contain num_files_committed"


@pytest.mark.ray
def test_rivulet_writer_ray(setup_daft_integration, rivulet_environment):
    """Test with a real RivuletWriter implementation using Ray (if available)"""
    try:
        import ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
    except ImportError:
        pytest.skip("Ray not installed, skipping Ray test")
    
    # Set Daft to use Ray
    daft.context.set_runner_ray()
    
    # Create test data
    df = daft.from_pydict({
        "id": list(range(1, 101)),  # 100 rows to ensure multiple partitions
        "name": [f"Name-{i}" for i in range(1, 101)],
        "value": [float(i * 1.5) for i in range(1, 101)]
    })
    
    # Force multiple partitions
    df = df.repartition(10)
    
    dataset_path = rivulet_environment["dataset_path"]
    schema_dict = rivulet_environment["schema_dict"]
    
    # Create a RivuletWriter
    writer = RivuletWriter(
        base_path=dataset_path,
        schema=schema_dict,
        merge_key="id"
    )
    
    # Create write options
    write_options = RivuletWriteOptions(
        write_mode="upsert",
        file_format="parquet"
    )
    
    # Write to Rivulet
    result = df.write_deltacat(writer, write_options)
    
    # Verify result
    assert "status" in result.to_pydict(), "Result should contain status"
    assert result.to_pydict()["status"][0] == "committed", "Status should be 'committed'"
    
    # Verify manifest file was created
    assert "manifest_location" in result.to_pydict(), "Result should contain manifest_location"
    manifest_file = result.to_pydict()["manifest_location"][0]
    assert os.path.exists(manifest_file), "Manifest file should exist"
    
    # Reset Daft to local runner for other tests
    daft.context.set_runner_py()