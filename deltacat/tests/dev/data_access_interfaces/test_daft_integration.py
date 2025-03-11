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

from deltacat.dev.data_access_layer.daft_integration import patch_daft
from deltacat.dev.data_access_layer.storage.writer import Writer, WriteOptions, WriteMode
from deltacat.dev.data_access_layer.storage.rivulet_writer import RivuletWriter, RivuletWriteOptions
from deltacat.storage.rivulet import Schema


class MockWriter(Writer):
    """Mock Writer implementation for testing"""
    
    def __init__(self, expected_batches=None):
        self.written_batches = []
        self.expected_batches = expected_batches
        self.finalize_local_called = False
        self.commit_called = False
        self.write_results = []
    
    def write_batches(self, record_batches, write_options):
        """Record batches being written"""
        self.written_batches.extend(record_batches)
        
        # Verify with expected batches if provided
        if self.expected_batches is not None:
            assert len(record_batches) == len(self.expected_batches), "Wrong number of batches"
            for i, batch in enumerate(record_batches):
                expected = self.expected_batches[i]
                assert batch.schema == expected.schema, "Schema mismatch"
                assert batch.num_rows == expected.num_rows, "Row count mismatch"
        
        # Return a result for each batch
        for i, batch in enumerate(record_batches):
            result = {"batch_index": i, "num_rows": batch.num_rows}
            self.write_results.append(result)
            yield result
    
    def finalize_local(self, write_metadata):
        self.finalize_local_called = True
        return {"num_batches": len(write_metadata), "local_metadata": "test"}
    
    def commit(self, write_metadata, *args, **kwargs):
        self.commit_called = True
        return {
            "status": "committed",
            "num_files_committed": len(self.written_batches),
            "total_rows": sum(batch.num_rows for batch in self.written_batches)
        }


class MockWriteOptions(WriteOptions):
    """Mock WriteOptions implementation for testing"""
    
    def __init__(self, write_mode=WriteMode.APPEND, **kwargs):
        super().__init__(write_mode=write_mode, **kwargs)


@pytest.fixture
def setup_daft_integration():
    """Setup the Daft-DeltaCAT integration by applying the monkey patch"""
    patch_daft()
    yield
    # No cleanup needed


@pytest.fixture
def rivulet_environment():
    """Create a temporary environment for Rivulet dataset"""
    # Create a temporary directory for the dataset
    temp_dir = tempfile.mkdtemp()
    dataset_path = os.path.join(temp_dir, "rivulet_dataset")
    os.makedirs(dataset_path, exist_ok=True)

    # Define schema for the test
    schema_dict = {
        "fields": [
            {"name": "id", "type": "long", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
            {"name": "value", "type": "double", "nullable": True}
        ],
        "merge_key": "id"
    }
    
    schema = Schema.from_dict(schema_dict)

    # Create the environment dictionary to return
    env = {
        "temp_dir": temp_dir,
        "dataset_path": dataset_path,
        "schema": schema,
        "schema_dict": schema_dict
    }

    yield env

    # Remove the temporary directory
    shutil.rmtree(temp_dir)


def test_mock_writer(setup_daft_integration):
    """Test with a mock writer implementation"""
    # Create test data
    df = daft.from_pydict({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.1, 30.9]
    })
    
    # Create mock writer
    writer = MockWriter()
    write_options = MockWriteOptions(write_mode=WriteMode.APPEND)
    
    # Write to the mock writer
    result = df.write_deltacat(writer, write_options)
    
    # Verify writer was called correctly
    assert writer.finalize_local_called, "finalize_local was not called"
    assert writer.commit_called, "commit was not called"
    assert len(writer.written_batches) > 0, "No batches were written"
    
    # Check that we got the correct data
    total_rows = sum(batch.num_rows for batch in writer.written_batches)
    assert total_rows == 3, "Wrong number of rows written"
    
    # Verify return value
    assert "status" in result.to_pydict(), "Result should contain status"
    assert result.to_pydict()["status"][0] == "committed", "Status should be 'committed'"
    assert result.to_pydict()["total_rows"][0] == 3, "Should have written 3 rows"


def test_rivulet_writer_local(setup_daft_integration, rivulet_environment):
    """Test with a real RivuletWriter implementation in local mode"""
    # Create test data
    df = daft.from_pydict({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.1, 30.9]
    })
    
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