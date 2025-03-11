import os
import tempfile
import uuid
import shutil
import pytest
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Generator, Optional

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, LongType
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import PyArrowFileIO

# Import our IcebergWriter implementation
from deltacat.catalog.v2.iceberg.iceberg_writer import IcebergWriter, WriteOptions, IcebergWriteResultMetadata, \
    IcebergWriteOptions

HIVE_METASTORE_FAKE_URL = "thrift://unknown:9083"

@pytest.fixture(scope="function")
def iceberg_environment():
    """Create a temporary environment with Iceberg catalog and table"""
    # Create a temporary directory for the warehouse
    temp_dir = tempfile.mkdtemp()
    warehouse_path = os.path.join(temp_dir, "warehouse")
    os.makedirs(warehouse_path, exist_ok=True)

    # Create a local HiveCatalog
    catalog = InMemoryCatalog(
        "test_catalog",
        warehouse_path
    )

    # Define test database and table names
    test_namespace = "test_db"
    test_table = "test_table"
    table_identifier = f"{test_namespace}.{test_table}"

    # Create namespace if it doesn't exist
    catalog.create_namespace(test_namespace)

    # Define schema for the test table - using LongType for id to match PyArrow's defaults
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),  # Changed to LongType to match PyArrow int64 default
        NestedField(2, "name", StringType()),
        NestedField(3, "category", StringType()),
        NestedField(4, "value", LongType())
    )

    # Define partition spec (partitioning by category)
    partition_field = PartitionField(
        source_id=3,  # ID for 'category' field
        field_id=1000,  # Field ID for the partition field
        transform=IdentityTransform(),
        name="category"
    )
    partition_spec = PartitionSpec(partition_field)

    # Create table properties
    properties = {
        "write.format.default": "parquet",
        "format-version": "2"
    }

    # Create the table if it doesn't exist
    if not catalog.table_exists(table_identifier):
        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec,
            properties=properties
        )

    # Get the table location
    table = catalog.load_table(table_identifier)
    table_location = table.metadata.location

    # Create the environment dictionary to return
    env = {
        "temp_dir": temp_dir,
        "warehouse_path": warehouse_path,
        "catalog": catalog,
        "namespace": test_namespace,
        "table_name": test_table,
        "table_identifier": table_identifier,
        "table_location": table_location,
        "schema": schema,
        "partition_spec": partition_spec
    }

    yield env

    # Remove the temporary directory
    shutil.rmtree(temp_dir)


def generate_test_data(num_batches: int = 2, rows_per_batch: int = 5,
                       category: Optional[str] = None) -> List[pa.RecordBatch]:
    """Generate test data batches"""
    batches = []

    for batch_idx in range(num_batches):
        start_id = batch_idx * rows_per_batch + 1

        # Create arrays for each column
        ids = pa.array([i for i in range(start_id, start_id + rows_per_batch)])
        names = pa.array([f"name-{i}" for i in range(start_id, start_id + rows_per_batch)])

        # Use the provided category or alternate categories
        if category:
            categories = pa.array([category] * rows_per_batch)
        else:
            categories = pa.array(["A" if i % 2 == 0 else "B" for i in range(start_id, start_id + rows_per_batch)])

        values = pa.array([i * 10 for i in range(start_id, start_id + rows_per_batch)])

        # Create a record batch
        batch = pa.RecordBatch.from_arrays(
            [ids, names, categories, values],
            names=["id", "name", "category", "value"]
        )

        batches.append(batch)

    return batches


def test_write_batches(iceberg_environment):
    """Test writing batches to an Iceberg table"""
    # Get environment variables
    catalog = iceberg_environment["catalog"]
    table_identifier = iceberg_environment["table_identifier"]
    table_location = iceberg_environment["table_location"]

    # Create the writer
    writer = IcebergWriter(
        catalog=catalog,
        table_identifier=table_identifier,
        partition_by=["category"]
    )

    # Generate test data
    test_batches = generate_test_data(num_batches=2, rows_per_batch=5)

    # Write the batches
    write_results = list(writer.write_batches(test_batches, IcebergWriteOptions()))


    # Finalize global commit
    commit_result = writer.commit(write_results)

    # Reload the table and verify the data
    updated_table = catalog.load_table(table_identifier)

    # Scan the table and convert to pandas to verify the contents
    scanner = updated_table.scan()
    arrow_table = scanner.to_arrow()

    # Verify the number of rows
    assert arrow_table.num_rows == 10, "Table should have 10 rows"

    # Verify columns and values
    column_names = arrow_table.column_names
    assert "id" in column_names
    assert "name" in column_names
    assert "category" in column_names
    assert "value" in column_names

def test_multi_threaded_writers(iceberg_environment):
    """Test multiple writers on different threads with transaction only committing at finalize_global"""
    # Get environment variables
    catalog = iceberg_environment["catalog"]
    table_identifier = iceberg_environment["table_identifier"]
    table_location = iceberg_environment["table_location"]

    # Load the initial table state
    initial_table = catalog.load_table(table_identifier)
    initial_snapshot_id = initial_table.metadata.current_snapshot_id

    # Shared transaction ID for all writers
    shared_tx_id = str(uuid.uuid4())

    # List to store results from each writer thread
    local_results = []

    # Function for thread to run a writer
    def worker_thread(worker_id):
        # Create write options with shared transaction ID
        write_options = WriteOptions(
            table_location=table_location,
            format="parquet",
            mode="append",
            transaction_id=shared_tx_id
        )

        # Create writer
        writer = IcebergWriter(
            catalog=catalog,
            table_identifier=table_identifier,
            write_options=write_options,
            partition_by=["category"]
        )

        # Generate data with a specific category for this worker
        category = f"worker_{worker_id}"
        test_batches = generate_test_data(
            num_batches=2,
            rows_per_batch=3,
            category=category
        )

        # Write batches
        write_results = list(writer.write_batches(test_batches))

        # Finalize local (no commit yet)
        local_metadata = writer.finalize_local(iter(write_results))

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

    # Check that the snapshot hasn't changed after all local finalizations
    table_after_locals = catalog.load_table(table_identifier)
    assert table_after_locals.metadata.current_snapshot_id == initial_snapshot_id, \
        "Snapshot should not change after parallel finalize_local calls"

    # No data should be readable yet
    scanner = table_after_locals.scan()
    arrow_table = scanner.to_arrow()
    assert arrow_table.num_rows == 0, "No data should be available before finalize_global"

    # Use one writer to perform the global finalization with all collected metadata
    global_writer = writers[0]
    commit_result = global_writer.finalize_global(iter(local_results))

    # Verify commit results
    assert commit_result["num_files_committed"] == num_workers * 2, \
        f"Should have committed {num_workers * 2} files (2 per worker)"
    assert commit_result["total_rows"] == num_workers * 2 * 3, \
        f"Should have committed {num_workers * 2 * 3} rows total"

    # Reload table and check for data
    final_table = catalog.load_table(table_identifier)
    assert final_table.metadata.current_snapshot_id != initial_snapshot_id, \
        "Snapshot should change after finalize_global"

    # Verify data is now available
    scanner = final_table.scan()
    arrow_table = scanner.to_arrow()
    assert arrow_table.num_rows == num_workers * 2 * 3, \
        f"Table should have {num_workers * 2 * 3} rows after commit"

    # Verify each worker's partition is present
    pandas_df = arrow_table.to_pandas()
    categories = pandas_df["category"].unique()

    for i in range(num_workers):
        expected_category = f"worker_{i}"
        assert expected_category in categories, f"Category {expected_category} should be present"

def test_file_paths_exist_before_commit(iceberg_environment):
    """Test that data files physically exist after write_batches but aren't visible in table scan"""
    # Get environment variables
    catalog = iceberg_environment["catalog"]
    table_identifier = iceberg_environment["table_identifier"]
    table_location = iceberg_environment["table_location"]

    # Create write options
    write_options = WriteOptions(
        table_location=table_location,
        format="parquet",
        mode="append"
    )

    # Create the writer
    writer = IcebergWriter(
        catalog=catalog,
        table_identifier=table_identifier,
        write_options=write_options,
        partition_by=["category"]
    )

    # Generate and write test data
    test_batches = generate_test_data(num_batches=1, rows_per_batch=3)
    write_results = list(writer.write_batches(test_batches))

    # Collect file paths from write results
    file_paths = []
    for batch_result in write_results:
        for file_info in batch_result:
            file_paths.append(file_info["file_path"])

    # Verify files physically exist
    for file_path in file_paths:
        assert os.path.exists(file_path), f"File {file_path} should exist after write_batches"

    # Verify files are not visible in table scan
    table_after_write = catalog.load_table(table_identifier)
    scanner = table_after_write.scan()
    arrow_table = scanner.to_arrow()
    assert arrow_table.num_rows == 0, "No data should be visible in table scan before commit"

    # Complete the write process
    local_metadata = writer.finalize_local(iter(write_results))
    writer.finalize_global(iter([local_metadata]))

    # Now verify files are visible in table scan
    table_after_commit = catalog.load_table(table_identifier)
    scanner = table_after_commit.scan()
    arrow_table = scanner.to_arrow()
    assert arrow_table.num_rows == 3, "Data should be visible after commit"