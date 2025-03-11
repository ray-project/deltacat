import os
import tempfile
import shutil
import pytest
import pyarrow as pa
from typing import List, Optional

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

# Import our IcebergWriter implementation
from deltacat.dev.data_access_layer.storage.iceberg_writer import (
    IcebergWriter,
    IcebergWriteOptions,
)

HIVE_METASTORE_FAKE_URL = "thrift://unknown:9083"


@pytest.fixture(scope="function")
def iceberg_environment():
    """Create a temporary environment with Iceberg catalog and table"""
    # Create a temporary directory for the warehouse
    temp_dir = tempfile.mkdtemp()
    warehouse_path = os.path.join(temp_dir, "warehouse")
    os.makedirs(warehouse_path, exist_ok=True)

    # Create a local HiveCatalog
    catalog = InMemoryCatalog("test_catalog", warehouse_path)

    # Define test database and table names
    test_namespace = "test_db"
    test_table = "test_table"
    table_identifier = f"{test_namespace}.{test_table}"

    # Create namespace if it doesn't exist
    catalog.create_namespace(test_namespace)

    # Define schema for the test table - using LongType for id to match PyArrow's defaults
    schema = Schema(
        NestedField(
            1, "id", LongType(), required=True
        ),  # Changed to LongType to match PyArrow int64 default
        NestedField(2, "name", StringType()),
        NestedField(3, "category", StringType()),
        NestedField(4, "value", LongType()),
    )

    # Define partition spec (partitioning by category)
    partition_field = PartitionField(
        source_id=3,  # ID for 'category' field
        field_id=1000,  # Field ID for the partition field
        transform=IdentityTransform(),
        name="category",
    )
    partition_spec = PartitionSpec(partition_field)

    # Create table properties
    properties = {"write.format.default": "parquet", "format-version": "2"}

    # Create the table if it doesn't exist
    if not catalog.table_exists(table_identifier):
        catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec,
            properties=properties,
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
        "partition_spec": partition_spec,
    }

    yield env

    # Remove the temporary directory
    shutil.rmtree(temp_dir)


def generate_test_data(
    num_batches: int = 2, rows_per_batch: int = 5, category: Optional[str] = None
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

        # Use the provided category or alternate categories
        if category:
            categories = pa.array([category] * rows_per_batch)
        else:
            categories = pa.array(
                [
                    "A" if i % 2 == 0 else "B"
                    for i in range(start_id, start_id + rows_per_batch)
                ]
            )

        values = pa.array([i * 10 for i in range(start_id, start_id + rows_per_batch)])

        # Create a record batch
        batch = pa.RecordBatch.from_arrays(
            [ids, names, categories, values], names=["id", "name", "category", "value"]
        )

        batches.append(batch)

    return batches


def test_write_batches(iceberg_environment):
    """Test writing batches to an Iceberg table"""
    # Get environment variables
    catalog = iceberg_environment["catalog"]
    table_identifier = iceberg_environment["table_identifier"]

    # Create the writer
    writer = IcebergWriter(
        catalog=catalog, table_identifier=table_identifier, partition_by=["category"]
    )

    # Generate test data
    test_batches = generate_test_data(num_batches=2, rows_per_batch=5)

    # Write the batches
    write_results = list(writer.write_batches(test_batches, IcebergWriteOptions()))

    # Finalize global commit
    writer.commit(write_results)

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
