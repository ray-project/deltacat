import time
from typing import Callable, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import Row
import os
import pyarrow.fs as pafs
from deltacat.experimental.converter_agent.beam.managed import write as deltacat_beam_managed_write
from deltacat.examples.experimental.iceberg.converter.beam.utils.common import (
    generate_random_suffix,
    verify_duplicate_resolution,
)
from deltacat.examples.experimental.iceberg.converter.beam.utils.spark import (
    SparkSQLIcebergRead,
    SparkSQLIcebergRewrite,
)

# Monkey-patch beam.managed.Write and beam.managed.Read
beam.managed.Write = deltacat_beam_managed_write


def run(
    beam_options: Optional[PipelineOptions] = None,
    mode: str = "write",  # 'write' to write data, 'read' to read data
    rest_catalog_uri: str = "http://localhost:8181",  # REST catalog server URI
    warehouse_path: Optional[str] = None,  # Optional custom warehouse path
    table_name: Optional[str] = None,  # Table name with namespace
    deltacat_converter_interval: float = 5.0,  # Converter monitoring interval
    ray_inactivity_timeout: int = 20,  # Ray cluster shutdown timeout
    max_converter_parallelism: int = 1,  # Maximum converter task parallelism
    filesystem: Optional[pafs.FileSystem] = None,  # Optional PyArrow filesystem
) -> None:
    """
    Run the pipeline in either 'write' or 'read' mode using Iceberg REST Catalog.

    Prerequisites:
    - Start the Iceberg REST catalog server:
      docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0
    - For read mode: Install PySpark:
      pip install pyspark

    Args:
        beam_options: Apache Beam pipeline options
        mode: 'write' to write data, 'read' to read data
        rest_catalog_uri: URI of the REST catalog server (default: http://localhost:8181)
        warehouse_path: Custom warehouse path (default: temporary directory)
        table_name: Name of the Iceberg table (default: None -  generates a random table name)
        deltacat_converter_interval: Interval for DeltaCat optimizer monitoring
        ray_inactivity_timeout: Timeout for shutting down Ray cluster
        max_converter_parallelism: Maximum number of concurrent converter tasks
        filesystem: PyArrow filesystem instance (default: LocalFileSystem)

    Pipeline Operations:
    - 'write': Write sample data to the Iceberg table with merge-on-read functionality.
      Uses job-based table monitoring for better scalability and resource management.
    - 'read': Read deduplicated data from the Iceberg table using Spark SQL.
      Uses Spark SQL instead of Beam's native Iceberg I/O to properly handle positional deletes.
    """
    # Use custom warehouse path or create a temporary one
    if warehouse_path is None:
        warehouse_path = os.path.join("/tmp", "iceberg_rest_warehouse")
        os.makedirs(warehouse_path, exist_ok=True)

    # Use provided filesystem or create a LocalFileSystem by default
    if filesystem is None:
        filesystem = pafs.LocalFileSystem()

    # Generate unique table name if using default to avoid conflicts
    if not table_name:
        random_suffix = generate_random_suffix()
        table_name = f"default.demo_table_{random_suffix}"
        print(f"📋 Generated unique table name: {table_name}")

    # Define catalog configuration for REST catalog (simplified, table creation handled separately)
    catalog_config = {
        "catalog_properties": {
            "warehouse": warehouse_path,
            "catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
            "uri": rest_catalog_uri,
        },
        "deltacat_converter_properties": {
            "deltacat_converter_interval": deltacat_converter_interval,
            "merge_keys": ["id"],  # Configure merge keys for duplicate detection
            "ray_inactivity_timeout": ray_inactivity_timeout,
            "filesystem": filesystem,  # Pass filesystem to DeltaCAT converter
            "max_converter_parallelism": max_converter_parallelism,
        },
    }

    # Ensure table name includes namespace
    if "." not in table_name:
        full_table_name = f"default.{table_name}"
    else:
        full_table_name = table_name

    print(f"🔧 Using Iceberg REST Catalog")
    print(f"   REST Server: {rest_catalog_uri}")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Mode: {mode}")
    print(f"   Table: {full_table_name}")
    print(f"   Filesystem: {type(filesystem).__name__}")

    # Remind user about prerequisites
    if mode == "write":
        print("📋 Prerequisites:")
        print("   Make sure the Iceberg REST catalog server is running:")
        print(
            "   docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0"
        )
        print()

    with beam.Pipeline(options=beam_options) as p:
        if mode == "write":
            # Step 1: Write initial data to create the table
            initial_data = p | "Create initial data" >> beam.Create(
                [
                    Row(id=1, name="Alice", value=100, version=1),
                    Row(id=2, name="Bob", value=200, version=1),
                    Row(id=3, name="Charlie", value=300, version=1),
                    Row(id=4, name="David", value=400, version=1),
                ]
            )

            initial_data | "Write initial data to Iceberg" >> beam.managed.Write(
                beam.managed.ICEBERG,
                config={
                    "table": full_table_name,  # Use fully qualified table name for REST catalog
                    "write_mode": "append",
                    **catalog_config,
                },
            )

            # Step 2: Write additional data (this will create new data files)
            additional_data = p | "Create additional data" >> beam.Create(
                [
                    Row(id=5, name="Eve", value=500, version=1),
                    Row(id=6, name="Frank", value=600, version=1),
                    Row(id=7, name="Grace", value=700, version=1),
                    Row(id=8, name="Henry", value=800, version=1),
                ]
            )

            additional_data | "Write additional data to Iceberg" >> beam.managed.Write(
                beam.managed.ICEBERG,
                config={
                    "table": full_table_name,  # Use fully qualified table name for REST catalog
                    "write_mode": "append",
                    **catalog_config,
                },
            )

            # Wait before writing updates to prior IDs to ensure that it's appended last
            time.sleep(1)

            # Step 3: Write updates to existing records (this creates merge-on-read scenarios)
            # These updates will be written as new data files, and the DeltaCat optimizer will
            # detect duplicates and trigger converter jobs to create position delete files
            updated_data = p | "Create updated data" >> beam.Create(
                [
                    Row(
                        id=2, name="Robert", value=201, version=2
                    ),  # Update Bob's record
                    Row(
                        id=3, name="Charles", value=301, version=2
                    ),  # Update Charlie's record
                    Row(id=9, name="Ivy", value=900, version=1),  # Add a new record
                ]
            )

            updated_data | "Write updated data to Iceberg" >> beam.managed.Write(
                beam.managed.ICEBERG,
                config={
                    "table": full_table_name,  # Use fully qualified table name for REST catalog
                    "write_mode": "append",
                    **catalog_config,
                },
            )

            print(f"\n📝 Data writing completed with DeltaCat optimization enabled.")
            print(
                f"   - Table monitoring interval: {deltacat_converter_interval} seconds"
            )
            print(
                f"   - Ray cluster shutdown timeout: {ray_inactivity_timeout} seconds"
            )
            print(f"   - Automatic duplicate detection and resolution")
            print(f"   - Position delete creation for duplicate resolution")
            print(f"   - Job-based table monitoring with Ray")
            print(f"   - Filesystem: {type(filesystem).__name__}")
            print(f"Read the table with: `python main.py --mode read --table-name {full_table_name}`")

        elif mode == "read":
            # Read from the Iceberg table using Spark SQL
            # Note: We use Spark SQL instead of beam.managed.Read because Beam's native Iceberg I/O
            # cannot handle positional delete files created by DeltaCAT converter sessions.

            print(f"📖 Reading from Iceberg table '{full_table_name}' using Spark SQL")

            # Create a trigger element to start the read
            trigger = p | "Create read trigger" >> beam.Create([None])

            # Read from Iceberg table using Spark SQL
            elements = trigger | "Read with Spark SQL" >> beam.ParDo(
                SparkSQLIcebergRead(
                    table_name=full_table_name,
                    catalog_uri=rest_catalog_uri,
                    warehouse=warehouse_path,
                )
            )

            # Display the data read (after positional deletes are applied)
            elements | "Print deduplicated data" >> beam.Map(
                lambda row: print(f"📋 Record: {row}")
            )

            # Count records for summary
            def count_and_display(elements_list):
                print(f"\n📊 Read Summary:")
                print(f"   - Total records: {len(elements_list)}")
                return elements_list

            # Collect all elements for counting
            elements | "Count records" >> beam.combiners.ToList() | "Display summary" >> beam.Map(
                count_and_display
            )

            # Verify that the data was correctly merged by ID
            verify_duplicate_resolution(full_table_name, warehouse_path)

        elif mode == "rewrite":
            # Rewrite table data files to materialize positional deletes
            print(f"🔄 Rewriting Iceberg table to materialize positional deletes")
            print(f"   - Table: {full_table_name}")
            print(f"   - Purpose: Remove positional deletes to enable Beam writes")
            print(f"   - Method: Spark rewrite_data_files procedure")

            # Create a trigger element to start the rewrite
            trigger = p | "Create rewrite trigger" >> beam.Create(
                [f"rewrite_{full_table_name}"]
            )

            # Use Spark SQL to rewrite the table
            rewrite_results = trigger | "Rewrite table with Spark SQL" >> beam.ParDo(
                SparkSQLIcebergRewrite(
                    catalog_uri=rest_catalog_uri,
                    warehouse_path=warehouse_path,
                    table_name=full_table_name,
                )
            )

            # Log the results
            rewrite_results | "Log rewrite results" >> beam.Map(
                lambda result: print(f"📋 Rewrite result: {result}")
            )
        else:
            raise ValueError(
                f"Unknown mode: {mode}. Use 'write', 'read', or 'rewrite'."
            )
