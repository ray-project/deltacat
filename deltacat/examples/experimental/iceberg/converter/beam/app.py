from typing import Callable, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import Row
import os
import tempfile
import pyarrow.fs as pafs
from deltacat.experimental.beam.managed import (
    read,
    write,
)

# Monkey-patch beam.managed.Write and beam.managed.Read
beam.managed.Write = write 
beam.managed.Read = read


def run(
    input_text: str,
    beam_options: Optional[PipelineOptions] = None,
    test: Callable[[beam.PCollection], None] = lambda _: None,
    mode: str = "write",  # 'write' to write data, 'read' to read data
    rest_catalog_uri: str = "http://localhost:8181",  # REST catalog server URI
    warehouse_path: Optional[str] = None,  # Optional custom warehouse path
    table_name: str = "default.demo_table",  # Table name with namespace
    deltacat_converter_interval: float = 3.0,  # Converter monitoring interval
    ray_inactivity_timeout: int = 10,  # Ray cluster shutdown timeout
    filesystem: Optional[pafs.FileSystem] = None,  # Optional PyArrow filesystem
) -> None:
    """
    Run the pipeline in either 'write' or 'read' mode using Iceberg REST Catalog.
    
    Prerequisites:
    - Start the Iceberg REST catalog server:
      docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0
    
    Args:
        input_text: Custom text to include in sample data
        beam_options: Apache Beam pipeline options
        test: Test function for validation (used in testing)
        mode: 'write' to write data, 'read' to read data
        rest_catalog_uri: URI of the REST catalog server (default: http://localhost:8181)
        warehouse_path: Custom warehouse path (default: temporary directory)
        table_name: Name of the Iceberg table
        deltacat_converter_interval: Interval for DeltaCat optimizer monitoring
        ray_inactivity_timeout: Timeout for shutting down Ray cluster
        filesystem: PyArrow filesystem instance (default: LocalFileSystem)
    
    Pipeline Operations:
    - 'write': Write sample data to the Iceberg table with merge-on-read functionality.
    - 'read': Read data from the Iceberg table and pass to test().
    """
    # Use custom warehouse path or create a temporary one
    if warehouse_path is None:
        warehouse_path = os.path.join(tempfile.gettempdir(), "iceberg_rest_warehouse")
        os.makedirs(warehouse_path, exist_ok=True)
    
    # Use provided filesystem or create a LocalFileSystem by default
    if filesystem is None:
        filesystem = pafs.LocalFileSystem()
    
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
        },
    }
    
    print(f"🔧 Using Iceberg REST Catalog")
    print(f"   REST Server: {rest_catalog_uri}")
    print(f"   Warehouse: {warehouse_path}")
    print(f"   Mode: {mode}")
    print(f"   Table: {table_name}")
    print(f"   Filesystem: {type(filesystem).__name__}")
    
    # Ensure table name includes namespace
    if "." not in table_name:
        full_table_name = f"default.{table_name}"
    else:
        full_table_name = table_name
    
    # Remind user about prerequisites
    if mode == "write":
        print("📋 Prerequisites:")
        print("   Make sure the Iceberg REST catalog server is running:")
        print("   docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0")
        print()
    
    with beam.Pipeline(options=beam_options) as p:
        if mode == "write":
            # Step 1: Write initial data to create the table
            initial_data = p | "Create initial data" >> beam.Create([
                Row(id=1, name="Alice", value=100, version=1),
                Row(id=2, name="Bob", value=200, version=1),
                Row(id=3, name="Charlie", value=300, version=1),
                Row(id=4, name=input_text, value=400, version=1)
            ])
            
            initial_data | "Write initial data to Iceberg" >> beam.managed.Write(
                beam.managed.ICEBERG,
                config={
                    "table": full_table_name,  # Use fully qualified table name for REST catalog
                    "write_mode": "append",
                    **catalog_config
                }
            )
            
            # Step 2: Write additional data (this will create new data files)
            additional_data = p | "Create additional data" >> beam.Create([
                Row(id=5, name="David", value=500, version=1),
                Row(id=6, name="Eve", value=600, version=1),
                Row(id=7, name="Frank", value=700, version=1),
                Row(id=8, name="Grace", value=800, version=1)
            ])
            
            additional_data | "Write additional data to Iceberg" >> beam.managed.Write(
                beam.managed.ICEBERG,
                config={
                    "table": full_table_name,  # Use fully qualified table name for REST catalog
                    "write_mode": "append",
                    **catalog_config
                }
            )
            
            # Step 3: Write updates to existing records (this creates merge-on-read scenarios)
            # These updates will be written as new data files, and the DeltaCat optimizer will
            # detect duplicates and trigger converter jobs to create position delete files
            updated_data = p | "Create updated data" >> beam.Create([
                Row(id=2, name="Robert", value=201, version=2),  # Update Bob's record
                Row(id=3, name="Charles", value=301, version=2),  # Update Charlie's record
                Row(id=9, name="Henry", value=900, version=1)  # Add a new record
            ])
            
            updated_data | "Write updated data to Iceberg" >> beam.managed.Write(
                beam.managed.ICEBERG,
                config={
                    "table": full_table_name,  # Use fully qualified table name for REST catalog
                    "write_mode": "append",
                    **catalog_config
                }
            )
            
            print(f"\n📝 Data writing completed with DeltaCat optimization enabled.")
            print(f"   - Table monitoring interval: {deltacat_converter_interval} seconds")
            print(f"   - Ray cluster shutdown timeout: {ray_inactivity_timeout} seconds")
            print(f"   - Automatic duplicate detection and resolution")
            print(f"   - Position delete creation for duplicate resolution")
            print(f"   - Ray-based converter processing")
            print(f"   - Filesystem: {type(filesystem).__name__}")
            
        elif mode == "read":
            # Read from the Iceberg table (merge-on-read will automatically apply updates)
            # For read operations, we only need the catalog properties, not DeltaCAT converter properties
            read_config = {
                "catalog_properties": catalog_config["catalog_properties"],
                # Note: DeltaCAT converter properties are not needed for read operations
            }
            
            elements = p | "Read from Iceberg" >> beam.managed.Read(
                beam.managed.ICEBERG,
                config={
                    "table": full_table_name,  # Use fully qualified table name for REST catalog
                    **read_config
                }
            )
            
            # Display the data read (after merge-on-read processing)
            elements | "Print the data read" >> beam.Map(print)
            
            # Used for testing only.
            test(elements)
        else:
            raise ValueError(f"Unknown mode: {mode}. Use 'write' or 'read'.")
