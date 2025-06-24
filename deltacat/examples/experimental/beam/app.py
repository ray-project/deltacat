from typing import Callable, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import Row
import os
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
) -> None:
    """
    Run the pipeline in either 'write' or 'read' mode.
    - 'write': Write sample data to the Iceberg table with merge-on-read functionality.
    - 'read': Read data from the Iceberg table and pass to test().
    """
    # Define warehouse path and catalog configuration for Hadoop catalog
    warehouse_path = os.path.join(os.getcwd(), "iceberg_warehouse")
    catalog_config = {
        "catalog_properties": {
            "warehouse": warehouse_path,
            "catalog-impl": "org.apache.iceberg.hadoop.HadoopCatalog",
        }
    }
    
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
                    "table": "foo",
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
                    "table": "foo",
                    "write_mode": "append",
                    **catalog_config
                }
            )
            
            # Step 3: Write updates to existing records (this creates merge-on-read scenarios)
            # These updates will be written as new data files, and Iceberg will merge them on read
            updated_data = p | "Create updated data" >> beam.Create([
                Row(id=2, name="Robert", value=201, version=2),  # Update Bob's record
                Row(id=3, name="Charles", value=301, version=2),  # Update Charlie's record
                Row(id=9, name="Henry", value=900, version=1)  # Add a new record
            ])
            
            updated_data | "Write updated data to Iceberg" >> beam.managed.Write(
                beam.managed.ICEBERG,
                config={
                    "table": "foo",
                    "write_mode": "append",
                    **catalog_config
                }
            )
            
        elif mode == "read":
            # Read from the Iceberg table (merge-on-read will automatically apply updates)
            elements = p | "Read from Iceberg" >> beam.managed.Read(
                beam.managed.ICEBERG,
                config={
                    "table": "foo",
                    **catalog_config
                }
            )
            
            # Display the data read (after merge-on-read processing)
            elements | "Print the data read" >> beam.Map(print)
            
            # Used for testing only.
            test(elements)
        else:
            raise ValueError(f"Unknown mode: {mode}. Use 'write' or 'read'.")
