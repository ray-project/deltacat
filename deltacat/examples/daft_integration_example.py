"""
Example demonstrating how to use the Daft-DeltaCAT integration.

This example shows how to use the write_deltacat method to write
data from a Daft DataFrame to DeltaCAT storage formats.
"""

import os
import tempfile
import daft
import ray

from deltacat.dev.data_access_layer.daft_integration import patch_daft
from deltacat.dev.data_access_layer.storage.rivulet_writer import RivuletWriter, RivuletWriteOptions


def rivulet_example(output_dir):
    """Example writing to Rivulet using Daft"""
    print("\n=== Rivulet Example ===")
    
    # Apply the monkey patch to add write_deltacat method to Daft
    patch_daft()
    
    # Create a Daft DataFrame
    df = daft.from_pydict({
        "id": list(range(1, 1001)),  # 1000 rows
        "name": [f"Name-{i}" for i in range(1, 1001)],
        "value": [float(i * 1.5) for i in range(1, 1001)],
        "is_active": [i % 2 == 0 for i in range(1, 1001)]
    })
    
    print(f"Created DataFrame with {len(df)} rows")
    df.show(5)
    
    # Define schema for Rivulet
    schema_dict = {
        "fields": [
            {"name": "id", "type": "long", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
            {"name": "value", "type": "double", "nullable": True},
            {"name": "is_active", "type": "boolean", "nullable": True}
        ],
        "merge_key": "id"
    }
    
    # Create RivuletWriter
    writer = RivuletWriter(
        base_path=output_dir,
        schema=schema_dict,
        merge_key="id"
    )
    
    # Create write options
    write_options = RivuletWriteOptions(
        write_mode="upsert",
        file_format="parquet"
    )
    
    print(f"Writing to Rivulet dataset at {output_dir}")
    
    # Write to Rivulet (synchronous operation)
    result = df.write_deltacat(writer, write_options)
    
    print("Write completed.")
    print("Result metadata:")
    result.show()
    
    # For a real application, you might want to read the data back
    # to verify it was written correctly
    
    print("=== Rivulet Example Complete ===\n")


def iceberg_example():
    """Example writing to Iceberg using Daft (requires additional setup)"""
    print("\n=== Iceberg Example ===")
    
    try:
        # This part requires PyIceberg and additional setup
        from pyiceberg.catalog import load_catalog
        
        # Apply the monkey patch if not already applied
        patch_daft()
        
        # Create sample DataFrame
        df = daft.from_pydict({
            "id": list(range(1, 101)),
            "name": [f"Name-{i}" for i in range(1, 101)],
            "value": [float(i * 1.5) for i in range(1, 101)]
        })
        
        print(f"Created DataFrame with {len(df)} rows")
        df.show(5)
        
        print("Note: This example requires a configured Iceberg catalog.")
        print("To run it, you need to modify the code with your catalog configuration.")
        print("Skipping actual write operation.")
        
        """
        # Uncomment and configure for actual use:
        
        # Load an Iceberg catalog
        catalog = load_catalog(
            "example_catalog",
            {
                "type": "rest",
                "uri": "http://localhost:8181"
                # Add other properties as needed
            }
        )
        
        # Create the IcebergWriter
        writer = IcebergWriter(
            catalog=catalog,
            table_identifier="example_db.example_table",
            partition_by=["id"]
        )
        
        # Create write options
        write_options = IcebergWriteOptions(write_mode="append")
        
        # Write to Iceberg
        result = df.write_deltacat(writer, write_options)
        
        print("Write completed.")
        print("Result metadata:")
        result.show()
        """
        
    except ImportError:
        print("Iceberg example requires pyiceberg to be installed.")
        print("Install it with: pip install 'pyiceberg>=0.7.0'")
    
    print("=== Iceberg Example Complete ===\n")


def ray_example(output_dir):
    """Example using Ray with Daft and DeltaCAT"""
    print("\n=== Ray Example ===")
    
    try:
        # Initialize Ray if not already initialized
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
        
        # Set Daft to use Ray
        daft.context.set_runner_ray()
        
        print("Using Ray as the execution engine")
        
        # Apply the monkey patch if not already applied
        patch_daft()
        
        # Create a larger DataFrame for demonstrating distributed execution
        df = daft.from_pydict({
            "id": list(range(1, 10001)),  # 10,000 rows
            "name": [f"Name-{i}" for i in range(1, 10001)],
            "value": [float(i * 1.5) for i in range(1, 10001)],
            "is_active": [i % 2 == 0 for i in range(1, 10001)]
        })
        
        # Force multiple partitions for distributed execution
        df = df.repartition(10)
        
        print(f"Created DataFrame with {len(df)} rows and {df.num_partitions()} partitions")
        df.show(5)
        
        # Define schema for Rivulet
        schema_dict = {
            "fields": [
                {"name": "id", "type": "long", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "value", "type": "double", "nullable": True},
                {"name": "is_active", "type": "boolean", "nullable": True}
            ],
            "merge_key": "id"
        }
        
        ray_output_dir = os.path.join(output_dir, "ray_output")
        os.makedirs(ray_output_dir, exist_ok=True)
        
        # Create RivuletWriter
        writer = RivuletWriter(
            base_path=ray_output_dir,
            schema=schema_dict,
            merge_key="id"
        )
        
        # Create write options
        write_options = RivuletWriteOptions(
            write_mode="upsert",
            file_format="parquet"
        )
        
        print(f"Writing to Rivulet dataset at {ray_output_dir} using Ray")
        
        # Write to Rivulet using Ray
        result = df.write_deltacat(writer, write_options)
        
        print("Write completed.")
        print("Result metadata:")
        result.show()
        
        # Reset Daft to local runner
        daft.context.set_runner_py()
        
    except ImportError:
        print("Ray example requires ray to be installed.")
        print("Install it with: pip install ray[default]")
    
    print("=== Ray Example Complete ===\n")


if __name__ == "__main__":
    # Create a temporary directory for the examples
    with tempfile.TemporaryDirectory() as tmp_dir:
        rivulet_dir = os.path.join(tmp_dir, "rivulet_example")
        os.makedirs(rivulet_dir, exist_ok=True)
        
        # Run the examples
        rivulet_example(rivulet_dir)
        iceberg_example()
        ray_example(rivulet_dir)
        
        print(f"\nAll examples completed. Output was written to: {tmp_dir}")