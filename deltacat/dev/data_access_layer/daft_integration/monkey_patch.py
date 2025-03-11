"""
Monkey patches for Daft to integrate with DeltaCAT.

This module contains the implementation of write_deltacat method
which is patched into the Daft DataFrame class.
"""

import functools
from typing import Any, Dict, Optional, List, Union, Iterable

import daft
import pyarrow as pa
from daft import DataFrame
from daft.daft import IOConfig, FileFormat
from daft.context import get_context
from daft.execution import execution_step
from daft.execution.execution_step import Instruction
from daft.logical.builder import LogicalPlanBuilder

from deltacat.dev.data_access_layer.storage.writer import Writer, WriteOptions
from .execution_handlers import deltacat_write_execution_step, process_deltacat_results

# Add DeltaCAT write instruction to execution_step module
class WriteDeltaCAT(Instruction):
    """
    Instruction to write data to DeltaCAT storage formats.
    
    This instruction is used with the physical plan to write data
    to storage formats supported by DeltaCAT.
    """
    
    def __init__(self, writer: Writer, write_options: WriteOptions, io_config: Optional[IOConfig] = None):
        self.writer = writer
        self.write_options = write_options
        self.io_config = io_config
    
    def __call__(self, inputs):
        """Execute the instruction on the provided inputs."""
        if len(inputs) != 1:
            raise ValueError(f"Expected 1 input, got {len(inputs)}")
        
        partition = inputs[0]
        
        # Convert to Arrow table and record batches
        arrow_table = partition.to_arrow()
        record_batches = arrow_table.to_batches()
        
        # Write batches and collect results
        write_results = list(self.writer.write_batches(record_batches, self.write_options))
        
        # Return results as a partition for further processing
        import pyarrow as pa
        if write_results:
            # Convert complex values to strings for serialization
            serialized_results = []
            for result in write_results:
                if isinstance(result, dict):
                    serialized_result = {}
                    for key, value in result.items():
                        if isinstance(value, (str, int, float, bool)) or value is None:
                            serialized_result[key] = value
                        else:
                            serialized_result[key] = str(value)
                    serialized_results.append(serialized_result)
                else:
                    serialized_results.append(str(result))
            
            # Create a dictionary where the results are stored in a list column
            result_dict = {"write_results": [serialized_results]}
        else:
            # Empty result
            result_dict = {"write_results": [[]]}
            
        # Return as a MicroPartition
        from daft.table import MicroPartition
        return MicroPartition.from_pydict(result_dict)

# Add to physical plan to handle deltacat_write nodes
def deltacat_write(
    child_plan,
    writer: Writer,
    write_options: WriteOptions,
    io_config: Optional[IOConfig] = None
):
    """
    Physical plan operation for writing to DeltaCAT storage formats.
    
    This is added to the PhysicalPlan class to handle write_deltacat operations.
    """
    yield from (
        step.add_instruction(
            WriteDeltaCAT(
                writer=writer,
                write_options=write_options,
                io_config=io_config,
            ),
        )
        if isinstance(step, execution_step.PartitionTaskBuilder)
        else step
        for step in child_plan
    )

def write_deltacat_df_method(
    self: DataFrame,
    writer: Writer,
    write_options: WriteOptions,
    io_config: Optional[IOConfig] = None
) -> DataFrame:
    """
    Write DataFrame to DeltaCAT using provided Writer implementation.
    
    This method allows writing to any storage format supported by DeltaCAT
    through the abstracted Writer interface, including Iceberg and Rivulet.
    
    Args:
        writer: A Writer implementation instance (IcebergWriter, RivuletWriter)
        write_options: Corresponding WriteOptions for the writer
        io_config: Optional IO configuration for remote storage access
        
    Returns:
        DataFrame containing metadata about the write operation
    """
    # Set default IO config if not provided
    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    
    # Use our custom write_deltacat method on LogicalPlanBuilder
    builder = self._builder.write_deltacat(writer, write_options, io_config)
    
    # Create a DataFrame with this write operation and collect the results
    write_df = DataFrame(builder)
    # TODO collect not invoking physical plan
    results = write_df.collect()
    
    # Extract write results from all partitions using our helper function
    all_write_results = process_deltacat_results(results)
    
    # Commit write results
    commit_result = writer.commit(all_write_results)
    
    # Convert commit result to a DataFrame-friendly format
    result_dict = {}
    if isinstance(commit_result, dict):
        # Extract scalar values from the commit result
        for key, value in commit_result.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                result_dict[key] = [value]
            else:
                result_dict[key] = [str(value)]
    else:
        result_dict["result"] = [str(commit_result)]
    
    # Create a DataFrame with the result metadata
    return daft.from_pydict(result_dict)

def write_deltacat_builder_method(
    self: LogicalPlanBuilder,
    writer: Writer,
    write_options: WriteOptions,
    io_config: Optional[IOConfig] = None
) -> LogicalPlanBuilder:
    """
    Add a DeltaCAT write operation to the logical plan.
    
    Args:
        writer: A Writer implementation instance
        write_options: Corresponding WriteOptions
        io_config: Optional IO configuration
        
    Returns:
        A new LogicalPlanBuilder with the write operation
    """
    # Since we can't add direct support for DeltaCAT writes to the Rust-side LogicalPlanBuilder,
    # we'll use the table_write operation as a base and mark it specially for our custom handling
    
    # We need a temporary path for table_write - we'll override this in the physical plan
    tmp_path = "/tmp/deltacat_placeholder"
    

    # No partitioning for now
    partition_cols = None
    
    # No compression
    compression = None
    
    # Create the basic table_write node
    builder = self._builder.table_write(
        tmp_path, 
        FileFormat.Parquet,
        partition_cols, 
        compression, 
        io_config
    )
    
    # Create a new LogicalPlanBuilder with this node
    result_builder = LogicalPlanBuilder(builder)
    
    # Attach the DeltaCAT metadata to this node
    # We'll need to look for this in PhysicalPlan.to_physical_plan
    setattr(result_builder, "_deltacat_write_info", {
        "writer": writer,
        "write_options": write_options,
        "io_config": io_config
    })
    
    return result_builder

def apply_monkey_patches():
    """
    Apply all monkey patches to integrate DeltaCAT with Daft.
    
    This function should be called once at module import time to ensure
    the patches are applied before any Daft operations are performed.
    """
    # Patch DataFrame to add write_deltacat method
    DataFrame.write_deltacat = write_deltacat_df_method
    
    # Patch LogicalPlanBuilder to add write_deltacat method
    LogicalPlanBuilder.write_deltacat = write_deltacat_builder_method
    
    # Patch table_write in the execution layer to handle our DeltaCAT write
    # This is where we need to modify the behavior for our placeholder path
    from daft.execution import physical_plan
    
    # Store the original file_write function
    original_file_write = physical_plan.file_write
    
    # Create a patched version that intercepts our placeholder path
    def patched_file_write(child_plan, file_format, schema, root_dir, compression, partition_cols, io_config):
        # Check if this is our DeltaCAT placeholder
        if root_dir == "/tmp/deltacat_placeholder":
            # This is a DeltaCAT write operation
            # Extract the writer info from the attributes of the logical plan builder
            from daft.context import get_context
            from daft.runners.runner import Runner
            
            # Since we can't easily get the logical plan builder here,
            # we'll use a simpler approach - check for attributes on the child plan
            for step in child_plan:
                # Process the data using our DeltaCAT write instruction
                if hasattr(step, "writer") and hasattr(step, "write_options"):
                    writer = step.writer
                    write_options = step.write_options
                    # Call our deltacat_write function
                    return deltacat_write(child_plan, writer, write_options, io_config)
            
            # If we couldn't find the writer info, fall back to the original
            return original_file_write(child_plan, file_format, schema, root_dir, 
                                     compression, partition_cols, io_config)
        else:
            # Not our placeholder, use the original function
            return original_file_write(child_plan, file_format, schema, root_dir, 
                                     compression, partition_cols, io_config)
    
    # Replace the file_write function with our patched version
    physical_plan.file_write = patched_file_write
    
    # Add WriteDeltaCAT to execution_step module
    execution_step.WriteDeltaCAT = WriteDeltaCAT
    
    # Log the successful patching
    print("DeltaCAT integration with Daft has been successfully applied.")
