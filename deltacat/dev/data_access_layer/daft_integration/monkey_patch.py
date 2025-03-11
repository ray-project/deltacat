"""
Monkey patches for Daft to integrate with DeltaCAT.

This module contains the implementation of write_deltacat method
which is patched into the Daft DataFrame class.
"""

import functools
from typing import Any, Dict, Optional, List

import daft
from daft import DataFrame
from daft.daft import IOConfig
from daft.expressions import col, lit, udf
from daft.datatype import DataType
from daft.context import get_context

from deltacat.dev.data_access_layer.storage.writer import Writer, WriteOptions
from .execution_handlers import deltacat_write_execution_step, deltacat_write_finalize_step

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
    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    
    # Create a UDF that calls our write execution handler
    @daft.udf(return_dtype=DataType.python())
    def deltacat_write_udf(dummy_col):
        # This is just a placeholder - the actual logic is in the execution handler
        return [{"status": "success"}]
    
    # Register the actual implementation as the execution handler
    deltacat_write_udf._func = functools.partial(
        deltacat_write_execution_step, 
        writer=writer, 
        write_options=write_options
    )
    
    # Create a second UDF for committing
    @daft.udf(return_dtype=DataType.python())
    def deltacat_commit_udf(write_results):
        # This is just a placeholder - the actual logic is in the execution handler
        return [{"status": "committed"}]
    
    # Register the actual commit implementation
    deltacat_commit_udf._func = functools.partial(
        deltacat_write_finalize_step,
        writer=writer
    )
    
    # Create a dummy column to apply the UDF to
    df_with_dummy = self.with_column("__deltacat_dummy", lit(1))
    
    # Apply the write UDF to each partition
    df_with_results = df_with_dummy.with_column(
        "write_results", 
        deltacat_write_udf(df_with_dummy["__deltacat_dummy"])
    )
    
    # Keep only the write_results column
    df_write_results = df_with_results.select("write_results")
    
    # Apply the commit UDF to consolidate results
    df_committed = df_write_results.with_column(
        "commit_result",
        deltacat_commit_udf(df_write_results["write_results"])
    )
    
    # Execute the plan (blocking operation)
    result_df = df_committed.select("commit_result").collect()
    
    return result_df