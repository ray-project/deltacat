"""
Custom execution handlers for DeltaCAT integration with Daft.

These handlers implement the actual logic for writing data from Daft to
DeltaCAT storage formats via the Writer interface.
"""

import functools
from typing import Any, Dict, List, Optional

import pyarrow as pa
from daft.table import MicroPartition
from daft.datatype import DataType

def deltacat_write_execution_step(table: MicroPartition, **kwargs) -> MicroPartition:
    """
    Custom execution step to write DeltaCAT data
    
    Args:
        table: The MicroPartition containing the data to write
        kwargs: Arguments including writer and write_options
        
    Returns:
        MicroPartition with metadata about the write operation
    """
    writer = kwargs.get("writer")
    write_options = kwargs.get("write_options")
    
    if writer is None or write_options is None:
        raise ValueError("Both writer and write_options must be provided")
    
    # Convert MicroPartition to PyArrow Table
    arrow_table = table.to_arrow()
    
    # Convert to record batches
    record_batches = [batch for batch in arrow_table.to_batches()]
    
    # Call write_batches and collect results
    write_results = list(writer.write_batches(record_batches, write_options))
    
    # Return MicroPartition with the write results
    return MicroPartition.from_pydict({"write_results": write_results})

def deltacat_write_finalize_step(table: MicroPartition, **kwargs) -> MicroPartition:
    """
    Custom execution step to finalize a write operation (commit)
    
    Args:
        table: The MicroPartition containing write results from write_batches
        kwargs: Arguments including writer
        
    Returns:
        MicroPartition with commit result metadata
    """
    writer = kwargs.get("writer")
    
    if writer is None:
        raise ValueError("Writer must be provided")
        
    # Extract write results from input table
    pydict = table.to_pydict()
    write_results = pydict.get("write_results", [])
    
    # Finalize local and commit
    local_result = writer.finalize_local(write_results)
    commit_result = writer.commit([local_result])
    
    # Convert commit result to a flat dictionary suitable for MicroPartition
    result_dict = {}
    
    if isinstance(commit_result, dict):
        # Flatten nested dictionaries for simple representation
        for key, value in commit_result.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                result_dict[key] = [value]
            elif isinstance(value, list):
                result_dict[f"{key}_count"] = [len(value)]
                result_dict[key] = [str(value)]
            else:
                result_dict[key] = [str(value)]
    else:
        result_dict["result"] = [str(commit_result)]
    
    return MicroPartition.from_pydict(result_dict)