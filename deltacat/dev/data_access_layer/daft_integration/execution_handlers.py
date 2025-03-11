"""
Custom execution handlers for DeltaCAT integration with Daft.

These handlers implement the actual logic for writing data from Daft to
DeltaCAT storage formats via the Writer interface.
"""

import functools
from typing import Any, Dict, List, Optional, Iterable

import pyarrow as pa
from daft.table import MicroPartition
from daft.datatype import DataType
from deltacat.dev.data_access_layer.storage.writer import Writer, WriteOptions

def deltacat_write_execution_step(table: MicroPartition, **kwargs) -> MicroPartition:
    """
    Custom execution step to write DeltaCAT data.
    
    This function processes a single partition of data, converting it to PyArrow RecordBatches
    and writing through the DeltaCAT Writer interface.
    
    Args:
        table: The MicroPartition containing the data to write
        kwargs: Context containing writer, write_options, and io_config
        
    Returns:
        MicroPartition with metadata about the write operation
    """
    # Extract writer and options from context
    context = kwargs.get("execution_context", {})
    writer = context.get("writer")
    write_options = context.get("write_options")
    
    if writer is None or write_options is None:
        raise ValueError("Both writer and write_options must be provided in execution_context")
    
    # Convert MicroPartition to PyArrow Table
    arrow_table = table.to_arrow()
    
    # Convert to record batches
    record_batches = [batch for batch in arrow_table.to_batches()]
    
    # Call write_batches and collect results
    write_results = list(writer.write_batches(record_batches, write_options))
    
    # Convert write results to a format suitable for MicroPartition
    # This ensures all values are serializable across distributed execution
    serializable_results = []
    for result in write_results:
        if isinstance(result, dict):
            # Ensure all dictionary values are serializable
            serialized_result = {}
            for key, value in result.items():
                # Convert complex types to strings for serialization
                if isinstance(value, (str, int, float, bool)) or value is None:
                    serialized_result[key] = value
                else:
                    serialized_result[key] = str(value)
            serializable_results.append(serialized_result)
        else:
            # If not a dict, convert to string representation
            serializable_results.append(str(result))
    
    # Return MicroPartition with the write results
    # Use a list column to preserve the structure across distributed execution
    return MicroPartition.from_pydict({"write_results": [serializable_results]})

def process_deltacat_results(tables: Iterable[MicroPartition]) -> List[Any]:
    """
    Process results from multiple DeltaCAT write operations.
    
    Extracts and concatenates all write results from multiple partitions.
    
    Args:
        tables: An iterable of MicroPartitions containing write results
        
    Returns:
        A list of write results that can be passed to writer.commit()
    """
    all_results = []
    
    for table in tables:
        dict_data = table.to_pydict()
        
        if "write_results" in dict_data:
            for result_list in dict_data["write_results"]:
                if isinstance(result_list, list):
                    all_results.extend(result_list)
                else:
                    all_results.append(result_list)
    
    return all_results