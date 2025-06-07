"""
Common utility functions for main storage compaction tests.

These functions are shared between incremental and multiple rounds compaction tests.
"""

import tempfile
import os
import shutil
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from deltacat.tests.compute.test_util_common import (
    PartitionKey,
    PartitionKeyType,
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
    BASE_TEST_DESTINATION_TABLE_VERSION,
    REBASING_NAMESPACE,
    REBASING_TABLE_NAME,
    REBASING_TABLE_VERSION,
)

from deltacat.storage import (
    DeltaType,
    Delta,
    Partition,
    Stream,
    metastore,
    PartitionKey as StoragePartitionKey,
    PartitionKeyList,
    IdentityTransform,
    PartitionScheme,
    Schema,
    SortScheme,
    PartitionLocator,
    LocalTable,
)
from deltacat.catalog.model.properties import CatalogProperties


def create_main_deltacat_storage_kwargs() -> Dict[str, Any]:
    """
    Helper function to create main deltacat storage kwargs
    
    Returns: kwargs to use for main deltacat storage, i.e. {"catalog": CatalogProperties(...)}
    """
    temp_dir = tempfile.mkdtemp()
    catalog = CatalogProperties(root=temp_dir)
    return {"catalog": catalog}


def clean_up_main_deltacat_storage_kwargs(storage_kwargs: Dict[str, Any]):
    """
    Cleans up directory created by create_main_deltacat_storage_kwargs
    """
    catalog = storage_kwargs["catalog"]
    if hasattr(catalog, 'root') and os.path.exists(catalog.root):
        shutil.rmtree(catalog.root)


def _create_table_main(
    namespace: str,
    table_name: str,
    table_version: str,
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]], 
    input_deltas: Optional[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of _create_table that works for both incremental and multiple rounds tests.
    
    For incremental tests, input_deltas is provided to extract schema.
    For multiple rounds tests, input_deltas can be None and we use a simpler approach.
    """
    # Create namespace first
    metastore.create_namespace(namespace=namespace, **ds_mock_kwargs)
    
    # Handle schema creation
    if input_deltas is not None:
        # Incremental test approach - extract schema from input deltas
        schema = input_deltas.schema
        
        # Add partition key fields to schema if they're not already present
        if partition_keys:
            for pk in partition_keys:
                field_name = pk.key_name
                if field_name not in schema.names:
                    # Add partition key field with appropriate type
                    if pk.key_type == PartitionKeyType.INT:
                        field_type = pa.int32()
                    elif pk.key_type == PartitionKeyType.STRING:
                        field_type = pa.string()
                    elif pk.key_type.value == "timestamp":  # Handle timestamp type properly
                        field_type = pa.timestamp('us')
                    else:
                        field_type = pa.string()  # Default to string
                    
                    schema = schema.append(pa.field(field_name, field_type))
        
        schema_obj = Schema.of(schema=schema)
    else:
        # Multiple rounds test approach - use None for schema (will be set later)
        schema_obj = None

    sort_scheme = SortScheme.of(sort_keys) if sort_keys else None

    # Convert test partition keys to storage partition keys
    storage_partition_keys = []
    if partition_keys:
        for pk in partition_keys:
            storage_partition_key = StoragePartitionKey.of(
                key=[pk.key_name],
                name=pk.key_name,
                transform=IdentityTransform.of(),
            )
            storage_partition_keys.append(storage_partition_key)

    # Create partition scheme
    partition_scheme = None
    if storage_partition_keys:
        partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(storage_partition_keys),
            scheme_id="default_partition_scheme"
        )

    # Create table version (which creates table and stream automatically)
    metastore.create_table_version(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        schema=schema_obj,
        partition_scheme=partition_scheme,
        sort_keys=sort_scheme,
        **ds_mock_kwargs,
    )

    return namespace, table_name, table_version


def create_src_table_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: Optional[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of create_src_table
    """
    source_namespace: str = BASE_TEST_SOURCE_NAMESPACE
    source_table_name: str = BASE_TEST_SOURCE_TABLE_NAME
    source_table_version: str = BASE_TEST_SOURCE_TABLE_VERSION
    return _create_table_main(
        source_namespace,
        source_table_name,
        source_table_version,
        sort_keys,
        partition_keys,
        input_deltas,
        ds_mock_kwargs,
    )


def create_destination_table_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: Optional[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of create_destination_table
    """
    destination_namespace: str = BASE_TEST_DESTINATION_NAMESPACE
    destination_table_name: str = BASE_TEST_DESTINATION_TABLE_NAME
    destination_table_version: str = BASE_TEST_DESTINATION_TABLE_VERSION
    return _create_table_main(
        destination_namespace,
        destination_table_name,
        destination_table_version,
        sort_keys,
        partition_keys,
        input_deltas,
        ds_mock_kwargs,
    )


def create_rebase_table_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: Optional[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of create_rebase_table
    """
    rebasing_namespace = REBASING_NAMESPACE
    rebasing_table_name = REBASING_TABLE_NAME
    rebasing_table_version = REBASING_TABLE_VERSION
    return _create_table_main(
        rebasing_namespace,
        rebasing_table_name,
        rebasing_table_version,
        sort_keys,
        partition_keys,
        input_deltas,
        ds_mock_kwargs,
    )


def _add_deltas_to_partition_main(
    deltas_ingredients: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    partition: Optional[Partition],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Optional[Delta], int]:
    """
    Add deltas to a partition using main storage
    """
    all_deltas_length = 0
    incremental_delta = None
    for (delta_data, delta_type, delete_parameters) in deltas_ingredients:
        staged_delta: Delta = metastore.stage_delta(
            delta_data,
            partition,
            delta_type,
            entry_params=delete_parameters,
            **ds_mock_kwargs,
        )
        incremental_delta = metastore.commit_delta(
            staged_delta,
            **ds_mock_kwargs,
        )
        all_deltas_length += len(delta_data) if delta_data else 0
    return incremental_delta, all_deltas_length


def add_late_deltas_to_partition_main(
    late_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    source_partition: Optional[Partition],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Optional[Delta], int]:
    """
    Add late deltas to a partition using main storage
    """
    return _add_deltas_to_partition_main(late_deltas, source_partition, ds_mock_kwargs)


def multiple_rounds_create_src_w_deltas_destination_rebase_w_deltas_strategy_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: List[pa.Table],
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Stream, Stream, Optional[Stream], bool]:
    """
    Main storage version of multiple_rounds_create_src_w_deltas_destination_rebase_w_deltas_strategy
    """
    # For multiple rounds, we need to extract the first delta to get schema
    first_delta_table = input_deltas[0][0] if input_deltas else None
    source_namespace, source_table_name, source_table_version = create_src_table_main(
        sort_keys, partition_keys, first_delta_table, ds_mock_kwargs
    )

    source_table_stream: Stream = metastore.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    
    # Convert partition values to correct types
    converted_partition_values = []
    if partition_values and partition_keys:
        for i, (value, pk) in enumerate(zip(partition_values, partition_keys)):
            if pk.key_type == PartitionKeyType.INT:
                converted_partition_values.append(int(value))
            else:
                converted_partition_values.append(value)
    else:
        converted_partition_values = partition_values
    
    staged_partition: Partition = metastore.stage_partition(
        source_table_stream, 
        converted_partition_values,
        partition_scheme_id="default_partition_scheme" if partition_keys else None,
        **ds_mock_kwargs
    )
    
    is_delete = False
    input_delta_length = 0
    for (
        input_delta,
        input_delta_type,
        input_delta_parameters,
    ) in input_deltas:
        if input_delta_type is DeltaType.DELETE:
            is_delete = True
        staged_delta = metastore.stage_delta(
            input_delta,
            staged_partition,
            input_delta_type,
            entry_params=input_delta_parameters,
            **ds_mock_kwargs,
        )
        metastore.commit_delta(staged_delta, **ds_mock_kwargs)
        input_delta_length += len(input_delta)
    metastore.commit_partition(staged_partition, **ds_mock_kwargs)

    destination_table_namespace, destination_table_name, destination_table_version = (
        create_destination_table_main(sort_keys, partition_keys, first_delta_table, ds_mock_kwargs)
    )
    destination_table_stream: Stream = metastore.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )

    # Always create rebase table for multiple rounds tests
    rebasing_table_namespace, rebasing_table_name, rebasing_table_version = (
        create_rebase_table_main(sort_keys, partition_keys, first_delta_table, ds_mock_kwargs)
    )
    rebasing_table_stream: Stream = metastore.get_stream(
        namespace=rebasing_table_namespace,
        table_name=rebasing_table_name,
        table_version=rebasing_table_version,
        **ds_mock_kwargs,
    )
    
    # Stage partition and add deltas to rebase table
    rebased_staged_partition: Partition = metastore.stage_partition(
        rebasing_table_stream, 
        converted_partition_values,
        partition_scheme_id="default_partition_scheme" if partition_keys else None,
        **ds_mock_kwargs
    )
    
    for (
        input_delta,
        input_delta_type,
        input_delta_parameters,
    ) in input_deltas:
        staged_delta = metastore.stage_delta(
            input_delta,
            rebased_staged_partition,
            input_delta_type,
            entry_params=input_delta_parameters,
            **ds_mock_kwargs,
        )
        metastore.commit_delta(staged_delta, **ds_mock_kwargs)
    metastore.commit_partition(rebased_staged_partition, **ds_mock_kwargs)

    return source_table_stream, destination_table_stream, rebasing_table_stream, is_delete


def create_src_w_deltas_destination_plus_destination_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: pa.Table,
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
    simulate_is_inplace: bool = False,
) -> Tuple[Stream, Stream, Optional[Stream], str, str, str]:
    """
    Create source with deltas and destination tables for incremental compaction testing
    """
    source_namespace, source_table_name, source_table_version = create_src_table_main(
        sort_keys, partition_keys, input_deltas, ds_mock_kwargs
    )

    source_table_stream: Stream = metastore.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    
    # Convert partition values to correct types
    converted_partition_values = []
    if partition_values and partition_keys:
        for i, (value, pk) in enumerate(zip(partition_values, partition_keys)):
            if pk.key_type == PartitionKeyType.INT:
                converted_partition_values.append(int(value))
            else:
                converted_partition_values.append(value)
    else:
        converted_partition_values = partition_values
    
    staged_partition: Partition = metastore.stage_partition(
        source_table_stream, 
        converted_partition_values,
        partition_scheme_id="default_partition_scheme" if partition_keys else None,
        **ds_mock_kwargs
    )
    metastore.commit_delta(
        metastore.stage_delta(
            input_deltas, staged_partition, input_delta_type, **ds_mock_kwargs
        ),
        **ds_mock_kwargs,
    )
    metastore.commit_partition(staged_partition, **ds_mock_kwargs)
    source_table_stream_after_committed: Stream = metastore.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    
    destination_table_namespace: Optional[str] = None
    destination_table_name: Optional[str] = None
    destination_table_version: Optional[str] = None
    if not simulate_is_inplace:
        (
            destination_table_namespace,
            destination_table_name,
            destination_table_version,
        ) = create_destination_table_main(sort_keys, partition_keys, input_deltas, ds_mock_kwargs)
    else:
        destination_table_namespace = source_namespace
        destination_table_name = source_table_name
        destination_table_version = source_table_version

    destination_table_stream: Stream = metastore.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )

    return (
        source_table_stream_after_committed,
        destination_table_stream,
        None,
        source_namespace,
        source_table_name,
        source_table_version,
    )


def create_src_w_deltas_destination_rebase_w_deltas_strategy_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: pa.Table,
    input_delta_type: DeltaType,
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Stream, Stream, Optional[Stream]]:
    """
    Main storage version of create_src_w_deltas_destination_rebase_w_deltas_strategy
    
    Creates source table with deltas, destination table, and rebase table for rebase testing.
    This test scenario sets up different source and rebase partition locators to simulate
    scenarios like hash bucket count changes.
    """
    from deltacat.utils.common import current_time_ms

    last_stream_position = current_time_ms()
    source_namespace, source_table_name, source_table_version = create_src_table_main(
        sort_keys, partition_keys, input_deltas, ds_mock_kwargs
    )

    source_table_stream: Stream = metastore.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    
    # Convert partition values to correct types, including timestamp handling
    converted_partition_values = []
    if partition_values and partition_keys:
        for i, (value, pk) in enumerate(zip(partition_values, partition_keys)):
            if pk.key_type == PartitionKeyType.INT:
                converted_partition_values.append(int(value))
            elif pk.key_type.value == "timestamp":
                # Handle timestamp partition values
                if isinstance(value, str) and "T" in value and value.endswith("Z"):
                    import pandas as pd
                    ts = pd.to_datetime(value)
                    # Convert to microseconds since epoch for PyArrow timestamp[us]
                    converted_partition_values.append(int(ts.timestamp() * 1_000_000))
                else:
                    converted_partition_values.append(value)
            else:
                converted_partition_values.append(value)
    else:
        converted_partition_values = partition_values
    
    staged_partition: Partition = metastore.stage_partition(
        source_table_stream,
        converted_partition_values,
        partition_scheme_id="default_partition_scheme" if partition_keys else None,
        **ds_mock_kwargs
    )
    staged_delta: Delta = metastore.stage_delta(
        input_deltas, staged_partition, input_delta_type, **ds_mock_kwargs
    )
    staged_delta.locator.stream_position = last_stream_position
    metastore.commit_delta(staged_delta, **ds_mock_kwargs)
    metastore.commit_partition(staged_partition, **ds_mock_kwargs)
    
    source_table_stream_after_committed: Stream = metastore.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    
    # Create the destination table
    (
        destination_table_namespace,
        destination_table_name,
        destination_table_version,
    ) = create_destination_table_main(sort_keys, partition_keys, input_deltas, ds_mock_kwargs)
    
    # Create the rebase table
    (
        rebase_table_namespace,
        rebase_table_name,
        rebase_table_version,
    ) = create_rebase_table_main(sort_keys, partition_keys, input_deltas, ds_mock_kwargs)
    
    rebasing_table_stream: Stream = metastore.get_stream(
        namespace=rebase_table_namespace,
        table_name=rebase_table_name,
        table_version=rebase_table_version,
        **ds_mock_kwargs,
    )
    
    staged_partition: Partition = metastore.stage_partition(
        rebasing_table_stream,
        converted_partition_values,
        partition_scheme_id="default_partition_scheme" if partition_keys else None,
        **ds_mock_kwargs
    )
    staged_delta: Delta = metastore.stage_delta(
        input_deltas, staged_partition, **ds_mock_kwargs
    )
    staged_delta.locator.stream_position = last_stream_position
    metastore.commit_delta(staged_delta, **ds_mock_kwargs)
    metastore.commit_partition(staged_partition, **ds_mock_kwargs)

    # Get destination stream
    destination_table_stream: Stream = metastore.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    
    rebased_stream_after_committed: Stream = metastore.get_stream(
        namespace=rebase_table_namespace,
        table_name=rebase_table_name,
        table_version=rebase_table_version,
        **ds_mock_kwargs,
    )
    
    return (
        source_table_stream_after_committed,
        destination_table_stream,
        rebased_stream_after_committed,
    )


def create_incremental_deltas_on_source_table_main(
    source_namespace: str,
    source_table_name: str,
    source_table_version: str,
    source_table_stream: Stream,
    partition_values_param,
    incremental_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    ds_mock_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[PartitionLocator, Delta, int, bool]:
    """
    Main storage version of create_incremental_deltas_on_source_table
    """
    total_records = 0
    has_delete_deltas = False
    new_delta = None
    
    # Convert partition values for partition lookup (same as in other helper functions)
    converted_partition_values_for_lookup = partition_values_param
    if partition_values_param and source_table_stream.partition_scheme and source_table_stream.partition_scheme.keys:
        converted_partition_values_for_lookup = []
        
        # Get partition field names from the storage partition scheme
        storage_partition_keys = source_table_stream.partition_scheme.keys
        partition_field_names = []
        
        for storage_key in storage_partition_keys:
            # Each storage PartitionKey has a 'key' property that contains FieldLocators
            # Extract the field name from the first FieldLocator
            field_name = storage_key.key[0] if storage_key.key else None
            partition_field_names.append(field_name)
        
        for i, value in enumerate(partition_values_param):
            # For timestamp fields like 'region_id', we need to convert the timestamp string
            if i < len(partition_field_names):
                field_name = partition_field_names[i]
                
                # Check if this is likely a timestamp field based on the value format
                if isinstance(value, str) and "T" in value and value.endswith("Z"):
                    # This looks like a timestamp string - convert it
                    import pandas as pd
                    ts = pd.to_datetime(value)
                    # Convert to microseconds since epoch for PyArrow timestamp[us]
                    converted_partition_values_for_lookup.append(int(ts.timestamp() * 1_000_000))
                elif isinstance(value, str) and value.isdigit():
                    # This looks like an integer string
                    converted_partition_values_for_lookup.append(int(value))
                else:
                    # Keep as-is
                    converted_partition_values_for_lookup.append(value)
            else:
                converted_partition_values_for_lookup.append(value)
    
    # Get the current partition to stage deltas against
    try:
        source_partition: Partition = metastore.get_partition(
            source_table_stream.locator,
            converted_partition_values_for_lookup,
            **ds_mock_kwargs,
        )
    except Exception as e:
        # If we can't get the partition, it might not exist yet. Try to create it.
        # Stage a new partition if it doesn't exist
        staged_partition: Partition = metastore.stage_partition(
            source_table_stream,
            converted_partition_values_for_lookup,
            partition_scheme_id="default_partition_scheme" if source_table_stream.partition_scheme else None,
            **ds_mock_kwargs
        )
        # Commit the empty partition first
        metastore.commit_partition(staged_partition, **ds_mock_kwargs)
        
        # Now try to get it again
        source_partition: Partition = metastore.get_partition(
            source_table_stream.locator,
            converted_partition_values_for_lookup,
            **ds_mock_kwargs,
        )

    if source_partition is None:
        raise ValueError(f"Could not create or retrieve partition for values: {converted_partition_values_for_lookup}")

    for delta_table, delta_type, properties_dict in incremental_deltas:
        # Skip None deltas (empty incremental deltas)
        if delta_table is None:
            continue
            
        total_records += len(delta_table)
        
        if delta_type == DeltaType.DELETE:
            has_delete_deltas = True

        # Stage and commit the delta
        staged_delta: Delta = metastore.stage_delta(
            delta_table, 
            source_partition, 
            delta_type, 
            entry_params=properties_dict,
            **ds_mock_kwargs
        )
        new_delta = metastore.commit_delta(staged_delta, **ds_mock_kwargs)
    
    # If all deltas were None, return None for new_delta  
    if new_delta is None:
        return None, None, total_records, has_delete_deltas
    
    # Get updated stream after deltas were committed
    source_table_stream_after_committed: Stream = metastore.get_stream(
        source_namespace,
        source_table_name,
        source_table_version,
        **ds_mock_kwargs,
    )
    
    # Get updated partition after deltas were committed
    source_partition_after_committed: Partition = metastore.get_partition(
        source_table_stream_after_committed.locator,
        converted_partition_values_for_lookup,
        **ds_mock_kwargs,
    )
    
    return (
        source_partition_after_committed.locator,
        new_delta,
        total_records,
        has_delete_deltas,
    ) 