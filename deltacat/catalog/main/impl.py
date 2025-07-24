from typing import Any, Dict, List, Optional, Union, Tuple
import logging
import threading
from collections import defaultdict

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import numpy as np
import ray.data as rd
import daft
from ray.data._internal.pandas_block import PandasBlockSchema
import deltacat as dc

from deltacat.storage.model.manifest import ManifestAuthor
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.exceptions import (
    NamespaceAlreadyExistsError,
    StreamNotFoundError,
    TableAlreadyExistsError,
    TableVersionNotFoundError,
)
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortScheme
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace, NamespaceProperties
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.table import TableProperties, Table
from deltacat.storage.model.types import (
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    StreamFormat,
    TransactionType,
    TransactionOperationType,
)
from deltacat.storage.model.partition import (
    Partition,
    PartitionLocator,
    PartitionScheme,
    PartitionValues,
)
from deltacat.storage.model.table_version import TableVersion
from deltacat.compute.merge_on_read.model.merge_on_read_params import MergeOnReadParams
from deltacat.storage.model.types import DeltaType
from deltacat.storage import Delta
from deltacat.storage.model.transaction import Transaction, TransactionOperation
from deltacat.storage.model.metafile import Metafile
from deltacat.storage.model.types import CommitState
from deltacat.types.media import ContentType, DatasetType, DistributedDatasetType
from deltacat.types.tables import TableProperty, TableReadOptimizationLevel, TableWriteMode
from deltacat.compute.merge_on_read import MERGE_FUNC_BY_DISTRIBUTED_DATASET_TYPE
from deltacat import logs
from deltacat.constants import DEFAULT_NAMESPACE

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

"""
Default Catalog interface implementation using DeltaCAT native storage.

The functions here should not be invoked directly, but should instead be
invoked through `delegate.py` (e.g., to support passing catalog's by name, and
to ensure that each initialized `Catalog` implementation has its `inner`
property set to the `CatalogProperties` returned from `initialize()`).

The `CatalogProperties` instance returned by `initialize()` contains all
durable state required to deterministically reconstruct the associated DeltaCAT
native `Catalog` implementation (e.g., the root URI for the catalog metastore).
"""


# catalog functions
def initialize(
    config: Optional[CatalogProperties] = None,
    *args,
    **kwargs,
) -> CatalogProperties:
    """
    Performs any required one-time initialization and validation of this
    catalog implementation based on the input configuration. If no config
    instance is given, a new `CatalogProperties` instance is constructed
    using the given keyword arguments.

    Returns the input config if given, and the newly created config otherwise.
    """
    if config is not None:
        if not isinstance(config, CatalogProperties):
            raise ValueError(
                f"Expected `CatalogProperties` but found `{type(config)}`."
            )
        return config
    else:
        return CatalogProperties(*args, **kwargs)


# table functions
def write_to_table(
    data: Union[LocalTable, LocalDataset, DistributedDataset],  # type: ignore
    table: str,
    *args,
    namespace: Optional[str] = None,
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    **kwargs,
) -> None:
    """Write local or distributed data to a table. Raises an error if the
    table does not exist and the table write mode is not CREATE or AUTO.

    When creating a table, all `create_table` parameters may be optionally
    specified as additional keyword arguments. When appending to, or replacing,
    an existing table, all `alter_table` parameters may be optionally specified
    as additional keyword arguments.
    
    Args:
        data: Local or distributed data to write to the table.
        table: Name of the table to write to.
        namespace: Optional namespace for the table. Uses default if not specified.
        mode: Write mode (AUTO, CREATE, APPEND, REPLACE, MERGE, DELETE).
        content_type: Content type for the data files.
        **kwargs: Additional keyword arguments.
    """
    namespace = namespace or default_namespace()

    # Determine if table exists and what action to take
    table_exists_flag = table_exists(
        table, 
        namespace=namespace, 
        **kwargs,
    )
    logger.info(f"Table to write to ({namespace}.{table}) exists: {table_exists_flag}")

    if mode == TableWriteMode.CREATE and table_exists_flag:
        raise ValueError(f"Table {namespace}.{table} already exists and mode is CREATE")
    elif (
        mode not in (TableWriteMode.CREATE, TableWriteMode.AUTO)
        and not table_exists_flag
    ):
        raise ValueError(f"Table {namespace}.{table} does not exist and mode is {mode.value.upper()}")

    # Create table if needed
    if not table_exists_flag:
        # Handle schema: differentiate between explicit schema=None vs no schema argument
        if "schema" not in kwargs:
            # No schema argument provided - infer schema from data
            # Try to infer schema from data
            if isinstance(data, pd.DataFrame):
                # Pandas DataFrame - already supported
                arrow_schema = pa.Schema.from_pandas(data)
                schema = Schema.of(schema=arrow_schema)
            elif isinstance(data, pl.DataFrame):
                # Polars DataFrame - already supported
                arrow_table = data.to_arrow()
                schema = Schema.of(schema=arrow_table.schema)
            elif isinstance(data, (pa.Table, pa.RecordBatch, ds.Dataset)):
                # PyArrow table/dataset/recordbatch - already supported
                schema = Schema.of(schema=data.schema)
            elif isinstance(data, rd.Dataset):
                # Ray Dataset - get schema using schema() method
                ray_schema = data.schema()
                # Convert Ray schema to PyArrow schema using base_schema
                base_schema = ray_schema.base_schema
                if isinstance(base_schema, pa.Schema):
                    # Already a PyArrow schema
                    arrow_schema = base_schema
                elif isinstance(base_schema, PandasBlockSchema):
                    # PandasBlockSchema - extract names and types and convert to PyArrow
                    try:
                        # Create a DataFrame with the correct dtypes and convert to PyArrow
                        dtype_dict = {
                            name: dtype
                            for name, dtype in zip(base_schema.names, base_schema.types)
                        }
                        empty_df = pd.DataFrame(columns=base_schema.names).astype(
                            dtype_dict
                        )
                        arrow_schema = pa.Schema.from_pandas(empty_df)
                    except Exception as e:
                        raise ValueError(
                            f"Failed to convert Ray Dataset PandasBlockSchema to PyArrow schema: {e}"
                        )
                else:
                    # Unknown schema type - raise error instead of fallback
                    raise ValueError(
                        f"Unsupported Ray Dataset schema type: {type(base_schema)}. "
                        f"Expected PyArrow Schema or PandasBlockSchema, got {base_schema}"
                    )
                schema = Schema.of(schema=arrow_schema)
            elif isinstance(data, daft.DataFrame):
                # Daft DataFrame - get native schema and convert to PyArrow
                daft_schema = data.schema()
                arrow_schema = daft_schema.to_pyarrow_schema()
                schema = Schema.of(schema=arrow_schema)
            elif isinstance(data, np.ndarray):
                # NumPy ndarray - already supported
                if data.ndim == 1:
                    # 1D array - single column
                    arrow_type = pa.from_numpy_dtype(data.dtype)
                    arrow_schema = pa.schema([("column_0", arrow_type)])
                elif data.ndim == 2:
                    # 2D array - multiple columns
                    arrow_type = pa.from_numpy_dtype(data.dtype)
                    fields = [(f"column_{i}", arrow_type) for i in range(data.shape[1])]
                    arrow_schema = pa.schema(fields)
                else:
                    raise ValueError(
                        f"NumPy arrays with {data.ndim} dimensions are not supported. Only 1D and 2D arrays are supported."
                    )
                schema = Schema.of(schema=arrow_schema)
            else:
                raise ValueError(
                    "Could not infer schema from data. Please provide schema explicitly."
                )

            # Set the inferred schema in kwargs
            kwargs["schema"] = schema
        # ELSE User explicitly set schema=None - create schemaless table
        #   Keep schema=None in kwargs, don't infer
        # OR User provided an explicit schema - use it as-is
        #   Schema is already in kwargs, no action needed
        table_definition = create_table(
            table,
            namespace=namespace,
            content_types=[content_type],
            *args,
            **kwargs,
        )
    else:
        table_definition = get_table(table, namespace=namespace, **kwargs)

    # Get the active table version and stream
    table_version_obj = _get_latest_or_given_table_version(
        namespace=namespace,
        table_name=table,
        table_version=table_definition.table_version.table_version,
        **kwargs,
    )


    # Handle different write modes
    delta_type = None
    if mode == TableWriteMode.REPLACE:
        # REPLACE mode: Stage and commit a new stream to replace existing data
        table_schema = table_definition.table_version.schema
        stream = _get_storage(**kwargs).stage_stream(
            namespace=namespace,
            table_name=table,
            table_version=table_version_obj.table_version,
            **kwargs,
        )

        # Commit the new stream (this replaces the old stream)
        stream = _get_storage(**kwargs).commit_stream(
            stream=stream,
            **kwargs,
        )
        delta_type = DeltaType.UPSERT if table_schema and table_schema.merge_keys else DeltaType.APPEND
    elif mode == TableWriteMode.APPEND:
        # APPEND mode: Ensure no merge keys exist (pure append operation)
        table_schema = table_definition.table_version.schema
        if table_schema and table_schema.merge_keys:
            raise ValueError(
                f"APPEND mode cannot be used with tables that have merge keys. "
                f"Table {namespace}.{table} has merge keys: {table_schema.merge_keys}. "
                f"Use MERGE mode instead."
            )

        # Get the existing stream for append
        stream = _get_storage(**kwargs).get_stream(
            namespace=namespace,
            table_name=table,
            table_version=table_version_obj.table_version,
            **kwargs,
        )
        delta_type = DeltaType.APPEND
    elif mode == TableWriteMode.MERGE or mode == TableWriteMode.DELETE:
        # MERGE/DELETE mode: Ensure merge keys exist (required for merge/delete operations)
        table_schema = table_definition.table_version.schema
        if not table_schema or not table_schema.merge_keys:
            raise ValueError(
                f"{mode.value.upper()} mode requires tables to have at least one merge key. "
                f"Table {namespace}.{table} has no merge keys. "
                f"Use APPEND mode instead or specify merge keys in the schema."
            )

        # Get the existing stream for merge
        stream = _get_storage(**kwargs).get_stream(
            namespace=namespace,
            table_name=table,
            table_version=table_version_obj.table_version,
            **kwargs,
        )
        delta_type = DeltaType.UPSERT if mode == TableWriteMode.MERGE else DeltaType.DELETE
    else:
        # AUTO and CREATE modes: Get the existing stream
        table_schema = table_definition.table_version.schema
        stream = _get_storage(**kwargs).get_stream(
            namespace=namespace,
            table_name=table,
            table_version=table_version_obj.table_version,
            **kwargs,
        )
        delta_type = DeltaType.UPSERT if table_schema and table_schema.merge_keys else DeltaType.APPEND

    if not stream:
        raise ValueError(f"No default stream found for table {namespace}.{table}")

    # Check if table is partitioned and raise NotImplementedError
    # TODO: Honor partitioning during write based on the table's PartitionScheme.
    # This requires deriving partition values from the data based on the partition keys
    # and transforms defined in the PartitionScheme, then using those values when
    # creating/retrieving partitions.
    if (
        stream.partition_scheme
        and stream.partition_scheme.keys is not None
        and len(stream.partition_scheme.keys) > 0
    ):
        raise NotImplementedError(
            f"write_to_table does not yet support partitioned tables. "
            f"Table {namespace}.{table} has partition scheme with "
            f"{len(stream.partition_scheme.keys)} partition key(s): "
            f"{[key.name or key.key[0] for key in stream.partition_scheme.keys]}. "
            f"Please use the lower-level metastore API for partitioned tables."
        )

    # Check if table has sort keys and raise NotImplementedError
    # TODO: Implement support for SortScheme during write based on the table's sort keys.
    # This requires sorting the data according to the sort scheme before writing deltas.
    if (
        table_version_obj.sort_scheme
        and table_version_obj.sort_scheme.keys is not None
        and len(table_version_obj.sort_scheme.keys) > 0
    ):
        raise NotImplementedError(
            f"write_to_table does not yet support tables with sort keys. "
            f"Table {namespace}.{table} has sort scheme with "
            f"{len(table_version_obj.sort_scheme.keys)} sort key(s): "
            f"{[key.key[0] for key in table_version_obj.sort_scheme.keys]}. "
            f"Please use the lower-level metastore API for sorted tables."
        )

    staged_partition_for_compaction = None
    current_thread = threading.current_thread()
    thread_id = current_thread.name
    
    # Handle partition creation/retrieval based on write mode
    if (mode == TableWriteMode.REPLACE or not table_exists_flag or 
        delta_type in (DeltaType.UPSERT, DeltaType.DELETE)):
        # REPLACE mode, new table, or UPSERT/DELETE operations: Stage a new partition
        partition = _get_storage(**kwargs).stage_partition(
            stream=stream,
            **kwargs,
        )
        commit_staged_partition = (delta_type not in (DeltaType.UPSERT, DeltaType.DELETE))
        if not commit_staged_partition:
            # The staged partition will be committed during compaction
            staged_partition_for_compaction = partition
    else:
        # APPEND mode on existing table: Get existing partition
        partition = _get_storage(**kwargs).get_partition(
            stream_locator=stream.locator,
            partition_values=None,
            **kwargs,
        )
        commit_staged_partition = False

        if not partition:
            # No existing partition found, create a new one
            partition = _get_storage(**kwargs).stage_partition(
                stream=stream,
                **kwargs,
            )
            commit_staged_partition = True

    # Convert unsupported data types to supported ones before staging delta
    converted_data = data
    if isinstance(data, daft.DataFrame):
        # Daft DataFrame - convert based on execution mode
        # Check if Daft is running with Ray backend or local backend
        ctx = daft.context.get_context()
        runner = ctx.get_or_create_runner()
        runner_type = runner.name


        if runner_type == "ray":
            # Running with Ray backend - convert to Ray Dataset
            converted_data = data.to_ray_dataset()
        else:
            # Running with local backend - convert to PyArrow Table
            converted_data = data.to_arrow()

    # If we have a staged partition for compaction, we need to copy all existing deltas
    # from the current committed partition to avoid data loss
    if staged_partition_for_compaction:
        # Get the current committed partition
        current_partition = _get_storage(**kwargs).get_partition(
            stream_locator=stream.locator,
            partition_values=None,  # Assuming unpartitioned tables for now
            **kwargs,
        )
        
        if current_partition:
            logger.info(f"Copying existing deltas from committed partition {current_partition.locator} to staged partition {staged_partition_for_compaction.locator}")
            
            # List all deltas from the current committed partition
            current_thread = threading.current_thread()
            thread_id = current_thread.name
            
            existing_deltas = _get_storage(**kwargs).list_partition_deltas(
                partition_like=current_partition,
                ascending_order=True,
                include_manifest=True,
                **kwargs,
            ).all_items()
            
            # Log all delta locators that we found
            delta_locators_found = [delta.locator for delta in existing_deltas]
            
            # Copy each existing delta to the staged partition
            for existing_delta in existing_deltas:
                logger.debug(f"Copying delta {existing_delta.locator} to staged partition")
                
                # Create a copy of the existing delta with a new ID
                copied_delta = Delta.based_on(existing_delta, str(existing_delta.stream_position))
                # Update the copied delta's partition locator to point to the staged partition
                copied_delta.locator.partition_locator = staged_partition_for_compaction.locator
                 
                _get_storage(**kwargs).commit_delta(
                    copied_delta,
                    **kwargs,
                )
            
            logger.info(f"Copied {len(existing_deltas)} existing deltas to staged partition")
        else:
            logger.info(f"No existing committed partition found - this is the first write for this partition")

    # Stage a delta with the data
    current_thread = threading.current_thread()
    thread_id = current_thread.name
    
    delta = _get_storage(**kwargs).stage_delta(
        data=converted_data,
        partition=partition,
        delta_type=delta_type,
        content_type=content_type,
        author=ManifestAuthor.of(name="write_to_table", version="1.0"),
        **kwargs,
    )

    delta = _get_storage(**kwargs).commit_delta(
        delta=delta,
        **kwargs,
    )
    
    if commit_staged_partition:
        _get_storage(**kwargs).commit_partition(
            partition=partition,
            **kwargs,
        )

    # Check compaction trigger decision  
    should_compact = _trigger_compaction(
        table_version_obj,
        delta,
        TableReadOptimizationLevel.MAX,
        **kwargs,
    )
    if should_compact:
        # Run V2 compaction session to merge or delete data
        _run_compaction_session(
            table_version_obj=table_version_obj,
            partition=partition,
            staged_partition_for_compaction=staged_partition_for_compaction,
            latest_delta_stream_position=delta.stream_position,
            namespace=namespace,
            table=table,
            **kwargs,
        )
    
    

def _trigger_compaction(
        table_version_obj: TableVersion, 
        latest_delta: Optional[Delta],
        target_read_optimization_level: TableReadOptimizationLevel,
        **kwargs,
    ) -> bool:
    # Import inside function to avoid circular imports
    from deltacat.compute.compactor.utils import round_completion_reader as rci
    
    # Extract delta type from latest_delta if available, otherwise default to no compaction
    if latest_delta is not None:
        delta_type = latest_delta.type
        partition_values = latest_delta.partition_locator.partition_values
        logger.info(f"Using delta type {delta_type} from latest delta {latest_delta.locator}")
    else:
        logger.info(f"No latest delta discovered, defaulting to no compaction.")
        return False
    
    if table_version_obj.read_table_property(TableProperty.READ_OPTIMIZATION_LEVEL) == target_read_optimization_level:
        if delta_type == DeltaType.DELETE or delta_type == DeltaType.UPSERT:
            return True
        elif delta_type == DeltaType.APPEND:
            # Get default stream to determine partition locator
            stream = _get_storage(**kwargs).get_stream(
                table_version_obj.locator.namespace,
                table_version_obj.locator.table_name,
                table_version_obj.locator.table_version,
                **kwargs,
            )
            
            if not stream:
                return False
                
            # Use provided partition_values or None for unpartitioned tables
            partition_locator = PartitionLocator.of(
                stream_locator=stream.locator,
                partition_values=partition_values,
                partition_id=None,
            )
            
            # Get round completion info to determine high watermark
            round_completion_info = rci.read_round_completion_info(
                source_partition_locator=partition_locator,
                destination_partition_locator=partition_locator,
                deltacat_storage=_get_storage(**kwargs),
                deltacat_storage_kwargs=kwargs,
            )
            
            high_watermark = round_completion_info.high_watermark if round_completion_info and isinstance(round_completion_info.high_watermark, int) else 0
            
            # Get all deltas appended since last compaction
            deltas = _get_storage(**kwargs).list_deltas(
                namespace=table_version_obj.locator.namespace,
                table_name=table_version_obj.locator.table_name,
                table_version=table_version_obj.locator.table_version,
                partition_values=partition_values,
                start_stream_position=high_watermark + 1,
                **kwargs,
            )
            
            if not deltas:
                return False
                
            # Count deltas appended since last compaction
            appended_deltas_since_last_compaction = len(deltas)
            delta_trigger = table_version_obj.read_table_property(TableProperty.APPENDED_DELTA_COUNT_COMPACTION_TRIGGER)
            if delta_trigger and appended_deltas_since_last_compaction >= delta_trigger:
                return True
                
            # Count files appended since last compaction
            appended_files_since_last_compaction = 0
            for delta in deltas:
                if delta.manifest and delta.manifest.entries:
                    appended_files_since_last_compaction += len(delta.manifest.entries)
                    
            file_trigger = table_version_obj.read_table_property(TableProperty.APPENDED_FILE_COUNT_COMPACTION_TRIGGER)
            if file_trigger and appended_files_since_last_compaction >= file_trigger:
                return True
                
            # Count records appended since last compaction
            appended_records_since_last_compaction = 0
            for delta in deltas:
                if delta.meta and delta.meta.record_count:
                    appended_records_since_last_compaction += delta.meta.record_count
                    
            record_trigger = table_version_obj.read_table_property(TableProperty.APPENDED_RECORD_COUNT_COMPACTION_TRIGGER)
            if record_trigger and appended_records_since_last_compaction >= record_trigger:
                return True
    return False


def _run_compaction_session(
    table_version_obj: TableVersion,
    partition: Partition,
    staged_partition_for_compaction: Optional[Partition],
    latest_delta_stream_position: int,
    namespace: str,
    table: str,
    **kwargs,
) -> None:
    """
    Run a V2 compaction session for the given table and partition.
    
    Args:
        table_version_obj: The table version object
        partition: The partition to compact
        namespace: The table namespace
        table: The table name
        **kwargs: Additional arguments including catalog and storage parameters
    """
    # Import inside function to avoid circular imports
    from deltacat.compute.compactor.model.compact_partition_params import (
        CompactPartitionParams,
    )
    from deltacat.compute.compactor_v2.compaction_session import compact_partition
    
    try:
        # Get the table schema for primary keys
        table_schema = table_version_obj.schema
        primary_keys = set(table_schema.merge_keys) if table_schema and table_schema.merge_keys else set()
        
        # Use the provided latest delta stream position
        latest_stream_position = latest_delta_stream_position
        
        # Determine hash bucket count from previous compaction or default to 8
        hash_bucket_count = 8  # Default
        if partition.compaction_round_completion_info and partition.compaction_round_completion_info.hash_bucket_count:
            hash_bucket_count = partition.compaction_round_completion_info.hash_bucket_count
            logger.info(f"Using hash bucket count {hash_bucket_count} from previous compaction")
        else:
            logger.info(f"Using default hash bucket count {hash_bucket_count}")
        
        # Set up compaction parameters
        # Note that, for scalability, compaction always hash buckets its input partitions independently of
        # whether the parent table version is hash partitioned or not.
        compact_partition_params = CompactPartitionParams.of({
            "catalog": kwargs.get("inner", kwargs.get("catalog")),
            "source_partition_locator": staged_partition_for_compaction.locator if staged_partition_for_compaction else partition.locator,
            "destination_partition_locator": partition.locator,  # In-place compaction
            "primary_keys": primary_keys,
            "last_stream_position_to_compact": latest_stream_position,
            "deltacat_storage": _get_storage(**kwargs),
            "deltacat_storage_kwargs": kwargs,
            "list_deltas_kwargs": kwargs,
            # TODO: Determine and evolve hash bucket count based on table size
            "hash_bucket_count": hash_bucket_count,
            "records_per_compacted_file": table_version_obj.read_table_property(
                TableProperty.RECORDS_PER_COMPACTED_FILE,
            ),
            "compacted_file_content_type": ContentType.PARQUET,
            "drop_duplicates": True,
            "sort_keys": table_version_obj.sort_scheme.keys if table_version_obj.sort_scheme else None,
            "expected_previous_partition_id": staged_partition_for_compaction.previous_partition_id if staged_partition_for_compaction else None,
        })
          
        # Run V2 compaction session
        compact_partition(params=compact_partition_params, **kwargs)
    except Exception as e:
        logger.error(f"Error during compaction session for {namespace}.{table}, partition {partition.locator}: {e}")
        raise


def read_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    table_type: Optional[DatasetType] = DatasetType.PYARROW,
    distributed_dataset_type: Optional[DatasetType] = DatasetType.RAY_DATASET,
    partition_filter: Optional[List[Union[Partition, PartitionLocator]]] = None,
    stream_position_range_inclusive: Optional[Tuple[int, int]] = None,
    merge_on_read: Optional[bool] = False,
    reader_kwargs: Optional[Dict[Any, Any]] = None,
    **kwargs,
) -> DistributedDataset:  # type: ignore
    """Read a table into a distributed dataset."""

    if reader_kwargs is None:
        reader_kwargs = {}

    _validate_read_table_args(
        namespace=namespace,
        table_type=table_type,
        distributed_dataset_type=distributed_dataset_type,
        merge_on_read=merge_on_read,
        **kwargs,
    )

    table_version_obj = _get_latest_or_given_table_version(
        namespace=namespace,
        table_name=table,
        table_version=table_version,
        **kwargs,
    )
    table_version = table_version_obj.table_version

    if (
        table_version_obj.content_types is None
        or len(table_version_obj.content_types) != 1
    ):
        raise ValueError(
            "Expected exactly one content type but "
            f"found {table_version_obj.content_types}."
        )

    logger.info(
        f"Reading metadata for table={namespace}/{table}/{table_version} "
        f"with partition_filters={partition_filter} and stream position"
        f" range={stream_position_range_inclusive}"
    )

    if partition_filter is None:
        logger.info(
            f"Reading all partitions metadata in the table={table} "
            "as partition_filter was None."
        )
        partition_filter = (
            _get_storage(**kwargs)
            .list_partitions(
                table_name=table,
                namespace=namespace,
                table_version=table_version,
                **kwargs,
            )
            .all_items()
        )
        partition_filter = [partition for partition in partition_filter if partition.state == CommitState.COMMITTED]
        logger.info(f"Found {len(partition_filter)} committed partitions for table={namespace}/{table}/{table_version}") # count the number of committed partitions for each partition value
        commit_count_per_partition_value = defaultdict(int)
        for partition in partition_filter:
            commit_count_per_partition_value[partition.partition_values] += 1
        # if there are multiple committed partitions for different partition values, raise an error
        for partition_values, commit_count in commit_count_per_partition_value.items():
            if commit_count > 1:
                raise RuntimeError(
                    f"Multiple committed partitions found for table={namespace}/{table}/{table_version}. "
                    f"Partition values: {partition_values}. Commit count: {commit_count}. This should not happen."
                )

    qualified_deltas = _get_deltas_from_partition_filter(
        stream_position_range_inclusive=stream_position_range_inclusive,
        partition_filter=partition_filter,
        **kwargs,
    )
    
    # Validate that all qualified deltas are append type - merge-on-read not yet implemented
    if qualified_deltas:
        non_append_deltas = []
        for delta in qualified_deltas:
            if delta.type != DeltaType.APPEND:
                non_append_deltas.append(delta)
        
        if non_append_deltas:
            delta_types = {delta.type for delta in non_append_deltas}
            delta_info = [(str(delta.locator), delta.type) for delta in non_append_deltas[:5]]  # Show first 5
            raise NotImplementedError(
                f"Merge-on-read is not yet implemented. Found {len(non_append_deltas)} non-append deltas "
                f"with types {delta_types}. All deltas must be APPEND type for read operations. "
                f"Examples: {delta_info}. Please run compaction first to merge non-append deltas."
            )
        
        logger.info(f"Validated {len(qualified_deltas)} qualified deltas are all APPEND type")

    logger.info(
        f"Total qualified deltas={len(qualified_deltas)} "
        f"from {len(partition_filter)} partitions."
    )

    merge_on_read_params = MergeOnReadParams.of(
        {
            "deltas": qualified_deltas,
            "deltacat_storage": _get_storage(**kwargs),
            "deltacat_storage_kwargs": {**kwargs},
            "reader_kwargs": reader_kwargs,
        }
    )

    return MERGE_FUNC_BY_DISTRIBUTED_DATASET_TYPE[distributed_dataset_type.value](
        params=merge_on_read_params, **kwargs
    )


def alter_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    **kwargs,
) -> None:
    """Alter deltacat table/table_version definition.

    Modifies various aspects of a table's metadata including lifecycle state,
    schema, partitioning, sort keys, description, and properties.

    Args:
        table: Name of the table to alter.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        lifecycle_state: New lifecycle state for the table.
        schema_updates: Map of schema updates to apply.
        partition_updates: Map of partition scheme updates to apply.
        sort_keys: New sort keys scheme.
        description: New description for the table.
        properties: New table properties.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not already exist.
    """
    namespace = namespace or default_namespace()

    _get_storage(**kwargs).update_table(
        *args,
        namespace=namespace,
        table_name=table,
        description=description,
        properties=properties,
        lifecycle_state=lifecycle_state,
        **kwargs,
    )

    table_version = _get_storage(**kwargs).get_latest_table_version(
        namespace, table, **kwargs
    )
    _get_storage(**kwargs).update_table_version(
        *args,
        namespace=namespace,
        table_name=table,
        table_version=table_version.id,
        description=description,
        schema_updates=schema_updates,
        partition_updates=partition_updates,
        sort_keys=sort_keys,
        **kwargs,
    )


def create_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = LifecycleState.ACTIVE,
    schema: Optional[Schema] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    namespace_properties: Optional[NamespaceProperties] = None,
    content_types: Optional[List[ContentType]] = None,
    fail_if_exists: bool = True,
    **kwargs,
) -> TableDefinition:
    """Create an empty table in the catalog.

    If a namespace isn't provided, the table will be created within the default deltacat namespace.
    Additionally if the provided namespace does not exist, it will be created for you.


    Args:
        name: Name of the table to create.
        namespace: Optional namespace for the table. Uses default namespace if not specified.
        version: Optional version identifier for the table.
        lifecycle_state: Lifecycle state of the new table. Defaults to ACTIVE.
        schema: Schema definition for the table.
        partition_scheme: Optional partitioning scheme for the table.
        sort_keys: Optional sort keys for the table.
        description: Optional description of the table.
        table_properties: Optional properties for the table.
        namespace_properties: Optional properties for the namespace if it needs to be created.
        content_types: Optional list of allowed content types for the table.
        fail_if_exists: If True, raises an error if table already exists. If False, returns existing table.

    Returns:
        TableDefinition object for the created or existing table.

    Raises:
        TableAlreadyExistsError: If the table already exists and fail_if_exists is True.
        NamespaceNotFoundError: If the provided namespace does not exist.
    """
    namespace = namespace or default_namespace()

    table = get_table(*args, name, namespace=namespace, table_version=version, **kwargs)
    if table is not None:
        if fail_if_exists:
            raise TableAlreadyExistsError(f"Table {namespace}.{name} already exists")
        return table

    if not namespace_exists(*args, namespace, **kwargs):
        create_namespace(
            *args, namespace=namespace, properties=namespace_properties, **kwargs
        )

    (table, table_version, stream) = _get_storage(**kwargs).create_table_version(
        *args,
        namespace=namespace,
        table_name=name,
        table_version=version,
        schema=schema,
        partition_scheme=partition_scheme,
        sort_keys=sort_keys,
        table_version_description=description,
        table_description=description,
        table_properties=table_properties,
        lifecycle_state=lifecycle_state or LifecycleState.ACTIVE,
        supported_content_types=content_types,
        **kwargs,
    )

    return TableDefinition.of(
        table=table,
        table_version=table_version,
        stream=stream,
    )


def drop_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    purge: bool = False,
    **kwargs,
) -> None:
    """Drop a table from the catalog and optionally purges underlying data.

    Args:
        name: Name of the table to drop.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        purge: If True, permanently delete the table data. If False, only remove from catalog.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not exist.

    TODO: Honor purge once garbage collection is implemented.
    TODO: Drop table version if specified, possibly create a delete_table_version api.
    """
    if purge:
        raise NotImplementedError("Purge flag is not currently supported.")

    namespace = namespace or default_namespace()
    _get_storage(**kwargs).delete_table(
        *args, namespace=namespace, name=name, purge=purge, **kwargs
    )


def refresh_table(table: str, *args, namespace: Optional[str] = None, **kwargs) -> None:
    """Refresh metadata cached on the Ray cluster for the given table.

    Args:
        table: Name of the table to refresh.
        namespace: Optional namespace of the table. Uses default namespace if not specified.

    Returns:
        None
    """
    raise NotImplementedError("refresh_table not implemented")


def list_tables(
    *args, namespace: Optional[str] = None, **kwargs
) -> ListResult[TableDefinition]:
    """List a page of table definitions.

    Args:
        namespace: Optional namespace to list tables from. Uses default namespace if not specified.

    Returns:
        ListResult containing TableDefinition objects for tables in the namespace.
    """
    namespace = namespace or default_namespace()
    tables = _get_storage(**kwargs).list_tables(*args, namespace=namespace, **kwargs)
    table_definitions = [
        get_table(*args, table.table_name, namespace, **kwargs)
        for table in tables.all_items()
    ]

    return ListResult(items=table_definitions)


def get_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    **kwargs,
) -> Optional[TableDefinition]:
    """Get table definition metadata.

    Args:
        name: Name of the table to retrieve.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional specific version of the table to retrieve.
            If not specified, the latest version is used.
        stream_format: Optional stream format to retrieve. Uses the default Deltacat stream
            format if not specified.

    Returns:
        Deltacat TableDefinition if the table exists, None otherwise.

    Raises:
        TableVersionNotFoundError: If the table version does not exist.
        StreamNotFoundError: If the stream does not exist.
    """
    namespace = namespace or default_namespace()
    table: Optional[Table] = _get_storage(**kwargs).get_table(
        *args, table_name=name, namespace=namespace, **kwargs
    )

    if table is None:
        return None

    table_version: Optional[TableVersion] = _get_storage(**kwargs).get_table_version(
        *args, namespace, name, table_version or table.latest_table_version, **kwargs
    )

    if table_version is None:
        raise TableVersionNotFoundError(
            f"TableVersion {namespace}.{name}.{table_version} does not exist."
        )

    stream = _get_storage(**kwargs).get_stream(
        *args,
        namespace=namespace,
        table_name=name,
        table_version=table_version.id,
        stream_format=stream_format,
        **kwargs,
    )

    if stream is None:
        raise StreamNotFoundError(
            f"Stream {namespace}.{table}.{table_version}.{stream} does not exist."
        )

    return TableDefinition.of(
        table=table,
        table_version=table_version,
        stream=stream,
    )


def truncate_table(
    table: str, *args, namespace: Optional[str] = None, **kwargs
) -> None:
    """Truncate table data.

    Args:
        table: Name of the table to truncate.
        namespace: Optional namespace of the table. Uses default namespace if not specified.

    Returns:
        None
    """
    raise NotImplementedError("truncate_table not implemented")


def rename_table(
    table: str, new_name: str, *args, namespace: Optional[str] = None, **kwargs
) -> None:
    """Rename an existing table.

    Args:
        table: Current name of the table.
        new_name: New name for the table.
        namespace: Optional namespace of the table. Uses default namespace if not specified.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not exist.
    """
    namespace = namespace or default_namespace()
    _get_storage(**kwargs).update_table(
        *args, table_name=table, new_table_name=new_name, namespace=namespace, **kwargs
    )


def table_exists(table: str, *args, namespace: Optional[str] = None, **kwargs) -> bool:
    """Check if a table exists in the catalog.

    Args:
        table: Name of the table to check.
        namespace: Optional namespace of the table. Uses default namespace if not specified.

    Returns:
        True if the table exists, False otherwise.
    """
    namespace = namespace or default_namespace()
    return _get_storage(**kwargs).table_exists(
        *args, table_name=table, namespace=namespace, **kwargs
    )


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces.

    Args:
        catalog: Catalog properties instance.

    Returns:
        ListResult containing Namespace objects.
    """
    return _get_storage(**kwargs).list_namespaces(*args, **kwargs)


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Get metadata for a specific table namespace.

    Args:
        namespace: Name of the namespace to retrieve.

    Returns:
        Namespace object if the namespace exists, None otherwise.
    """
    return _get_storage(**kwargs).get_namespace(*args, namespace=namespace, **kwargs)


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Check if a namespace exists.

    Args:
        namespace: Name of the namespace to check.

    Returns:
        True if the namespace exists, False otherwise.
    """
    return _get_storage(**kwargs).namespace_exists(*args, namespace=namespace, **kwargs)


def create_namespace(
    namespace: str, *args, properties: Optional[NamespaceProperties] = None, **kwargs
) -> Namespace:
    """Create a new namespace.

    Args:
        namespace: Name of the namespace to create.
        properties: Optional properties for the namespace.

    Returns:
        Created Namespace object.

    Raises:
        NamespaceAlreadyExistsError: If the namespace already exists.
    """
    if namespace_exists(namespace, **kwargs):
        raise NamespaceAlreadyExistsError(f"Namespace {namespace} already exists")

    return _get_storage(**kwargs).create_namespace(
        *args, namespace=namespace, properties=properties, **kwargs
    )


def alter_namespace(
    namespace: str,
    *args,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    **kwargs,
) -> None:
    """Alter a namespace definition.

    Args:
        namespace: Name of the namespace to alter.
        properties: Optional new properties for the namespace.
        new_namespace: Optional new name for the namespace.

    Returns:
        None
    """
    _get_storage(**kwargs).update_namespace(
        namespace=namespace,
        properties=properties,
        new_namespace=new_namespace,
        *args,
        **kwargs,
    )


def drop_namespace(namespace: str, *args, purge: bool = False, **kwargs) -> None:
    """Drop a namespace and all of its tables from the catalog.

    Args:
        namespace: Name of the namespace to drop.
        purge: If True, permanently delete all tables in the namespace.
            If False, only remove from catalog.

    Returns:
        None

    TODO: Honor purge once garbage collection is implemented.
    """
    if purge:
        raise NotImplementedError("Purge flag is not currently supported.")

    _get_storage(**kwargs).delete_namespace(
        *args, namespace=namespace, purge=purge, **kwargs
    )


def default_namespace(*args, **kwargs) -> str:
    """Return the default namespace for the catalog.

    Returns:
        String name of the default namespace.
    """
    return DEFAULT_NAMESPACE  # table functions


def _validate_read_table_args(
    namespace: Optional[str] = None,
    table_type: Optional[DatasetType] = None,
    distributed_dataset_type: Optional[DistributedDatasetType] = None,
    merge_on_read: Optional[bool] = None,
    **kwargs,
):
    storage = _get_storage(**kwargs)
    if storage is None:
        raise ValueError(
            "Catalog not initialized. Did you miss calling "
            "initialize(ds=<deltacat_storage>)?"
        )

    if merge_on_read:
        raise ValueError("Merge on read not supported currently.")

    if table_type is not DatasetType.PYARROW:
        raise ValueError("Only PYARROW table type is supported as of now")

    if distributed_dataset_type is not DistributedDatasetType.DAFT:
        raise ValueError("Only DAFT dataset type is supported as of now")

    if namespace is None:
        raise ValueError(
            "namespace must be passed to uniquely identify a table in the catalog."
        )


def _get_latest_or_given_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> TableVersion:
    table_version_obj = None
    if table_version is None:
        table_version_obj = _get_storage(**kwargs).get_latest_table_version(
            namespace=namespace, table_name=table_name, *args, **kwargs
        )
        table_version = table_version_obj.table_version
    else:
        table_version_obj = _get_storage(**kwargs).get_table_version(
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            *args,
            **kwargs,
        )

    return table_version_obj


def _get_deltas_from_partition_filter(
    partition_filter: Optional[List[Union[Partition, PartitionLocator]]] = None,
    stream_position_range_inclusive: Optional[Tuple[int, int]] = None,
    *args,
    **kwargs,
):

    result_deltas = []
    start_stream_position, end_stream_position = stream_position_range_inclusive or (
        None,
        None,
    )
    for partition_like in partition_filter:
        deltas = (
            _get_storage(**kwargs)
            .list_partition_deltas(
                partition_like=partition_like,
                ascending_order=True,
                include_manifest=True,
                start_stream_position=start_stream_position,
                last_stream_position=end_stream_position,
                *args,
                **kwargs,
            )
            .all_items()
        )

        for delta in deltas:
            if (
                start_stream_position is None
                or delta.stream_position >= start_stream_position
            ) and (
                end_stream_position is None
                or delta.stream_position <= end_stream_position
            ):
                if delta.type == DeltaType.DELETE:
                    raise ValueError("DELETE type deltas are not supported")
                result_deltas.append(delta)

    return result_deltas


def _get_storage(**kwargs):
    """
    Returns the implementation of `deltacat.storage.interface` to use with this catalog

    This is configured in the `CatalogProperties` stored during initialization and passed through `delegate.py`
    """
    properties: Optional[CatalogProperties] = kwargs.get("inner")
    if properties is not None and properties.storage is not None:
        return properties.storage
    else:
        return dc.storage.metastore
