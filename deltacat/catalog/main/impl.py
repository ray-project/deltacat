from typing import Any, Dict, List, Optional, Union, Tuple
import logging
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import daft
import deltacat as dc

from deltacat.storage.model.manifest import ManifestAuthor
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.exceptions import (
    NamespaceAlreadyExistsError,
    StreamNotFoundError,
    TableAlreadyExistsError,
    TableVersionNotFoundError,
    TableNotFoundError,
    TableVersionAlreadyExistsError,
    TableValidationError,
    SchemaValidationError,
)
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortScheme
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace, NamespaceProperties
from deltacat.storage.model.schema import (
    Schema,
    SchemaUpdateOperations,
)
from deltacat.storage.model.table import TableProperties, Table
from deltacat.storage.model.types import (
    Dataset,
    LifecycleState,
    StreamFormat,
    SchemaConsistencyType,
)
from deltacat.storage.model.partition import (
    Partition,
    PartitionLocator,
    PartitionScheme,
)
from deltacat.storage.model.table_version import (
    TableVersion,
    TableVersionProperties,
)
from deltacat.storage.model.types import DeltaType
from deltacat.storage import Delta
from deltacat.storage.model.types import CommitState
from deltacat.storage.model.transaction import (
    Transaction,
    setup_transaction,
)
from deltacat.types.media import (
    ContentType,
    DatasetType,
    StorageType,
)
from deltacat.types.tables import (
    SchemaEvolutionMode,
    TableProperty,
    TableReadOptimizationLevel,
    TableWriteMode,
    concat_tables,
    infer_table_schema,
)
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
def _validate_write_mode_and_table_existence(
    table: str,
    namespace: str,
    mode: TableWriteMode,
    **kwargs,
) -> bool:
    """Validate write mode against table existence and return whether table exists."""
    table_exists_flag = table_exists(
        table,
        namespace=namespace,
        **kwargs,
    )
    logger.info(f"Table to write to ({namespace}.{table}) exists: {table_exists_flag}")

    if mode == TableWriteMode.CREATE and table_exists_flag:
        raise ValueError(
            f"Table {namespace}.{table} already exists and mode is CREATE."
        )
    elif (
        mode not in (TableWriteMode.CREATE, TableWriteMode.AUTO)
        and not table_exists_flag
    ):
        raise TableNotFoundError(
            f"Table {namespace}.{table} does not exist and mode is {mode.value.upper() if hasattr(mode, 'value') else str(mode).upper()}. Use CREATE or AUTO mode to create a new table."
        )

    return table_exists_flag


def _validate_write_mode_and_table_version_existence(
    table: str,
    namespace: str,
    table_version: Optional[str],
    mode: TableWriteMode,
    **kwargs,
) -> Tuple[bool, bool]:
    """Validate write mode against table and table version existence.

    Returns:
        Tuple of (table_exists_flag, table_version_exists_flag)
    """
    # First validate table existence
    table_exists_flag = table_exists(
        table,
        namespace=namespace,
        **kwargs,
    )
    logger.info(f"Table to write to ({namespace}.{table}) exists: {table_exists_flag}")

    # Validate table existence constraints
    if mode == TableWriteMode.CREATE and table_exists_flag and table_version is None:
        raise TableAlreadyExistsError(
            f"Table {namespace}.{table} already exists and mode is CREATE"
        )
    elif (
        mode not in (TableWriteMode.CREATE, TableWriteMode.AUTO)
        and not table_exists_flag
    ):
        raise TableNotFoundError(
            f"Table {namespace}.{table} does not exist and mode is {mode.value.upper() if hasattr(mode, 'value') else str(mode).upper()}"
        )

    # Check table version existence if specified
    table_version_exists_flag = False
    if table_version is not None and table_exists_flag:
        try:
            existing_table_def = get_table(
                table,
                namespace=namespace,
                table_version=table_version,
                **kwargs,
            )
            table_version_exists_flag = existing_table_def is not None
        except (TableVersionNotFoundError, TableNotFoundError, StreamNotFoundError):
            table_version_exists_flag = False

        logger.info(
            f"Table version ({namespace}.{table}.{table_version}) exists: {table_version_exists_flag}"
        )

        # Validate table version constraints
        if mode == TableWriteMode.CREATE and table_version_exists_flag:
            raise TableVersionAlreadyExistsError(
                f"Table version {namespace}.{table}.{table_version} already exists and mode is CREATE"
            )

    return table_exists_flag, table_version_exists_flag


def _validate_content_type_against_supported_content_types(
    namespace: str,
    table: str,
    content_type: ContentType,
    supported_content_types: Optional[List[ContentType]],
) -> None:
    if supported_content_types and content_type not in supported_content_types:
        raise ValueError(
            f"Content type proposed for write to table {namespace}.{table} ({content_type}) "
            f"conflicts with the proposed list of new supported content types: {supported_content_types}"
        )


def _get_or_create_table_and_version(
    data: Dataset,
    table: str,
    namespace: str,
    table_version: Optional[str],
    table_exists_flag: bool,
    table_version_exists_flag: bool,
    content_type: ContentType,
    mode: TableWriteMode,
    *args,
    **kwargs,
) -> TableDefinition:
    """Get existing table/version or create new one based on existence flags."""
    if not table_exists_flag:
        # Table doesn't exist - create new table with specified version
        if "schema" not in kwargs:
            kwargs["schema"] = infer_table_schema(data)

        _validate_content_type_against_supported_content_types(
            namespace,
            table,
            content_type,
            kwargs.get("content_types"),
        )

        return create_table(
            table,
            namespace=namespace,
            table_version=table_version,  # Pass the specific version
            *args,
            **kwargs,
        )
    elif table_version is not None:
        if table_version_exists_flag:
            # Table version exists - get it
            return get_table(
                table,
                namespace=namespace,
                table_version=table_version,
                **kwargs,
            )
        else:
            # Table exists but version doesn't - create new version if allowed
            if mode in (TableWriteMode.CREATE, TableWriteMode.AUTO):
                # For CREATE/AUTO mode, we want to create a new table version
                if "schema" not in kwargs:
                    kwargs["schema"] = infer_table_schema(data)

                _validate_content_type_against_supported_content_types(
                    namespace,
                    table,
                    content_type,
                    kwargs.get("content_types"),
                )

                # Create a new table version directly using storage API
                # We need to bypass the create_table function's existence check
                (table_obj, table_version_obj, stream) = _get_storage(
                    **kwargs
                ).create_table_version(
                    *args,
                    namespace=namespace,
                    table_name=table,
                    table_version=table_version,
                    schema=kwargs.get("schema"),
                    partition_scheme=kwargs.get("partition_scheme"),
                    sort_keys=kwargs.get("sort_keys"),
                    table_version_description=kwargs.get("table_version_description"),
                    table_version_properties=kwargs.get("table_version_properties"),
                    table_description=kwargs.get("description"),
                    table_properties=kwargs.get("table_properties"),
                    lifecycle_state=kwargs.get(
                        "lifecycle_state", LifecycleState.ACTIVE
                    ),
                    supported_content_types=kwargs.get("content_types"),
                    **{
                        k: v
                        for k, v in kwargs.items()
                        if k
                        not in [
                            "schema",
                            "partition_scheme",
                            "sort_keys",
                            "description",
                            "table_properties",
                            "lifecycle_state",
                        ]
                    },
                )

                return TableDefinition.of(
                    table=table_obj,
                    table_version=table_version_obj,
                    stream=stream,
                )
            else:
                raise TableVersionNotFoundError(
                    f"Table version {namespace}.{table}.{table_version} does not exist. "
                    f"Use CREATE or AUTO mode to create a new table version, or omit table_version "
                    f"to use the latest version."
                )
    else:
        # No specific version requested - get latest
        return get_table(
            table,
            namespace=namespace,
            **kwargs,
        )


def write_to_table(
    data: Dataset,
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    transaction: Optional[Transaction] = None,
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
        table_version: Optional version of the table to write to. If specified,
            will create this version if it doesn't exist (in CREATE mode) or
            get this version if it exists (in other modes). If not specified,
            uses the latest version.
        mode: Write mode (AUTO, CREATE, APPEND, REPLACE, MERGE, DELETE).
        content_type: Content type used to write the data files. Defaults to PARQUET.
        schema: Optional DeltaCAT schema for the table. Used when creating tables
            and updating existing table version schemas.
        lifecycle_state: Lifecycle state of any new table version created. Defaults to ACTIVE.
        transaction: Optional transaction to append write operations to instead of
            creating and committing a new transaction.
        **kwargs: Additional keyword arguments.
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    write_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = write_transaction

    try:
        # Validate write mode and table/table version existence
        (
            table_exists_flag,
            table_version_exists_flag,
        ) = _validate_write_mode_and_table_version_existence(
            table,
            namespace,
            table_version,
            mode,
            **kwargs,
        )

        # Get or create table and table version
        table_definition = _get_or_create_table_and_version(
            data,
            table,
            namespace,
            table_version,
            table_exists_flag,
            table_version_exists_flag,
            content_type,
            mode,
            *args,
            **kwargs,
        )

        # Get the active table version and stream
        table_version_obj = _get_latest_active_or_given_table_version(
            namespace=table_definition.table.namespace,
            table_name=table_definition.table.table_name,
            table_version=table_version or table_definition.table_version.table_version,
            **kwargs,
        )

        # Handle different write modes and get stream and delta type
        stream, delta_type = _handle_write_mode(
            mode,
            table_definition,
            table_version_obj,
            namespace,
            table,
            **kwargs,
        )

        if not stream:
            raise ValueError(f"No default stream found for table {namespace}.{table}")

        # Automatically set entry_params for DELETE/MERGE modes if not provided
        _set_entry_params_if_needed(
            mode,
            table_version_obj,
            kwargs,
        )

        # Validate table configuration
        _validate_table_configuration(
            stream,
            table_version_obj,
            namespace,
            table,
        )

        # Handle partition creation/retrieval
        partition, commit_staged_partition = _handle_partition_creation(
            mode,
            table_exists_flag,
            delta_type,
            stream,
            **kwargs,
        )

        # Get table properties for schema evolution
        schema_evolution_mode = table_version_obj.read_table_property(
            TableProperty.SCHEMA_EVOLUTION_MODE
        )
        default_schema_consistency_type = table_version_obj.read_table_property(
            TableProperty.DEFAULT_SCHEMA_CONSISTENCY_TYPE
        )

        # Validate and coerce data against schema BEFORE conversion
        # This ensures we can properly handle Daft DataFrames without memory collection
        (
            validated_data,
            schema_modified,
            updated_schema,
        ) = _validate_and_coerce_data_against_schema(
            data,  # Use original data, not converted
            table_version_obj.schema,
            schema_evolution_mode=schema_evolution_mode,
            default_schema_consistency_type=default_schema_consistency_type,
        )

        # Convert validated data to supported format for storage
        converted_data = _convert_data_if_needed(validated_data)

        # Update table version if schema was modified during evolution
        if schema_modified:
            # Extract catalog properties and filter kwargs
            catalog_kwargs = {
                "catalog": kwargs.get("catalog"),
                "inner": kwargs.get("inner"),
                "transaction": write_transaction,  # Pass transaction to update_table_version
            }

            _get_storage(**catalog_kwargs).update_table_version(
                namespace=namespace,
                table_name=table,
                table_version=table_version_obj.table_version,
                schema=updated_schema,
                **catalog_kwargs,
            )

        # Stage and commit delta, handle compaction
        _stage_commit_and_compact(
            converted_data,
            partition,
            delta_type,
            content_type,
            commit_staged_partition,
            table_version_obj,
            namespace,
            table,
            **kwargs,
        )
    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during write_to_table: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            write_transaction.seal()


def _handle_write_mode(
    mode: TableWriteMode,
    table_definition: TableDefinition,
    table_version_obj: TableVersion,
    namespace: str,
    table: str,
    **kwargs,
) -> Tuple[Any, DeltaType]:  # Using Any for stream type to avoid complex imports
    """Handle different write modes and return appropriate stream and delta type."""
    table_schema = table_definition.table_version.schema

    if mode == TableWriteMode.REPLACE:
        return _handle_replace_mode(
            table_schema,
            namespace,
            table,
            table_version_obj,
            **kwargs,
        )
    elif mode == TableWriteMode.APPEND:
        return _handle_append_mode(
            table_schema,
            namespace,
            table,
            table_version_obj,
            **kwargs,
        )
    elif mode in (TableWriteMode.MERGE, TableWriteMode.DELETE):
        return _handle_merge_delete_mode(
            mode,
            table_schema,
            namespace,
            table,
            table_version_obj,
            **kwargs,
        )
    else:
        # AUTO and CREATE modes
        return _handle_auto_create_mode(
            table_schema,
            namespace,
            table,
            table_version_obj,
            **kwargs,
        )


def _handle_replace_mode(
    table_schema,
    namespace: str,
    table: str,
    table_version_obj: TableVersion,
    **kwargs,
) -> Tuple[Any, DeltaType]:
    """Handle REPLACE mode by staging and committing a new stream."""
    stream = _get_storage(**kwargs).stage_stream(
        namespace=namespace,
        table_name=table,
        table_version=table_version_obj.table_version,
        **kwargs,
    )

    stream = _get_storage(**kwargs).commit_stream(stream=stream, **kwargs)
    delta_type = (
        DeltaType.UPSERT
        if table_schema and table_schema.merge_keys
        else DeltaType.APPEND
    )
    return stream, delta_type


def _handle_append_mode(
    table_schema,
    namespace: str,
    table: str,
    table_version_obj: TableVersion,
    **kwargs,
) -> Tuple[Any, DeltaType]:
    """Handle APPEND mode by validating no merge keys and getting existing stream."""
    if table_schema and table_schema.merge_keys:
        raise SchemaValidationError(
            f"APPEND mode cannot be used with tables that have merge keys. "
            f"Table {namespace}.{table} has merge keys: {table_schema.merge_keys}. "
            f"Use MERGE mode instead."
        )

    stream = _get_table_stream(
        namespace,
        table,
        table_version_obj.table_version,
        **kwargs,
    )
    return stream, DeltaType.APPEND


def _handle_merge_delete_mode(
    mode: TableWriteMode,
    table_schema,
    namespace: str,
    table: str,
    table_version_obj: TableVersion,
    **kwargs,
) -> Tuple[Any, DeltaType]:
    """Handle MERGE/DELETE modes by validating merge keys and getting existing stream."""
    if not table_schema or not table_schema.merge_keys:
        raise TableValidationError(
            f"{mode.value.upper() if hasattr(mode, 'value') else str(mode).upper()} mode requires tables to have at least one merge key. "
            f"Table {namespace}.{table}.{table_version_obj.table_version} has no merge keys. "
            f"Use APPEND, AUTO, or REPLACE mode instead."
        )

    stream = _get_table_stream(
        namespace,
        table,
        table_version_obj.table_version,
        **kwargs,
    )
    delta_type = DeltaType.UPSERT if mode == TableWriteMode.MERGE else DeltaType.DELETE
    return stream, delta_type


def _handle_auto_create_mode(
    table_schema,
    namespace: str,
    table: str,
    table_version_obj: TableVersion,
    **kwargs,
) -> Tuple[Any, DeltaType]:
    """Handle AUTO and CREATE modes by getting existing stream."""
    stream = _get_table_stream(
        namespace,
        table,
        table_version_obj.table_version,
        **kwargs,
    )
    delta_type = (
        DeltaType.UPSERT
        if table_schema and table_schema.merge_keys
        else DeltaType.APPEND
    )
    return stream, delta_type


def _validate_table_configuration(
    stream,
    table_version_obj: TableVersion,
    namespace: str,
    table: str,
) -> None:
    """Validate table configuration for unsupported features."""
    # Check if table is partitioned
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

    # Check if table has sort keys
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


def _handle_partition_creation(
    mode: TableWriteMode,
    table_exists_flag: bool,
    delta_type: DeltaType,
    stream,
    **kwargs,
) -> Tuple[Any, bool]:  # partition, commit_staged_partition
    """Handle partition creation/retrieval based on write mode."""
    if mode == TableWriteMode.REPLACE or not table_exists_flag:
        # REPLACE mode or new table: Stage a new partition
        partition = _get_storage(**kwargs).stage_partition(stream=stream, **kwargs)
        # If we're doing UPSERT/DELETE operations, let compaction handle the commit
        commit_staged_partition = delta_type not in (DeltaType.UPSERT, DeltaType.DELETE)
        return partition, commit_staged_partition
    elif delta_type in (DeltaType.UPSERT, DeltaType.DELETE):
        # UPSERT/DELETE operations: Try to use existing committed partition first
        partition = _get_storage(**kwargs).get_partition(
            stream_locator=stream.locator,
            partition_values=None,
            **kwargs,
        )
        commit_staged_partition = False

        if not partition:
            # No existing committed partition found, stage a new one
            partition = _get_storage(**kwargs).stage_partition(stream=stream, **kwargs)
            commit_staged_partition = False  # Let compaction handle the commit

        return partition, commit_staged_partition
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
            partition = _get_storage(**kwargs).stage_partition(stream=stream, **kwargs)
            commit_staged_partition = True

        return partition, commit_staged_partition


def _convert_data_if_needed(data: Dataset) -> Dataset:
    """Convert unsupported data types to supported ones."""
    if isinstance(data, daft.DataFrame):
        # Daft DataFrame - convert based on execution mode
        ctx = daft.context.get_context()
        runner = ctx.get_or_create_runner()
        runner_type = runner.name

        if runner_type == "ray":
            # Running with Ray backend - convert to Ray Dataset
            return data.to_ray_dataset()
        else:
            # Running with local backend - convert to PyArrow Table
            return data.to_arrow()

    return data


def _validate_and_coerce_data_against_schema(
    data: Dataset,
    schema: Optional[Schema],
    schema_evolution_mode: Optional[SchemaEvolutionMode] = None,
    default_schema_consistency_type: Optional[SchemaConsistencyType] = None,
) -> Tuple[Dataset, bool, Optional[Schema]]:
    """Validate and coerce data against the table schema if schema consistency types are set.

    Args:
        data: The dataset to validate/coerce
        schema: The DeltaCAT schema to validate against (optional)
        schema_evolution_mode: How to handle fields not in schema (MANUAL or AUTO)
        default_schema_consistency_type: Default consistency type for new fields in AUTO mode

    Returns:
        Tuple[Dataset, bool, Optional[Schema]]: Validated/coerced data, flag indicating if schema was modified, and updated schema

    Raises:
        ValueError: If validation fails or coercion is not possible
    """
    if not schema:
        return data, False, None

    # Use the new generalized validation method that handles all dataset types
    validated_data, updated_schema = schema.validate_and_coerce_dataset(
        data,
        schema_evolution_mode=schema_evolution_mode,
        default_schema_consistency_type=default_schema_consistency_type,
    )

    # Check if schema was modified by comparing with original
    schema_modified = not updated_schema.equivalent_to(schema, True)
    # Return updated schema only if it was modified
    updated_schema = updated_schema if schema_modified else None

    return validated_data, schema_modified, updated_schema


def _stage_commit_and_compact(
    converted_data: Dataset,
    partition,
    delta_type: DeltaType,
    content_type: ContentType,
    commit_staged_partition: bool,
    table_version_obj: TableVersion,
    namespace: str,
    table: str,
    **kwargs,
) -> None:
    """Stage and commit delta, then handle compaction if needed."""
    # Remove schema from kwargs to avoid duplicate parameter conflict
    # We explicitly pass the correct schema from table_version_obj
    kwargs.pop("schema", None)

    # Stage a delta with the data
    delta = _get_storage(**kwargs).stage_delta(
        data=converted_data,
        partition=partition,
        delta_type=delta_type,
        content_type=content_type,
        author=ManifestAuthor.of(name="write_to_table", version="1.0"),
        schema=table_version_obj.schema,
        **kwargs,
    )

    delta = _get_storage(**kwargs).commit_delta(delta=delta, **kwargs)

    if commit_staged_partition:
        _get_storage(**kwargs).commit_partition(partition=partition, **kwargs)

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
        logger.info(
            f"Using delta type {delta_type} from latest delta {latest_delta.locator}"
        )
    else:
        logger.info(f"No latest delta discovered, defaulting to no compaction.")
        return False

    if (
        table_version_obj.read_table_property(TableProperty.READ_OPTIMIZATION_LEVEL)
        == target_read_optimization_level
    ):
        if delta_type == DeltaType.DELETE or delta_type == DeltaType.UPSERT:
            return True
        elif delta_type == DeltaType.APPEND:
            # Get default stream to determine partition locator
            stream = _get_table_stream(
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

            high_watermark = (
                round_completion_info.high_watermark
                if round_completion_info
                and isinstance(round_completion_info.high_watermark, int)
                else 0
            )

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
            delta_trigger = table_version_obj.read_table_property(
                TableProperty.APPENDED_DELTA_COUNT_COMPACTION_TRIGGER
            )
            if delta_trigger and appended_deltas_since_last_compaction >= delta_trigger:
                return True

            # Count files appended since last compaction
            appended_files_since_last_compaction = 0
            for delta in deltas:
                if delta.manifest and delta.manifest.entries:
                    appended_files_since_last_compaction += len(delta.manifest.entries)

            file_trigger = table_version_obj.read_table_property(
                TableProperty.APPENDED_FILE_COUNT_COMPACTION_TRIGGER
            )
            if file_trigger and appended_files_since_last_compaction >= file_trigger:
                return True

            # Count records appended since last compaction
            appended_records_since_last_compaction = 0
            for delta in deltas:
                if delta.meta and delta.meta.record_count:
                    appended_records_since_last_compaction += delta.meta.record_count

            record_trigger = table_version_obj.read_table_property(
                TableProperty.APPENDED_RECORD_COUNT_COMPACTION_TRIGGER
            )
            if (
                record_trigger
                and appended_records_since_last_compaction >= record_trigger
            ):
                return True
    return False


def _get_compaction_primary_keys(table_version_obj: TableVersion) -> set:
    """Extract primary keys from table schema for compaction."""
    table_schema = table_version_obj.schema
    return (
        set(table_schema.merge_keys)
        if table_schema and table_schema.merge_keys
        else set()
    )


def _get_compaction_hash_bucket_count(partition: Partition) -> int:
    """Determine hash bucket count from previous compaction or default."""
    hash_bucket_count = 8  # Default
    if (
        partition.compaction_round_completion_info
        and partition.compaction_round_completion_info.hash_bucket_count
    ):
        hash_bucket_count = partition.compaction_round_completion_info.hash_bucket_count
        logger.info(
            f"Using hash bucket count {hash_bucket_count} from previous compaction"
        )
    else:
        logger.info(f"Using default hash bucket count {hash_bucket_count}")
    return hash_bucket_count


def _get_merge_order_sort_keys(table_version_obj: TableVersion):
    """Extract sort keys from merge_order fields in schema for compaction.

    Args:
        table_version_obj: The table version containing schema

    Returns:
        List of SortKey objects from merge_order fields, or None if no merge_order fields are defined
    """
    if table_version_obj.schema:
        return table_version_obj.schema.merge_order_sort_keys()
    return None


def _create_compaction_params(
    table_version_obj: TableVersion,
    partition: Partition,
    latest_stream_position: int,
    primary_keys: set,
    hash_bucket_count: int,
    **kwargs,
):
    """Create compaction parameters for the compaction session."""
    from deltacat.compute.compactor.model.compact_partition_params import (
        CompactPartitionParams,
    )

    # Remove create_table/alter_table kwargs not needed for compaction
    kwargs.pop("lifecycle_state", None)
    kwargs.pop("schema", None)
    kwargs.pop("partition_scheme", None)
    kwargs.pop("sort_keys", None)
    kwargs.pop("table_description", None)
    kwargs.pop("table_version_description", None)
    kwargs.pop("table_properties", None)
    kwargs.pop("table_version_properties", None)
    kwargs.pop("namespace_properties", None)
    kwargs.pop("content_types", None)
    kwargs.pop("fail_if_exists", None)
    kwargs.pop("schema_updates", None)
    kwargs.pop("partition_updates", None)
    kwargs.pop("sort_key_updates", None)

    table_writer_kwargs = kwargs.pop("table_writer_kwargs", {})
    table_writer_kwargs["schema"] = table_version_obj.schema
    table_writer_kwargs["sort_scheme_id"] = table_version_obj.sort_scheme.id

    return CompactPartitionParams.of(
        {
            "catalog": kwargs.get("inner", kwargs.get("catalog")),
            "source_partition_locator": partition.locator,
            "destination_partition_locator": partition.locator,  # In-place compaction
            "primary_keys": primary_keys,
            "last_stream_position_to_compact": latest_stream_position,
            "deltacat_storage": _get_storage(**kwargs),
            "deltacat_storage_kwargs": kwargs,
            "list_deltas_kwargs": kwargs,
            "table_writer_kwargs": table_writer_kwargs,
            "hash_bucket_count": hash_bucket_count,
            "records_per_compacted_file": table_version_obj.read_table_property(
                TableProperty.RECORDS_PER_COMPACTED_FILE,
            ),
            "compacted_file_content_type": ContentType.PARQUET,
            "drop_duplicates": True,
            "sort_keys": _get_merge_order_sort_keys(table_version_obj),
        }
    )


def _run_compaction_session(
    table_version_obj: TableVersion,
    partition: Partition,
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
        latest_delta_stream_position: Stream position of the latest delta
        namespace: The table namespace
        table: The table name
        **kwargs: Additional arguments including catalog and storage parameters
    """
    # Import inside function to avoid circular imports
    from deltacat.compute.compactor_v2.compaction_session import compact_partition

    try:
        # Extract compaction configuration
        primary_keys = _get_compaction_primary_keys(table_version_obj)
        hash_bucket_count = _get_compaction_hash_bucket_count(partition)

        # Create compaction parameters
        compact_partition_params = _create_compaction_params(
            table_version_obj,
            partition,
            latest_delta_stream_position,
            primary_keys,
            hash_bucket_count,
            **kwargs,
        )

        # Run V2 compaction session
        compact_partition(params=compact_partition_params)
    except Exception as e:
        logger.error(
            f"Error during compaction session for {namespace}.{table}, "
            f"partition {partition.locator}: {e}"
        )
        raise


def _get_merge_key_field_names_from_schema(schema) -> List[str]:
    """Extract merge key field names from a DeltaCAT Schema object.

    Args:
        schema: DeltaCAT Schema object

    Returns:
        List of field names that are marked as merge keys
    """
    if not schema or not schema.merge_keys:
        return []

    merge_key_field_names = []
    field_ids_to_fields = schema.field_ids_to_fields

    for merge_key_id in schema.merge_keys:
        if merge_key_id in field_ids_to_fields:
            field = field_ids_to_fields[merge_key_id]
            merge_key_field_names.append(field.arrow.name)

    return merge_key_field_names


def _set_entry_params_if_needed(
    mode: TableWriteMode, table_version_obj, kwargs: dict
) -> None:
    """Automatically set entry_params to merge keys if not already set by user.

    Args:
        mode: The table write mode
        table_version_obj: The table version object containing schema
        kwargs: Keyword arguments dictionary that may contain entry_params
    """
    # Only set entry_params for DELETE and MERGE modes
    if mode not in [TableWriteMode.DELETE, TableWriteMode.MERGE]:
        return

    # Don't override if user already provided entry_params
    if "entry_params" in kwargs and kwargs["entry_params"] is not None:
        return

    # Get schema from table version
    if not table_version_obj or not table_version_obj.schema:
        return

    # Extract merge key field names
    merge_key_field_names = _get_merge_key_field_names_from_schema(
        table_version_obj.schema
    )

    if merge_key_field_names:
        from deltacat.storage import EntryParams

        kwargs["entry_params"] = EntryParams.of(merge_key_field_names)


def _get_table_stream(namespace: str, table: str, table_version: str, **kwargs):
    """Helper function to get a stream for a table version."""
    return _get_storage(**kwargs).get_stream(
        namespace=namespace,
        table_name=table,
        table_version=table_version,
        **kwargs,
    )


def _validate_read_table_input(
    namespace: str,
    table: str,
    table_schema: Optional[Schema],
    table_type: Optional[DatasetType],
    distributed_dataset_type: Optional[DatasetType],
) -> None:
    """Validate input parameters for read_table operation."""
    if (
        distributed_dataset_type
        and distributed_dataset_type not in DatasetType.distributed()
    ):
        raise ValueError(
            f"{distributed_dataset_type} is not a valid distributed dataset type. "
            f"Valid distributed dataset types are: {DatasetType.distributed()}."
        )
    if table_type and table_type not in DatasetType.local():
        raise ValueError(
            f"{table_type} is not a valid local table type. "
            f"Valid table types are: {DatasetType.local()}."
        )

    # For schemaless tables, distributed datasets are not yet supported
    if table_schema is None and distributed_dataset_type:
        raise NotImplementedError(
            f"Distributed dataset reading is not yet supported for schemaless tables. "
            f"Table '{namespace}.{table}' has no schema, but distributed_dataset_type={distributed_dataset_type} was specified. "
            f"Please use local storage by setting distributed_dataset_type=None."
        )


def _get_qualified_deltas_for_read(
    table: str,
    namespace: str,
    table_version: str,
    partition_filter: Optional[List[Union[Partition, PartitionLocator]]],
    **kwargs,
) -> List[Delta]:
    """Get qualified deltas for reading based on partition filter."""
    logger.info(
        f"Reading metadata for table={namespace}/{table}/{table_version} "
        f"with partition_filters={partition_filter}."
    )

    # Get partition filter if not provided
    if partition_filter is None:
        partition_filter = _get_all_committed_partitions(
            table, namespace, table_version, **kwargs
        )

    # Get deltas from partitions
    qualified_deltas = _get_deltas_from_partition_filter(
        partition_filter=partition_filter,
        **kwargs,
    )

    logger.info(
        f"Total qualified deltas={len(qualified_deltas)} "
        f"from {len(partition_filter)} partitions."
    )

    return qualified_deltas


def _get_max_parallelism(
    max_parallelism: Optional[int],
    distributed_dataset_type: Optional[DatasetType],
) -> int:
    """Get the max parallelism for a read operation."""
    if distributed_dataset_type:
        max_parallelism = max_parallelism or 100
    else:
        # TODO(pdames): Set max parallelism using available resources and dataset size
        max_parallelism = 1
    if max_parallelism < 1:
        raise ValueError(
            f"max_parallelism must be greater than 0, but got {max_parallelism}"
        )
    logger.info(f"Using max_parallelism={max_parallelism} for read operation")

    return max_parallelism


def _download_and_process_table_data(
    namespace: str,
    table: str,
    qualified_deltas: List[Delta],
    read_as: Optional[DatasetType],
    max_parallelism: Optional[int],
    columns: Optional[List[str]],
    file_path_column: Optional[str],
    table_schema: Optional[Schema],
    **kwargs,
) -> Dataset:
    """Download delta data and process result based on storage type."""
    # Merge deltas and download data
    merged_delta = Delta.merge_deltas(qualified_deltas)

    # Convert read parameters to download parameters
    table_type = (
        read_as
        if read_as in DatasetType.local()
        else (kwargs.pop("table_type", None) or DatasetType.PYARROW)
    )
    distributed_dataset_type = read_as if read_as in DatasetType.distributed() else None

    # Validate input parameters
    _validate_read_table_input(
        namespace,
        table,
        table_schema,
        table_type,
        distributed_dataset_type,
    )

    # Determine max parallelism
    max_parallelism = _get_max_parallelism(
        max_parallelism,
        distributed_dataset_type,
    )

    result = _get_storage(**kwargs).download_delta(
        merged_delta,
        table_type=read_as,
        storage_type=StorageType.DISTRIBUTED
        if distributed_dataset_type
        else StorageType.LOCAL,
        max_parallelism=max_parallelism,
        columns=columns,
        distributed_dataset_type=distributed_dataset_type,
        file_path_column=file_path_column,
        **kwargs,
    )

    # Handle local storage table concatenation
    if not distributed_dataset_type and table_type and isinstance(result, list):
        return _handle_local_table_concatenation(result, table_type, table_schema)

    return result


def _coerce_dataset_to_schema(dataset: Dataset, target_schema: pa.Schema) -> Dataset:
    """Coerce a dataset to match the target PyArrow schema using DeltaCAT Schema.coerce method."""
    # Convert target PyArrow schema to DeltaCAT schema and use its coerce method
    deltacat_schema = Schema.of(schema=target_schema)
    return deltacat_schema.coerce(dataset)


def _extract_pyarrow_schema(table_result: Dataset) -> Optional[pa.Schema]:
    """Extract PyArrow schema from various table result types."""
    if isinstance(table_result, pa.Table):
        return table_result.schema
    elif hasattr(table_result, "to_arrow"):
        return table_result.to_arrow().schema
    elif isinstance(table_result, pd.DataFrame):
        return pa.Table.from_pandas(table_result).schema
    else:
        return None


def _collect_all_column_names(results: List[Dataset]) -> set:
    """Collect all column names across all table results."""
    all_columns = set()
    for table_result in results:
        temp_schema = _extract_pyarrow_schema(table_result)
        if temp_schema:
            all_columns.update(temp_schema.names)
    return all_columns


def _build_target_schema(
    table_schema: Schema, all_columns: set, results: List[Dataset]
) -> Optional[pa.Schema]:
    """Build target schema combining table schema fields with additional columns from results."""
    latest_schema = table_schema.arrow
    target_fields = []
    table_column_names = {field.name for field in latest_schema}

    # First, add fields from table schema that are present in results
    for field in latest_schema:
        if field.name in all_columns:
            target_fields.append(field)

    # Then, add any additional columns from results that aren't in table schema
    for table_result in results:
        temp_schema = _extract_pyarrow_schema(table_result)
        if temp_schema:
            for field in temp_schema:
                if field.name in all_columns and field.name not in table_column_names:
                    target_fields.append(field)
                    table_column_names.add(field.name)
            break  # Only need to check first result for additional columns

    return pa.schema(target_fields) if target_fields else None


def _coerce_results_to_schema(
    results: Dataset, target_schema: pa.Schema
) -> List[Dataset]:
    """Coerce all table results to match the target schema."""
    coerced_results = []
    for i, table_result in enumerate(results):
        try:
            coerced_result = _coerce_dataset_to_schema(table_result, target_schema)
            coerced_results.append(coerced_result)
            logger.debug(f"Coerced table {i} to unified schema")
        except Exception as e:
            logger.warning(f"Failed to coerce table {i}: {e}. Using original result.")
            coerced_results.append(table_result)
    return coerced_results


def _handle_local_table_concatenation(
    results: Dataset, table_type: DatasetType, table_schema: Optional[Schema]
) -> Dataset:
    """Handle concatenation of local table results with schema coercion."""
    if table_schema is None:
        logger.debug(
            f"Returning raw list of {len(results)} tables for schemaless table"
        )
        return results

    try:
        # Collect all column names and build unified schema
        all_columns = _collect_all_column_names(results)
        target_schema = _build_target_schema(table_schema, all_columns, results)

        if target_schema:
            logger.debug(f"Target schema for coercion: {target_schema}")
            coerced_results = _coerce_results_to_schema(results, target_schema)
        else:
            coerced_results = results

        # Attempt concatenation
        logger.debug(
            f"Concatenating {len(coerced_results)} LOCAL tables of type {table_type}"
        )
        concatenated_result = concat_tables(coerced_results, table_type)
        logger.debug(
            f"Concatenation complete, result type: {type(concatenated_result)}"
        )
        return concatenated_result

    except Exception as e:
        logger.warning(
            f"Schema coercion or concatenation failed: {e}. Returning original results."
        )
        return results


def read_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    read_as: Optional[DatasetType] = DatasetType.DAFT,
    partition_filter: Optional[List[Union[Partition, PartitionLocator]]] = None,
    max_parallelism: Optional[int] = None,
    columns: Optional[List[str]] = None,
    file_path_column: Optional[str] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Dataset:
    """Read a table into a dataset.

    Args:
        table: Name of the table to read.
        namespace: Optional namespace of the table. Uses default if not specified.
        table_version: Optional specific version of the table to read.
        read_as: Dataset type to use for reading table files. Defaults to DatasetType.DAFT.
        partition_filter: Optional list of partitions to read from.
        max_parallelism: Optional maximum parallelism for data download. Defaults to the number of
            available CPU cores for local dataset type reads (i.e., members of DatasetType.local())
            and 100 for distributed dataset type reads (i.e., members of DatasetType.distributed()).
        columns: Optional list of columns to include in the result.
        file_path_column: Optional column name to add file paths to the result.
        transaction: Optional transaction to chain this read operation to. If provided, uncommitted
            changes from the transaction will be visible to this read operation.
        **kwargs: Additional keyword arguments.

    Returns:
        Dataset containing the table data.
    """
    # Set up transaction handling
    read_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = read_transaction

    try:
        # Resolve namespace and get table metadata
        namespace = namespace or default_namespace()

        table_version_obj = _get_latest_active_or_given_table_version(
            namespace=namespace,
            table_name=table,
            table_version=table_version,
            **kwargs,
        )

        # Get partitions and deltas to read
        qualified_deltas = _get_qualified_deltas_for_read(
            table,
            namespace,
            table_version_obj.table_version,
            partition_filter,
            **kwargs,
        )

        # Download and process the data
        # TODO(pdames): Remove once we implement a custom SerDe for pa.ParquetFile
        if read_as == DatasetType.PYARROW_PARQUET:
            max_parallelism = 1
            logger.warning(
                f"Forcing max_parallelism to 1 for PyArrow Parquet reads to avoid serialization errors."
            )
        result = _download_and_process_table_data(
            namespace,
            table,
            qualified_deltas,
            read_as,
            max_parallelism,
            columns,
            file_path_column,
            table_version_obj.schema,
            **kwargs,
        )
        return result
    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during read_table: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            read_transaction.seal()


def alter_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[SchemaUpdateOperations] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    sort_key_updates: Optional[SortScheme] = None,
    table_description: Optional[str] = None,
    table_version_description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    table_version_properties: Optional[TableVersionProperties] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """Alter deltacat table/table_version definition.

    Modifies various aspects of a table's metadata including lifecycle state,
    schema, partitioning, sort keys, description, and properties.

    Args:
        table: Name of the table to alter.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional specific version of the table to alter. Defaults to the latest active version.
        lifecycle_state: New lifecycle state for the table.
        schema_updates: Map of schema updates to apply.
        partition_updates: Map of partition scheme updates to apply.
        sort_key_updates: New sort keys scheme.
        table_description: New description for the table.
        table_version_description: New description for the table version.
        table_properties: New table properties.
        table_version_properties: New table version properties.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not already exist.
        TableVersionNotFoundError: If the specified table version or active table version does not exist.
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    alter_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = alter_transaction

    try:
        if partition_updates:
            raise NotImplementedError("Partition updates are not yet supported.")
        if sort_key_updates:
            raise NotImplementedError("Sort key updates are not yet supported.")

        _get_storage(**kwargs).update_table(
            *args,
            namespace=namespace,
            table_name=table,
            description=table_description,
            properties=table_properties,
            **kwargs,
        )

        if table_version is None:
            table_version = _get_storage(**kwargs).get_latest_active_table_version(
                namespace, table, **kwargs
            )
            if table_version is None:
                raise TableVersionNotFoundError(
                    f"No active table version found for table {namespace}.{table}. "
                    "Please specify a table_version parameter."
                )
        else:
            table_version = _get_storage(**kwargs).get_table_version(
                namespace, table, table_version, **kwargs
            )
            if table_version is None:
                raise TableVersionNotFoundError(
                    f"Table version '{table_version}' not found for table {namespace}.{table}"
                )

        # Apply schema updates if provided
        updated_schema = None
        if schema_updates is not None:
            # Get the current schema from the table version
            current_schema = table_version.schema

            # Create a SchemaUpdate from the current schema
            schema_update = current_schema.update()

            # Apply each operation in the list
            for operation in schema_updates:
                if operation.operation == "add":
                    schema_update = schema_update.add_field(operation.field)
                elif operation.operation == "remove":
                    schema_update = schema_update.remove_field(operation.field_locator)
                elif operation.operation == "update":
                    schema_update = schema_update._update_field(
                        operation.field_locator,
                        operation.field,
                    )
                else:
                    raise ValueError(
                        f"Unknown schema update operation: {operation.operation}"
                    )

            # Apply all the updates to get the final schema
            updated_schema = schema_update.apply()

        _get_storage(**kwargs).update_table_version(
            *args,
            namespace=namespace,
            table_name=table,
            table_version=table_version.id,
            lifecycle_state=lifecycle_state,
            description=table_version_description or table_description,
            schema=updated_schema,
            properties=table_version_properties,
            **kwargs,
        )

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during alter_table: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            alter_transaction.seal()


def create_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = LifecycleState.ACTIVE,
    schema: Optional[Schema] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    sort_keys: Optional[SortScheme] = None,
    table_description: Optional[str] = None,
    table_version_description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    table_version_properties: Optional[TableVersionProperties] = None,
    namespace_properties: Optional[NamespaceProperties] = None,
    content_types: Optional[List[ContentType]] = None,
    fail_if_exists: bool = True,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> TableDefinition:
    """Create an empty table in the catalog.

    If a namespace isn't provided, the table will be created within the default deltacat namespace.
    Additionally if the provided namespace does not exist, it will be created for you.

    Args:
        table: Name of the table to create.
        namespace: Optional namespace for the table. Uses default namespace if not specified.
        version: Optional version identifier for the table.
        lifecycle_state: Lifecycle state of the new table. Defaults to ACTIVE.
        schema: Schema definition for the table.
        partition_scheme: Optional partitioning scheme for the table.
        sort_keys: Optional sort keys for the table.
        table_description: Optional description of the table.
        table_version_description: Optional description for the table version.
        table_properties: Optional properties for the table.
        table_version_properties: Optional properties for the table version.
        namespace_properties: Optional properties for the namespace if it needs to be created.
        content_types: Optional list of allowed content types for the table.
        fail_if_exists: If True, raises an error if table already exists. If False, returns existing table.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        TableDefinition object for the created or existing table.

    Raises:
        TableAlreadyExistsError: If the table already exists and fail_if_exists is True.
        NamespaceNotFoundError: If the provided namespace does not exist.
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    create_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = create_transaction

    try:
        existing_table = get_table(
            table,
            namespace=namespace,
            table_version=table_version,
            *args,
            **kwargs,
        )
        if existing_table is not None:
            if fail_if_exists:
                table_identifier = (
                    f"{namespace}.{table}"
                    if not table_version
                    else f"{namespace}.{table}.{table_version}"
                )
                raise TableAlreadyExistsError(
                    f"Table {table_identifier} already exists"
                )
            return existing_table

        if not namespace_exists(namespace, **kwargs):
            create_namespace(
                namespace=namespace,
                properties=namespace_properties,
                *args,
                **kwargs,
            )

        (table, table_version, stream) = _get_storage(**kwargs).create_table_version(
            namespace=namespace,
            table_name=table,
            table_version=table_version,
            schema=schema,
            partition_scheme=partition_scheme,
            sort_keys=sort_keys,
            table_version_description=table_version_description
            if table_version_description is not None
            else table_description,
            table_description=table_description,
            table_properties=table_properties,
            table_version_properties=table_version_properties
            if table_version_properties is not None
            else table_properties,
            lifecycle_state=lifecycle_state or LifecycleState.ACTIVE,
            supported_content_types=content_types,
            *args,
            **kwargs,
        )

        result = TableDefinition.of(
            table=table,
            table_version=table_version,
            stream=stream,
        )

        return result

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during create_table: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            create_transaction.seal()


def drop_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    purge: bool = False,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """Drop a table from the catalog and optionally purges underlying data.

    Args:
        table: Name of the table to drop.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional table version of the table to drop. If not specified, the parent table of all
        table versions will be dropped.
        purge: If True, permanently delete the table data. If False, only remove from catalog.
        transaction: Optional transaction to use. If None, creates a new transaction.

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

    # Set up transaction handling
    drop_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = drop_transaction

    try:
        if not table_version:
            _get_storage(**kwargs).delete_table(
                namespace=namespace,
                table_name=table,
                purge=purge,
                *args,
                **kwargs,
            )
        else:
            _get_storage(**kwargs).update_table_version(
                namespace=namespace,
                table_name=table,
                table_version=table_version,
                lifecycle_state=LifecycleState.DELETED,
                *args,
                **kwargs,
            )

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during drop_table: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            drop_transaction.seal()


def refresh_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """Refresh metadata cached on the Ray cluster for the given table.

    Args:
        table: Name of the table to refresh.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional specific version of the table to refresh.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None
    """
    raise NotImplementedError("refresh_table not implemented")


def list_tables(
    *args,
    namespace: Optional[str] = None,
    table: Optional[str] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> ListResult[TableDefinition]:
    """List a page of table definitions.

    Args:
        namespace: Optional namespace to list tables from. Uses default namespace if not specified.
        table: Optional table to list its table versions. If not specified, lists the latest active version of each table in the namespace.
        transaction: Optional transaction to use for reading. If provided, will see uncommitted changes.

    Returns:
        ListResult containing TableDefinition objects for tables in the namespace.
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    list_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = list_transaction

    try:
        if not table:
            tables = _get_storage(**kwargs).list_tables(
                namespace=namespace,
                *args,
                **kwargs,
            )
            table_definitions = [
                get_table(table.table_name, namespace=namespace, *args, **kwargs)
                for table in tables.all_items()
            ]
        else:
            table_versions = _get_storage(**kwargs).list_table_versions(
                namespace=namespace,
                table_name=table,
                *args,
                **kwargs,
            )
            table_definitions = [
                get_table(
                    table,
                    namespace=namespace,
                    table_version=table_version.id,
                    *args,
                    **kwargs,
                )
                for table_version in table_versions.all_items()
            ]

        result = ListResult(items=table_definitions)

        return result

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during list_tables: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            list_transaction.seal()


def get_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[TableDefinition]:
    """Get table definition metadata.

    Args:
        name: Name of the table to retrieve.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional specific version of the table to retrieve. Defaults to the latest active version.
        stream_format: Optional stream format to retrieve. Defaults to DELTACAT.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        Deltacat TableDefinition if the table exists, None otherwise.

    Raises:
        TableVersionNotFoundError: If the table version does not exist.
        StreamNotFoundError: If the stream does not exist.
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    get_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = get_transaction

    try:
        table_obj: Optional[Table] = _get_storage(**kwargs).get_table(
            table_name=table,
            namespace=namespace,
            *args,
            **kwargs,
        )

        if table_obj is None:
            return None

        table_version: Optional[TableVersion] = _get_storage(
            **kwargs
        ).get_table_version(
            namespace,
            table,
            table_version or table_obj.latest_active_table_version,
            *args,
            **kwargs,
        )

        if table_version is None:
            raise TableVersionNotFoundError(
                f"TableVersion {namespace}.{table}.{table_version} does not exist."
            )

        stream = _get_storage(**kwargs).get_stream(
            namespace=namespace,
            table_name=table,
            table_version=table_version.id,
            stream_format=stream_format,
            *args,
            **kwargs,
        )

        if stream is None:
            raise StreamNotFoundError(
                f"Stream {namespace}.{table}.{table_version}.{stream} does not exist."
            )

        result = TableDefinition.of(
            table=table_obj,
            table_version=table_version,
            stream=stream,
        )

        return result

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during get_table: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            get_transaction.seal()


def truncate_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """Truncate table data.

    Args:
        table: Name of the table to truncate.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional specific version of the table to truncate. Defaults to the latest active version.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None
    """
    raise NotImplementedError("truncate_table not implemented")


def rename_table(
    table: str,
    new_name: str,
    *args,
    namespace: Optional[str] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """Rename an existing table.

    Args:
        table: Current name of the table.
        new_name: New name for the table.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not exist.
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    rename_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = rename_transaction

    try:
        _get_storage(**kwargs).update_table(
            table_name=table,
            new_table_name=new_name,
            namespace=namespace,
            *args,
            **kwargs,
        )

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during rename_table: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            rename_transaction.seal()


def table_exists(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> bool:
    """Check if a table exists in the catalog.

    Args:
        table: Name of the table to check.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional specific version of the table to check. Defaults to the latest active version.
        stream_format: Optional stream format to check. Defaults to DELTACAT.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        True if the table exists, False otherwise.
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    exists_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = exists_transaction

    try:
        table_obj = _get_storage(**kwargs).get_table(
            namespace=namespace,
            table_name=table,
            *args,
            **kwargs,
        )
        if table_obj is None:
            return False
        table_version = table_version or table_obj.latest_active_table_version
        if not table_version:
            return False
        table_version_exists = _get_storage(**kwargs).table_version_exists(
            namespace,
            table,
            table_version,
            *args,
            **kwargs,
        )
        if not table_version_exists:
            return False
        stream_exists = _get_storage(**kwargs).stream_exists(
            namespace=namespace,
            table_name=table,
            table_version=table_version,
            stream_format=stream_format,
            *args,
            **kwargs,
        )
        return stream_exists
    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during table_exists: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            exists_transaction.seal()


def list_namespaces(
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> ListResult[Namespace]:
    """List a page of table namespaces.

    Args:
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        ListResult containing Namespace objects.
    """
    # Set up transaction handling
    list_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = list_transaction

    try:
        result = _get_storage(**kwargs).list_namespaces(*args, **kwargs)

        return result

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during list_namespaces: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            list_transaction.seal()


def get_namespace(
    namespace: str,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Optional[Namespace]:
    """Get metadata for a specific table namespace.

    Args:
        namespace: Name of the namespace to retrieve.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        Namespace object if the namespace exists, None otherwise.
    """
    # Set up transaction handling
    get_ns_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = get_ns_transaction

    try:
        result = _get_storage(**kwargs).get_namespace(
            *args, namespace=namespace, **kwargs
        )

        return result

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during get_namespace: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            get_ns_transaction.seal()


def namespace_exists(
    namespace: str,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> bool:
    """Check if a namespace exists.

    Args:
        namespace: Name of the namespace to check.
        transaction: Optional transaction to use for reading. If provided, will see uncommitted changes.

    Returns:
        True if the namespace exists, False otherwise.
    """
    # Set up transaction handling
    exists_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = exists_transaction

    try:
        result = _get_storage(**kwargs).namespace_exists(
            *args, namespace=namespace, **kwargs
        )

        return result

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during namespace_exists: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            exists_transaction.seal()


def create_namespace(
    namespace: str,
    *args,
    properties: Optional[NamespaceProperties] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Namespace:
    """Create a new namespace.

    Args:
        namespace: Name of the namespace to create.
        properties: Optional properties for the namespace.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        Created Namespace object.

    Raises:
        NamespaceAlreadyExistsError: If the namespace already exists.
    """
    # Set up transaction handling
    namespace_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = namespace_transaction

    try:
        if namespace_exists(namespace, **kwargs):
            raise NamespaceAlreadyExistsError(f"Namespace {namespace} already exists")

        result = _get_storage(**kwargs).create_namespace(
            *args, namespace=namespace, properties=properties, **kwargs
        )

        return result

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during create_namespace: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            namespace_transaction.seal()


def alter_namespace(
    namespace: str,
    *args,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """Alter a namespace definition.

    Args:
        namespace: Name of the namespace to alter.
        properties: Optional new properties for the namespace.
        new_namespace: Optional new name for the namespace.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None
    """
    # Set up transaction handling
    alter_ns_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = alter_ns_transaction

    try:
        _get_storage(**kwargs).update_namespace(
            namespace=namespace,
            properties=properties,
            new_namespace=new_namespace,
            *args,
            **kwargs,
        )

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during alter_namespace: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            alter_ns_transaction.seal()


def drop_namespace(
    namespace: str,
    *args,
    purge: bool = False,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> None:
    """Drop a namespace and all of its tables from the catalog.

    Args:
        namespace: Name of the namespace to drop.
        purge: If True, permanently delete all table data in the namespace.
            If False, only removes the namespace from the catalog.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None

    TODO: Honor purge once garbage collection is implemented.
    """
    if purge:
        raise NotImplementedError("Purge flag is not currently supported.")

    # Set up transaction handling
    drop_ns_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = drop_ns_transaction

    try:
        _get_storage(**kwargs).delete_namespace(
            *args,
            namespace=namespace,
            purge=purge,
            **kwargs,
        )

    except Exception as e:
        # If any error occurs, the transaction remains uncommitted
        commit_transaction = False
        logger.error(f"Error during drop_namespace: {e}")
        raise
    finally:
        if commit_transaction:
            # Seal the interactive transaction to commit all operations atomically
            drop_ns_transaction.seal()


def default_namespace(*args, **kwargs) -> str:
    """Return the default namespace for the catalog.

    Returns:
        Name of the default namespace.
    """
    return DEFAULT_NAMESPACE


def _get_latest_active_or_given_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> TableVersion:
    table_version_obj = None
    if table_version is None:
        table_version_obj = _get_storage(**kwargs).get_latest_active_table_version(
            namespace=namespace,
            table_name=table_name,
            *args,
            **kwargs,
        )
        if table_version_obj is None:
            raise TableVersionNotFoundError(
                f"No active table version found for table {namespace}.{table_name}"
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


def _get_all_committed_partitions(
    table: str,
    namespace: str,
    table_version: str,
    **kwargs,
) -> List[Union[Partition, PartitionLocator]]:
    """Get all committed partitions for a table and validate uniqueness."""
    logger.info(
        f"Reading all partitions metadata in the table={table} "
        "as partition_filter was None."
    )

    all_partitions = (
        _get_storage(**kwargs)
        .list_partitions(
            table_name=table,
            namespace=namespace,
            table_version=table_version,
            **kwargs,
        )
        .all_items()
    )

    committed_partitions = [
        partition
        for partition in all_partitions
        if partition.state == CommitState.COMMITTED
    ]

    logger.info(
        f"Found {len(committed_partitions)} committed partitions for "
        f"table={namespace}/{table}/{table_version}"
    )

    _validate_partition_uniqueness(
        committed_partitions, namespace, table, table_version
    )
    return committed_partitions


def _validate_partition_uniqueness(
    partitions: List[Partition], namespace: str, table: str, table_version: str
) -> None:
    """Validate that there are no duplicate committed partitions for the same partition values."""
    commit_count_per_partition_value = defaultdict(int)
    for partition in partitions:
        # Normalize partition values: both None and [] represent unpartitioned data
        normalized_values = (
            None
            if (
                partition.partition_values is None
                or (
                    isinstance(partition.partition_values, list)
                    and len(partition.partition_values) == 0
                )
            )
            else partition.partition_values
        )
        commit_count_per_partition_value[normalized_values] += 1

    # Check for multiple committed partitions for the same partition values
    for partition_values, commit_count in commit_count_per_partition_value.items():
        if commit_count > 1:
            raise RuntimeError(
                f"Multiple committed partitions found for table={namespace}/{table}/{table_version}. "
                f"Partition values: {partition_values}. Commit count: {commit_count}. "
                f"This should not happen."
            )


def _get_deltas_from_partition_filter(
    partition_filter: Optional[List[Union[Partition, PartitionLocator]]] = None,
    *args,
    **kwargs,
):
    result_deltas = []
    for partition_like in partition_filter:
        deltas = (
            _get_storage(**kwargs)
            .list_partition_deltas(
                partition_like=partition_like,
                ascending_order=True,
                include_manifest=True,
                *args,
                **kwargs,
            )
            .all_items()
        )

        # Validate that all qualified deltas are append type - merge-on-read not yet implemented
        # TODO(pdames): Run compaction minus materialize for MoR of each partition.
        if deltas:
            non_append_deltas = []
            for delta in deltas:
                if delta.type != DeltaType.APPEND:
                    non_append_deltas.append(delta)
                else:
                    result_deltas.append(delta)
            if non_append_deltas:
                delta_types = {delta.type for delta in non_append_deltas}
                delta_info = [
                    (str(delta.locator), delta.type) for delta in non_append_deltas[:5]
                ]  # Show first 5
                raise NotImplementedError(
                    f"Merge-on-read is not yet implemented. Found {len(non_append_deltas)} non-append deltas "
                    f"with types {delta_types}. All deltas must be APPEND type for read operations. "
                    f"Examples: {delta_info}. Please run compaction first to merge non-append deltas."
                )

            logger.info(f"Validated {len(deltas)} qualified deltas are all APPEND type")
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
