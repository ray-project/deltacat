from typing import Any, Dict, List, Optional, Union, Tuple, Set
import logging
from collections import defaultdict

import numpy as np
import pyarrow as pa
import pandas as pd
import daft
import deltacat as dc

from deltacat.storage.model.manifest import ManifestAuthor
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.exceptions import (
    NamespaceAlreadyExistsError,
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
    SchemaUpdate,
)
from deltacat.storage.model.table import TableProperties, Table
from deltacat.storage.model.types import (
    CommitState,
    Dataset,
    DeltaType,
    LifecycleState,
    StreamFormat,
    SchemaConsistencyType,
)
from deltacat.storage.model.delta import (
    Delta,
    MAX_DELTA_STREAM_POSITION,
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
from deltacat.storage.model.transaction import (
    Transaction,
    setup_transaction,
)
from deltacat.types.media import (
    ContentType,
    DatasetType,
    StorageType,
    SCHEMA_CONTENT_TYPES,
)
from deltacat.types.tables import (
    SchemaEvolutionMode,
    TableProperty,
    TablePropertyDefaultValues,
    TableReadOptimizationLevel,
    TableWriteMode,
    get_dataset_type,
    get_table_schema,
    get_table_column_names,
    from_manifest_table as from_manifest_table_util,
    from_pyarrow,
    concat_tables,
    empty_table,
    infer_table_schema,
    to_pandas,
)
from deltacat.utils import pyarrow as pa_utils
from deltacat.utils.reader_compatibility_mapping import get_compatible_readers
from deltacat.utils.pyarrow import get_base_arrow_type_name
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


def _get_table_and_validate_write_mode(
    table: str,
    namespace: str,
    table_version: Optional[str],
    mode: TableWriteMode,
    **kwargs,
) -> Tuple[bool, TableDefinition]:
    """Validate write mode against table and table version existence.

    Returns:
        Tuple of (table_exists_flag, table_definition)
    """
    # First validate table, table version, and stream existence
    existing_table_def = get_table(
        table,
        namespace=namespace,
        table_version=table_version,
        **kwargs,
    )
    table_exists_flag = (
        existing_table_def is not None
        and existing_table_def.table_version
        and existing_table_def.stream
    )
    logger.info(f"Table to write to ({namespace}.{table}) exists: {table_exists_flag}")

    # Then validate table existence constraints
    if mode == TableWriteMode.CREATE and table_exists_flag and table_version is None:
        raise TableAlreadyExistsError(
            f"Table {namespace}.{table} already exists and mode is CREATE."
        )
    elif (
        mode not in (TableWriteMode.CREATE, TableWriteMode.AUTO)
        and existing_table_def is None
    ):
        raise TableNotFoundError(
            f"Table {namespace}.{table} does not exist and write mode is {mode}. Use CREATE or AUTO mode to create a new table."
        )

    # Then validate table version existence constraints
    if table_version is not None and table_exists_flag:
        if mode == TableWriteMode.CREATE:
            raise TableVersionAlreadyExistsError(
                f"Table version {namespace}.{table}.{table_version} already exists and mode is CREATE."
            )
        logger.info(f"Table version ({namespace}.{table}.{table_version}) exists.")
    elif (
        mode not in (TableWriteMode.CREATE, TableWriteMode.AUTO)
        and table_version is not None
        and not table_exists_flag
    ):
        raise TableVersionNotFoundError(
            f"Table version {namespace}.{table}.{table_version} does not exist and write mode is {mode}. "
            f"Use CREATE or AUTO mode to create a new table version, or omit table_version "
            f"to use the latest version."
        )
    return table_exists_flag, existing_table_def


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


def _create_table_for_write(
    data: Dataset,
    table: str,
    namespace: str,
    table_version: Optional[str],
    content_type: ContentType,
    existing_table_definition: Optional[TableDefinition],
    *args,
    **kwargs,
) -> TableDefinition:
    """Creates a new table, table version, and/or stream in preparation for a write operation."""
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
        table_version=table_version,
        existing_table_definition=existing_table_definition,
        *args,
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
) -> List[Delta]:
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
        transaction: Optional transaction to append write operations to instead of
            creating and committing a new transaction.
        **kwargs: Additional keyword arguments.

    Returns:
        List of deltas written to the table (typically one delta per touched partition).
    """
    namespace = namespace or default_namespace()

    # Set up transaction handling
    write_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = write_transaction

    try:
        # Validate write mode and table/table version/stream existence
        (table_exists_flag, table_definition,) = _get_table_and_validate_write_mode(
            table,
            namespace,
            table_version,
            mode,
            **kwargs,
        )

        # Get or create table, table version, and/or stream
        if not table_exists_flag:
            table_definition = _create_table_for_write(
                data,
                table,
                namespace,
                table_version,
                content_type,
                table_definition,
                *args,
                **kwargs,
            )
        else:
            # call alter_table if there are any alter_table kwargs provided
            if (
                "lifecycle_state" in kwargs
                or "schema_updates" in kwargs
                or "partition_updates" in kwargs
                or "sort_scheme" in kwargs
                or "table_description" in kwargs
                or "table_version_description" in kwargs
                or "table_properties" in kwargs
                or "table_version_properties" in kwargs
            ):
                alter_table(
                    table,
                    namespace=namespace,
                    table_version=table_version,
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

        # Validate schema compatibility for schemaless content types with schema tables
        if (
            content_type.value not in SCHEMA_CONTENT_TYPES
            and table_version_obj.schema is not None
        ):
            schemaless_types = {
                ct for ct in ContentType if ct.value not in SCHEMA_CONTENT_TYPES
            }
            raise TableValidationError(
                f"Content type '{content_type.value}' cannot be written to a table with a schema. "
                f"Table '{namespace}.{table}' has a schema, but content type '{content_type.value}' "
                f"is schemaless. Schemaless content types ({', '.join(sorted([ct.value for ct in schemaless_types]))}) "
                f"can only be written to schemaless tables."
            )

        # Handle different write modes and get stream and delta type
        stream, delta_type = _handle_write_mode(
            mode,
            table_definition,
            table_version_obj,
            namespace,
            table,
            table_exists_flag,
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

        # Convert unsupported dataset types and NumPy arrays that need schema validation
        if isinstance(data, np.ndarray) and table_version_obj.schema is not None:
            # NumPy arrays need conversion to Pandas for proper column naming in schema validation
            converted_data = _convert_numpy_for_schema_validation(
                data, table_version_obj.schema
            )
        else:
            # Convert other unsupported dataset types (e.g., Daft) or keep NumPy as-is for schemaless tables
            converted_data = _convert_data_if_needed(data)

        # Capture original field set before schema coercion for partial UPSERT support
        original_fields = set(get_table_column_names(converted_data))

        # Validate and coerce data against schema
        # This ensures proper schema evolution and type handling
        (
            validated_data,
            schema_modified,
            updated_schema,
        ) = _validate_and_coerce_data_against_schema(
            converted_data,  # Use converted data for NumPy, original for others
            table_version_obj.schema,
            schema_evolution_mode=schema_evolution_mode,
            default_schema_consistency_type=default_schema_consistency_type,
        )

        # Convert validated data to supported format for storage if needed
        converted_data = _convert_data_if_needed(validated_data)

        # Validate reader compatibility against supported reader types
        supported_reader_types = table_version_obj.read_table_property(
            TableProperty.SUPPORTED_READER_TYPES
        )
        _validate_reader_compatibility(
            converted_data,
            content_type,
            supported_reader_types,
        )

        # Update table version if schema was modified during evolution
        if schema_modified:
            # Extract catalog properties and filter kwargs
            catalog_kwargs = {
                "catalog": kwargs.get("catalog"),
                "inner": kwargs.get("inner"),
                "transaction": write_transaction,  # Pass transaction to update_table_version
            }

            _, updated_table_version_obj, _ = _get_storage(
                **catalog_kwargs
            ).update_table_version(
                namespace=namespace,
                table_name=table,
                table_version=table_version_obj.table_version,
                schema=updated_schema,
                **catalog_kwargs,
            )

        # Stage and commit delta, handle compaction
        # Remove schema from kwargs to avoid duplicate parameter conflict
        filtered_kwargs = {k: v for k, v in kwargs.items() if k != "schema"}
        # Use updated schema if schema evolution occurred, otherwise use original schema
        deltas = _stage_commit_and_compact(
            converted_data,
            partition,
            delta_type,
            content_type,
            commit_staged_partition,
            table_version_obj,
            updated_table_version_obj if schema_modified else None,
            namespace,
            table,
            original_fields=original_fields,
            **filtered_kwargs,
        )
        return deltas
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
    table_exists_flag: bool,
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
    elif mode == TableWriteMode.ADD:
        return _handle_add_mode(
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
            table_exists_flag,
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


def _handle_add_mode(
    table_schema,
    namespace: str,
    table: str,
    table_version_obj: TableVersion,
    **kwargs,
) -> Tuple[Any, DeltaType]:
    """Handle ADD mode by validating no merge keys and getting existing stream."""
    if table_schema and table_schema.merge_keys:
        raise SchemaValidationError(
            f"ADD mode cannot be used with tables that have merge keys. "
            f"Table {namespace}.{table} has merge keys: {table_schema.merge_keys}. "
            f"Use MERGE mode instead."
        )

    stream = _get_table_stream(
        namespace,
        table,
        table_version_obj.table_version,
        **kwargs,
    )
    return stream, DeltaType.ADD


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
    table_exists_flag: bool,
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
        # Tables are always created with an ordered APPEND delta, then default to unordered ADD deltas
        else DeltaType.ADD
        if table_exists_flag
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
        # APPEND/ADD mode on existing table: Get existing partition
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


def _convert_numpy_for_schema_validation(
    data: np.ndarray, schema: Optional[Schema]
) -> Dataset:
    """Convert NumPy array to Pandas DataFrame with proper column names for schema validation.

    Args:
        data: NumPy array to convert
        schema: DeltaCAT Schema object for column naming

    Returns:
        Pandas DataFrame with proper column names matching schema

    Raises:
        ValueError: If array has more columns than schema or schema is invalid
    """
    if not isinstance(schema, Schema) or not schema.arrow:
        raise ValueError(
            f"Expected DeltaCAT schema for Numpy schema validation, but found: {schema}"
        )

    # Use schema subset matching NumPy array dimensions
    arrow_schema = schema.arrow
    num_cols = data.shape[1] if data.ndim > 1 else 1

    if len(arrow_schema) >= num_cols:
        # Use the first N columns from the schema to match data dimensions
        subset_fields = [arrow_schema.field(i) for i in range(num_cols)]
        subset_schema = pa.schema(subset_fields)
        return to_pandas(data, schema=subset_schema)
    else:
        raise ValueError(
            f"NumPy array has {num_cols} columns but table schema only has {len(arrow_schema)} columns. "
            f"Cannot write NumPy data with more columns than the table schema supports."
        )


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


def _validate_reader_compatibility(
    data: Dataset,
    content_type: ContentType,
    supported_reader_types: Optional[List[DatasetType]],
) -> None:
    """Validate that the data types being written are compatible with all supported reader types.

    Args:
        data: The dataset to validate
        content_type: Content type being written
        supported_reader_types: List of DatasetTypes that must be able to read this data

    Raises:
        TableValidationError: If any data types would break supported reader compatibility
    """
    if not supported_reader_types:
        return

    # Get the schema from the data
    schema = get_table_schema(data)

    # Get the dataset type of the current data
    writer_dataset_type = get_dataset_type(data)

    # PYARROW_PARQUET is equivalent to PYARROW for compatibility
    writer_type_str = (
        writer_dataset_type.value
        if writer_dataset_type != DatasetType.PYARROW_PARQUET
        else "pyarrow"
    )

    content_type_str = content_type.value

    # Check each field type for compatibility
    incompatible_fields = []

    for field in schema:
        field_name = field.name
        arrow_type_str = str(field.type)

        # Get the base type name from PyArrow field type
        base_type_name = get_base_arrow_type_name(field.type)

        # Get compatible readers for this (arrow_type, writer_dataset_type, content_type) combination
        compatible_readers = get_compatible_readers(
            base_type_name,
            writer_type_str,
            content_type_str,
        )

        # Check if all supported reader types are compatible
        for required_reader in supported_reader_types:
            reader_is_compatible = required_reader in compatible_readers

            # Special case: PYARROW_PARQUET is equivalent to PYARROW for compatibility if we're writing parquet
            if (
                not reader_is_compatible
                and content_type == ContentType.PARQUET
                and required_reader == DatasetType.PYARROW_PARQUET
            ):
                reader_is_compatible = DatasetType.PYARROW in compatible_readers

            if not reader_is_compatible:
                incompatible_fields.append(
                    {
                        "field_name": field_name,
                        "arrow_type": arrow_type_str,
                        "incompatible_reader": required_reader,
                        "writer_type": writer_dataset_type,
                        "content_type": content_type,
                    }
                )

    # Raise error if any incompatibilities found
    if incompatible_fields:
        error_details = []
        for incompatible in incompatible_fields:
            error_details.append(
                f"Field '{incompatible['field_name']}' with type '{incompatible['arrow_type']}' "
                f"written by {incompatible['writer_type']} to {incompatible['content_type']} "
                f"cannot be read by required reader type {incompatible['incompatible_reader']}. "
                f"If you expect this write to succeed and this reader is not required, then it "
                f"can be removed from the table's supported reader types property."
            )

        raise TableValidationError(
            f"Reader compatibility validation failed. The following fields would break "
            f"supported reader types:\n" + "\n".join(error_details)
        )


def _stage_commit_and_compact(
    converted_data: Dataset,
    partition,
    delta_type: DeltaType,
    content_type: ContentType,
    commit_staged_partition: bool,
    original_table_version_obj: TableVersion,
    updated_table_version_obj: Optional[TableVersion],
    namespace: str,
    table: str,
    original_fields: Set[str],
    **kwargs,
) -> List[Delta]:
    """Stage and commit delta, then handle compaction if needed."""
    # Remove schema from kwargs to avoid duplicate parameter conflict
    # We explicitly pass the correct schema parameter
    kwargs.pop("schema", None)

    resolved_table_version_obj = (
        updated_table_version_obj
        if updated_table_version_obj
        else original_table_version_obj
    )

    # Stage a delta with the data
    delta: Delta = _get_storage(**kwargs).stage_delta(
        data=converted_data,
        partition=partition,
        delta_type=delta_type,
        content_type=content_type,
        author=ManifestAuthor.of(
            name="deltacat.write_to_table", version=dc.__version__
        ),
        schema=resolved_table_version_obj.schema,
        **kwargs,
    )

    delta: Delta = _get_storage(**kwargs).commit_delta(delta=delta, **kwargs)

    if commit_staged_partition:
        _get_storage(**kwargs).commit_partition(partition=partition, **kwargs)

    # Check compaction trigger decision
    should_compact = _trigger_compaction(
        resolved_table_version_obj,
        delta,
        TableReadOptimizationLevel.MAX,
        **kwargs,
    )
    if should_compact:
        # Run V2 compaction session to merge or delete data
        if not original_table_version_obj.schema:
            raise RuntimeError("Table version schema is required to run compaction.")
        original_table_version_column_names = (
            original_table_version_obj.schema.arrow.names
        )
        _run_compaction_session(
            table_version_obj=resolved_table_version_obj,
            partition=partition,
            latest_delta_stream_position=MAX_DELTA_STREAM_POSITION,
            namespace=namespace,
            table=table,
            original_fields=original_fields,
            original_table_version_column_names=original_table_version_column_names,
            **kwargs,
        )
    return [delta]


def _trigger_compaction(
    table_version_obj: TableVersion,
    latest_delta: Optional[Delta],
    target_read_optimization_level: TableReadOptimizationLevel,
    **kwargs,
) -> bool:
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
        elif delta_type in (DeltaType.APPEND, DeltaType.ADD):
            # Get all deltas appended since last compaction
            deltas = (
                _get_storage(**kwargs)
                .list_deltas(
                    namespace=table_version_obj.locator.namespace,
                    table_name=table_version_obj.locator.table_name,
                    table_version=table_version_obj.locator.table_version,
                    partition_values=partition_values,
                    start_stream_position=2,  # the last compacted delta will always have stream position 1
                    **kwargs,
                )
                .all_items()
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
        else:
            raise RuntimeError(
                f"Unexpected delta type for compaction trigger: {delta_type}"
            )
    return False


def _get_compaction_primary_keys(table_version_obj: TableVersion) -> set:
    """Extract primary keys from table schema for compaction."""
    table_schema = table_version_obj.schema
    return (
        set(table_schema.merge_keys)
        if table_schema and table_schema.merge_keys
        else set()
    )


def _get_compaction_hash_bucket_count(
    partition: Partition, table_version_obj: TableVersion
) -> int:
    """Determine hash bucket count from previous compaction, table property, or default."""
    # First check if we have a hash bucket count from previous compaction
    if (
        partition.compaction_round_completion_info
        and partition.compaction_round_completion_info.hash_bucket_count
    ):
        hash_bucket_count = partition.compaction_round_completion_info.hash_bucket_count
        logger.info(
            f"Using hash bucket count {hash_bucket_count} from previous compaction"
        )
        return hash_bucket_count

    # Otherwise use the table property for default compaction hash bucket count
    hash_bucket_count = table_version_obj.read_table_property(
        TableProperty.DEFAULT_COMPACTION_HASH_BUCKET_COUNT
    )
    logger.info(f"Using hash bucket count {hash_bucket_count} from table property")
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
    original_fields: Set[str],
    all_column_names: Optional[List[str]],
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
    kwargs.pop("sort_scheme", None)

    table_writer_kwargs = kwargs.pop("table_writer_kwargs", {})
    table_writer_kwargs["schema"] = table_version_obj.schema
    table_writer_kwargs["sort_scheme_id"] = table_version_obj.sort_scheme.id
    deltacat_storage_kwargs = kwargs.pop("deltacat_storage_kwargs", {})
    deltacat_storage_kwargs["transaction"] = kwargs.get("transaction", None)
    list_deltas_kwargs = kwargs.pop("list_deltas_kwargs", {})
    list_deltas_kwargs["transaction"] = kwargs.get("transaction", None)

    return CompactPartitionParams.of(
        {
            "catalog": kwargs.get("inner", kwargs.get("catalog")),
            "source_partition_locator": partition.locator,
            "destination_partition_locator": partition.locator,  # In-place compaction
            "primary_keys": primary_keys,
            "last_stream_position_to_compact": latest_stream_position,
            "deltacat_storage": _get_storage(**kwargs),
            "deltacat_storage_kwargs": deltacat_storage_kwargs,
            "list_deltas_kwargs": list_deltas_kwargs,
            "table_writer_kwargs": table_writer_kwargs,
            "hash_bucket_count": hash_bucket_count,
            "records_per_compacted_file": table_version_obj.read_table_property(
                TableProperty.RECORDS_PER_COMPACTED_FILE,
            ),
            "compacted_file_content_type": ContentType.PARQUET,
            "drop_duplicates": True,
            "sort_keys": _get_merge_order_sort_keys(table_version_obj),
            "original_fields": original_fields,
            "all_column_names": all_column_names,
        }
    )


def _run_compaction_session(
    table_version_obj: TableVersion,
    partition: Partition,
    latest_delta_stream_position: int,
    namespace: str,
    table: str,
    original_fields: Set[str],
    original_table_version_column_names: List[str],
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
        original_fields: The original field set for partial UPSERT support
        **kwargs: Additional arguments including catalog and storage parameters
    """
    # Import inside function to avoid circular imports
    from deltacat.compute.compactor_v2.compaction_session import compact_partition

    try:
        # Extract compaction configuration
        primary_keys = _get_compaction_primary_keys(table_version_obj)
        hash_bucket_count = _get_compaction_hash_bucket_count(
            partition,
            table_version_obj,
        )

        # Create compaction parameters
        compact_partition_params = _create_compaction_params(
            table_version_obj,
            partition,
            latest_delta_stream_position,
            primary_keys,
            hash_bucket_count,
            original_fields=original_fields,
            all_column_names=original_table_version_column_names,
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


def _handle_schemaless_table_read(
    qualified_deltas: List[Delta],
    read_as: DatasetType,
    **kwargs,
) -> Dataset:
    """Handle reading schemaless tables by flattening manifest entries."""
    # Create a PyArrow table for each delta
    # TODO(pdames): More efficient implementation for tables with millions/billions of entries
    tables = []
    for delta in qualified_deltas:
        # Get the manifest for this delta
        if delta.manifest:
            manifest = delta.manifest
        else:
            # Fetch manifest from storage
            manifest = _get_storage(**kwargs).get_delta_manifest(
                delta.locator,
                transaction=kwargs.get("transaction"),
                **kwargs,
            )
        # Create flattened table from this delta's manifest
        table = pa_utils.delta_manifest_to_table(
            manifest,
            delta,
        )
        tables.append(table)

    # Concatenate all PyArrow tables
    final_table = pa_utils.concat_tables(tables)

    # Convert from PyArrow to the requested dataset type
    return from_pyarrow(final_table, read_as)


def _download_and_process_table_data(
    namespace: str,
    table: str,
    qualified_deltas: List[Delta],
    read_as: DatasetType,
    max_parallelism: Optional[int],
    columns: Optional[List[str]],
    file_path_column: Optional[str],
    table_version_obj: Optional[TableVersion],
    **kwargs,
) -> Dataset:
    """Download delta data and process result based on storage type."""

    # Handle NUMPY read requests by translating to PANDAS internally
    original_read_as = read_as
    effective_read_as = read_as
    if read_as == DatasetType.NUMPY:
        effective_read_as = DatasetType.PANDAS
        logger.debug("Translating NUMPY read request to PANDAS for internal processing")

    # Merge deltas and download data
    if not qualified_deltas:
        # Return empty table with original read_as type
        return empty_table(original_read_as)

    # Special handling for non-empty schemaless tables
    if table_version_obj.schema is None:
        result = _handle_schemaless_table_read(
            qualified_deltas,
            effective_read_as,
            **kwargs,
        )
        # Convert to numpy if original request was for numpy
        if original_read_as == DatasetType.NUMPY:
            return _convert_pandas_to_numpy(result)
        return result

    # Standard non-empty schema table read path - merge deltas and download data
    merged_delta = Delta.merge_deltas(qualified_deltas)

    # Convert read parameters to download parameters
    table_type = (
        effective_read_as
        if effective_read_as in DatasetType.local()
        else (kwargs.pop("table_type", None) or DatasetType.PYARROW)
    )
    distributed_dataset_type = (
        effective_read_as if effective_read_as in DatasetType.distributed() else None
    )

    # Validate input parameters
    _validate_read_table_input(
        namespace,
        table,
        table_version_obj.schema,
        table_type,
        distributed_dataset_type,
    )

    # Determine max parallelism
    max_parallelism = _get_max_parallelism(
        max_parallelism,
        distributed_dataset_type,
    )
    # Filter out parameters that are already passed as keyword arguments
    # to avoid "multiple values for argument" errors
    filtered_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k
        not in [
            "delta_like",
            "table_type",
            "storage_type",
            "max_parallelism",
            "columns",
            "distributed_dataset_type",
            "file_path_column",
        ]
    }
    result = _get_storage(**kwargs).download_delta(
        merged_delta,
        table_type=effective_read_as,
        storage_type=StorageType.DISTRIBUTED
        if distributed_dataset_type
        else StorageType.LOCAL,
        max_parallelism=max_parallelism,
        columns=columns,
        distributed_dataset_type=distributed_dataset_type,
        file_path_column=file_path_column,
        **filtered_kwargs,
    )

    # Handle local storage table concatenation and PYARROW_PARQUET lazy materialization
    if not distributed_dataset_type and table_type and isinstance(result, list):
        if table_type == DatasetType.PYARROW_PARQUET:
            # For PYARROW_PARQUET, preserve lazy materialization:
            return result[0] if len(result) == 1 else result
        else:
            # For other types, perform normal concatenation
            result = _handle_local_table_concatenation(
                result,
                table_type,
                table_version_obj.schema,
                file_path_column,
                columns,
            )
    # Convert pandas to numpy if original request was for numpy
    if original_read_as == DatasetType.NUMPY:
        return _convert_pandas_to_numpy(result)

    return result


def _convert_pandas_to_numpy(dataset: Dataset):
    """Convert pandas DataFrame to numpy ndarray."""
    if not isinstance(dataset, pd.DataFrame):
        raise ValueError(f"Expected pandas DataFrame but found {type(dataset)}")
    return dataset.to_numpy()


def _coerce_dataset_to_schema(
    dataset: Dataset,
    target_schema: pa.Schema,
) -> Dataset:
    """Coerce a dataset to match the target PyArrow schema using DeltaCAT Schema.coerce method."""
    # Convert target PyArrow schema to DeltaCAT schema and use its coerce method
    deltacat_schema = Schema.of(schema=target_schema)
    return deltacat_schema.coerce(dataset)


def _coerce_results_to_schema(
    results: Dataset,
    target_schema: pa.Schema,
) -> List[Dataset]:
    """Coerce all table results to match the target schema."""
    coerced_results = []
    for i, table_result in enumerate(results):
        coerced_result = _coerce_dataset_to_schema(
            table_result,
            target_schema,
        )
        coerced_results.append(coerced_result)
        logger.debug(f"Coerced table {i} to unified schema")
    return coerced_results


def _create_target_schema(
    arrow_schema: pa.Schema,
    columns: Optional[List[str]] = None,
    file_path_column: Optional[str] = None,
) -> pa.Schema:
    """Create target schema for concatenation with optional column selection and file_path_column."""
    if columns is not None:
        # Column selection - use only specified columns
        field_map = {field.name: field for field in arrow_schema}
        selected_fields = []

        for col_name in columns:
            if col_name in field_map:
                selected_fields.append(field_map[col_name])
        arrow_schema = pa.schema(selected_fields)
    if file_path_column and file_path_column not in arrow_schema.names:
        arrow_schema = arrow_schema.append(pa.field(file_path_column, pa.string()))
    return arrow_schema


def _handle_local_table_concatenation(
    results: Dataset,
    table_type: DatasetType,
    table_schema: Optional[Schema],
    file_path_column: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> Dataset:
    """Handle concatenation of local table results with schema coercion."""
    logger.debug(f"Target table schema for concatenation: {table_schema}")

    # Create target schema for coercion, respecting column selection
    target_schema = _create_target_schema(table_schema.arrow, columns, file_path_column)
    logger.debug(f"Created target schema: {target_schema.names}")

    # Coerce results to unified schema
    coerced_results = _coerce_results_to_schema(
        results,
        target_schema,
    )

    # Second step: concatenate the coerced results
    logger.debug(
        f"Concatenating {len(coerced_results)} local tables of type {table_type} with unified schemas"
    )
    concatenated_result = concat_tables(coerced_results, table_type)
    logger.debug(f"Concatenation complete, result type: {type(concatenated_result)}")
    return concatenated_result


def read_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    read_as: DatasetType = DatasetType.DAFT,
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
            table_version_obj,
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
    schema_updates: Optional[SchemaUpdate] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    sort_scheme: Optional[SortScheme] = None,
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
        schema_updates: Schema updates to apply.
        partition_updates: Partition scheme updates to apply.
        sort_scheme: New sort scheme.
        table_description: New description for the table.
        table_version_description: New description for the table version. Defaults to `table_description` if not  specified.
        table_properties: New table properties.
        table_version_properties: New table version properties. Defaults to the current parent table properties if not specified.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not already exist.
        TableVersionNotFoundError: If the specified table version or active table version does not exist.
    """
    resolved_table_properties = None
    if table_properties is not None:
        resolved_table_properties = _add_default_table_properties(table_properties)
        _validate_table_properties(resolved_table_properties)

    namespace = namespace or default_namespace()

    # Set up transaction handling
    alter_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = alter_transaction

    try:
        if partition_updates:
            raise NotImplementedError("Partition updates are not yet supported.")
        if sort_scheme:
            raise NotImplementedError("Sort scheme updates are not yet supported.")

        new_table: Table = _get_storage(**kwargs).update_table(
            *args,
            namespace=namespace,
            table_name=table,
            description=table_description,
            properties=resolved_table_properties,
            **kwargs,
        )

        if table_version is None:
            table_version: Optional[TableVersion] = _get_storage(
                **kwargs
            ).get_latest_active_table_version(namespace, table, **kwargs)
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

        # Get table properties for schema evolution
        schema_evolution_mode = table_version.read_table_property(
            TableProperty.SCHEMA_EVOLUTION_MODE
        )
        if schema_updates and schema_evolution_mode == SchemaEvolutionMode.DISABLED:
            raise TableValidationError(
                "Schema evolution is disabled for this table. Please enable schema evolution or remove schema updates."
            )

        # Only update table version properties if they are explicitly provided
        resolved_tv_properties = None
        if table_version_properties is not None:
            # inherit properties from the parent table if not specified
            default_tv_properties = new_table.properties
            if table_version.schema is None:
                # schemaless tables don't validate reader compatibility by default
                default_tv_properties[TableProperty.SUPPORTED_READER_TYPES] = None
            resolved_tv_properties = _add_default_table_properties(
                table_version_properties,
                default_tv_properties,
            )
            _validate_table_properties(resolved_tv_properties)

        # Apply schema updates if provided
        updated_schema = None
        if schema_updates is not None:
            # Get the current schema from the table version
            current_schema = table_version.schema
            if current_schema != schema_updates.base_schema:
                raise ValueError(
                    f"Schema updates are not compatible with the current schema for table `{namespace}.{table}`. Current schema: {current_schema}, Schema update base schema: {schema_updates.base_schema}"
                )

            # Apply all the updates to get the final schema
            updated_schema = schema_updates.apply()

        _get_storage(**kwargs).update_table_version(
            *args,
            namespace=namespace,
            table_name=table,
            table_version=table_version.id,
            lifecycle_state=lifecycle_state,
            description=table_version_description or table_description,
            schema=updated_schema,
            properties=resolved_tv_properties,  # This will be None if table_version_properties was not provided
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


def _add_default_table_properties(
    table_properties: Optional[TableProperties],
    default_table_properties: TableProperties = TablePropertyDefaultValues,
) -> TableProperties:
    if table_properties is None:
        table_properties = {}
    for k, v in default_table_properties.items():
        if k not in table_properties:
            table_properties[k] = v
    return table_properties


def _validate_table_properties(
    table_properties: TableProperties,
) -> None:
    read_optimization_level = table_properties.get(
        TableProperty.READ_OPTIMIZATION_LEVEL,
        TablePropertyDefaultValues[TableProperty.READ_OPTIMIZATION_LEVEL],
    )
    if read_optimization_level != TableReadOptimizationLevel.MAX:
        raise NotImplementedError(
            f"Table read optimization level `{read_optimization_level} is not yet supported. Please use {TableReadOptimizationLevel.MAX}"
        )


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
    auto_create_namespace: bool = False,
    **kwargs,
) -> TableDefinition:
    """Create an empty table in the catalog.

    If a namespace isn't provided, the table will be created within the default deltacat namespace.
    The provided namespace will be created if it doesn't exist and auto_create_namespace is True.

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
        table_version_properties: Optional properties for the table version. Defaults to the current parent table properties if not specified.
        namespace_properties: Optional properties for the namespace if it needs to be created.
        content_types: Optional list of allowed content types for the table.
        fail_if_exists: If True, raises an error if table already exists. If False, returns existing table.
        auto_create_namespace: If True, creates the namespace if it doesn't exist. Defaults to False.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        TableDefinition object for the created or existing table.

    Raises:
        TableAlreadyExistsError: If the table already exists and fail_if_exists is True.
        NamespaceNotFoundError: If the provided namespace does not exist.
    """
    resolved_table_properties = _add_default_table_properties(table_properties)
    # Note: resolved_tv_properties will be set after checking existing table

    namespace = namespace or default_namespace()

    # Set up transaction handling
    create_transaction, commit_transaction = setup_transaction(transaction, **kwargs)
    kwargs["transaction"] = create_transaction

    try:
        existing_table = (
            get_table(
                table,
                namespace=namespace,
                table_version=table_version,
                *args,
                **kwargs,
            )
            if "existing_table_definition" not in kwargs
            else kwargs["existing_table_definition"]
        )
        if existing_table is not None:
            if existing_table.table_version and existing_table.stream:
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
            # the table exists but the table version doesn't - inherit the existing table properties
            # Also ensure table properties are inherited when not explicitly provided
            if table_properties is None:
                resolved_table_properties = existing_table.table.properties

            # Set up table version properties based on existing table or explicit properties
            default_tv_properties = resolved_table_properties
            if schema is None:
                default_tv_properties = dict(
                    default_tv_properties
                )  # Make a copy to avoid modifying original
                default_tv_properties[TableProperty.SUPPORTED_READER_TYPES] = None
            resolved_tv_properties = _add_default_table_properties(
                table_version_properties, default_tv_properties
            )
        else:
            # create the namespace if it doesn't exist
            if auto_create_namespace:
                try:
                    create_namespace(
                        namespace=namespace,
                        properties=namespace_properties,
                        *args,
                        **kwargs,
                    )
                except NamespaceAlreadyExistsError:
                    logger.info(f"Namespace {namespace} already exists.")

            # Set up table version properties for new table
            default_tv_properties = resolved_table_properties
            if schema is None:
                default_tv_properties = dict(
                    default_tv_properties
                )  # Make a copy to avoid modifying original
                default_tv_properties[TableProperty.SUPPORTED_READER_TYPES] = None
            resolved_tv_properties = _add_default_table_properties(
                table_version_properties, default_tv_properties
            )

        _validate_table_properties(resolved_tv_properties)

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
            table_properties=resolved_table_properties,
            table_version_properties=resolved_tv_properties,
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
        Deltacat TableDefinition if the table exists, None otherwise. The table definition's table version will be
        None if the requested version is not found. The table definition's stream will be None if the requested stream
        format is not found.
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

        table_version_obj: Optional[TableVersion] = _get_storage(
            **kwargs
        ).get_table_version(
            namespace,
            table,
            table_version or table_obj.latest_active_table_version,
            *args,
            **kwargs,
        )

        stream = None
        if table_version_obj:
            stream = _get_storage(**kwargs).get_stream(
                namespace=namespace,
                table_name=table,
                table_version=table_version_obj.id,
                stream_format=stream_format,
                *args,
                **kwargs,
            )

        return TableDefinition.of(
            table=table_obj,
            table_version=table_version_obj,
            stream=stream,
        )
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
                if delta.type not in (DeltaType.ADD, DeltaType.APPEND):
                    non_append_deltas.append(delta)
                else:
                    result_deltas.append(delta)
            if non_append_deltas:
                delta_types = {delta.type for delta in non_append_deltas}
                delta_info = [
                    (str(delta.locator), delta.type) for delta in non_append_deltas[:5]
                ]  # Show first 5
                raise NotImplementedError(
                    f"Merge-on-read is not yet implemented. Found {len(non_append_deltas)} non-ADD/APPEND deltas "
                    f"with types {delta_types}. All deltas must be ADD or APPEND type for read operations. "
                    f"Examples: {delta_info}. Please run compaction first to merge non-ADD/APPEND deltas."
                )

            logger.info(
                f"Validated {len(deltas)} qualified deltas are all ADD or APPEND type"
            )
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


def from_manifest_table(
    manifest_table: Dataset,
    *args,
    read_as: DatasetType = DatasetType.DAFT,
    schema: Optional[Schema] = None,
    **kwargs,
) -> Dataset:
    """
    Read a manifest table (containing file paths and metadata) and download the actual data.

    This utility function takes the output from a schemaless table read (which returns
    manifest entries instead of data) and downloads the actual file contents.

    Args:
        manifest_table: Dataset containing manifest entries with file paths and metadata
        read_as: The type of dataset to return (DAFT, RAY_DATASET, PYARROW, etc.)
        schema: Optional schema to attempt to coerce the data into.
        **kwargs: Additional arguments forwarded to download functions

    Returns:
        Dataset containing the actual file contents
    """
    return from_manifest_table_util(
        manifest_table,
        read_as=read_as,
        schema=schema.arrow if schema else None,
        *args,
        **kwargs,
    )
