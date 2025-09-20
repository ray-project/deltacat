from typing import Any, Dict, List, Optional, Union

from deltacat.storage.model.delta import Delta
from deltacat.storage.model.partition import (
    Partition,
    PartitionLocator,
    PartitionScheme,
)
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortScheme
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace, NamespaceProperties
from deltacat.storage.model.schema import (
    Schema,
    SchemaUpdateOperations,
)
from deltacat.storage.model.table import TableProperties
from deltacat.storage.model.table_version import TableVersionProperties
from deltacat.storage.model.types import (
    Dataset,
    LifecycleState,
    StreamFormat,
)
from deltacat.storage.model.transaction import Transaction
from deltacat.types.media import ContentType
from deltacat.types.tables import (
    DatasetType,
    TableWriteMode,
)


# catalog functions
def initialize(*args, **kwargs) -> Optional[Any]:
    """
    Initializes the data catalog with the given arguments.

    Will return an object containing any state needed for the operation of the catalog. For example,
    initializing an iceberg catalog will return the underlying native PyIceberg catalog.

    The return value initialize is stored in  :class:`deltacat.Catalog` as the "inner" property,
    and then passed to catalog function invocations as the kwarg "inner"
    """
    raise NotImplementedError("initialize not implemented")


# table functions
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
    raise NotImplementedError("write_to_table not implemented")


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
    raise NotImplementedError("read_table not implemented")


def alter_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[SchemaUpdateOperations] = None,
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
    raise NotImplementedError("alter_table not implemented")


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
    auto_create_namespace: bool = False,
    transaction: Optional[Transaction] = None,
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

    raise NotImplementedError("create_table not implemented")


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
        table_version: Optional specific version of the table to drop. Defaults to the latest active version.
        purge: If True, permanently delete the table data. If False, only remove from catalog.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not exist.
        TableVersionNotFoundError: If the table version does not exist.
    """
    raise NotImplementedError("drop_table not implemented")


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
        table_version: Optional specific version of the table to refresh. Defaults to the latest active version.
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
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        ListResult containing TableDefinition objects for tables in the namespace.
    """
    raise NotImplementedError("list_tables not implemented")


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
    raise NotImplementedError("get_table not implemented")


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
    raise NotImplementedError("rename_table not implemented")


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
    raise NotImplementedError("table_exists not implemented")


# namespace functions
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
    raise NotImplementedError("list_namespaces not implemented")


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
    raise NotImplementedError("get_namespace not implemented")


def namespace_exists(
    namespace: str,
    *args,
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> bool:
    """Check if a namespace exists.

    Args:
        namespace: Name of the namespace to check.
        transaction: Optional transaction to use. If None, creates a new transaction.

    Returns:
        True if the namespace exists, False otherwise.
    """
    raise NotImplementedError("namespace_exists not implemented")


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
    raise NotImplementedError("create_namespace not implemented")


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
    raise NotImplementedError("alter_namespace not implemented")


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
    """
    raise NotImplementedError("drop_namespace not implemented")


def default_namespace(*args, **kwargs) -> str:
    """Return the default namespace for the catalog.

    Returns:
        Name of the default namespace.
    """
    raise NotImplementedError("default_namespace not implemented")


def from_manifest_table(
    manifest_table: Dataset,
    *args,
    read_as: DatasetType = DatasetType.DAFT,
    schema: Optional[Schema] = None,
    transaction: Optional[Transaction] = None,
    catalog: Optional[str] = None,
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
    raise NotImplementedError("from_manifest_table not implemented")
