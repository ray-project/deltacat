from typing import Any, Dict, List, Optional, Union

from deltacat.storage.model.partition import PartitionScheme
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortScheme
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace, NamespaceProperties
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.table import TableProperties
from deltacat.storage.model.types import (
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    StreamFormat,
)
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode


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
    data: Union[LocalTable, LocalDataset, DistributedDataset],
    table: str,
    *args,
    namespace: Optional[str] = None,
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    **kwargs,
) -> None:
    """Write data to a DeltaCat table.

    Args:
        data: Data to write to the table. Can be a LocalTable, LocalDataset, or DistributedDataset.
        table: Name of the table to write to.
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        mode: Write mode to use when writing to the table.
        content_type: Content type of the data being written.

    Returns:
        None
    """
    raise NotImplementedError("write_to_table not implemented")


def read_table(
    table: str, *args, namespace: Optional[str] = None, **kwargs
) -> DistributedDataset:
    """Read data from a DeltaCat table.

    Args:
        table: Name of the table to read from.
        namespace: Optional namespace of the table. Uses default namespace if not specified.

    Returns:
        A Deltacat DistributedDataset containing the table data.
    """
    raise NotImplementedError("read_table not implemented")


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
    raise NotImplementedError("alter_table not implemented")


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
    raise NotImplementedError("create_table not implemented")


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
        table_version: Optional specific version of the table to drop.
        purge: If True, permanently delete the table data. If False, only remove from catalog.

    Returns:
        None

    Raises:
        TableNotFoundError: If the table does not exist.
    """
    raise NotImplementedError("drop_table not implemented")


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
    raise NotImplementedError("list_tables not implemented")


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
    raise NotImplementedError("get_table not implemented")


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
    raise NotImplementedError("rename_table not implemented")


def table_exists(table: str, *args, namespace: Optional[str] = None, **kwargs) -> bool:
    """Check if a table exists in the catalog.

    Args:
        table: Name of the table to check.
        namespace: Optional namespace of the table. Uses default namespace if not specified.

    Returns:
        True if the table exists, False otherwise.
    """
    raise NotImplementedError("table_exists not implemented")


# namespace functions
def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces.

    Args:
        catalog: Catalog properties instance.

    Returns:
        ListResult containing Namespace objects.
    """
    raise NotImplementedError("list_namespaces not implemented")


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Get metadata for a specific table namespace.

    Args:
        namespace: Name of the namespace to retrieve.

    Returns:
        Namespace object if the namespace exists, None otherwise.
    """
    raise NotImplementedError("get_namespace not implemented")


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Check if a namespace exists.

    Args:
        namespace: Name of the namespace to check.

    Returns:
        True if the namespace exists, False otherwise.
    """
    raise NotImplementedError("namespace_exists not implemented")


def create_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    *args,
    **kwargs,
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
    raise NotImplementedError("create_namespace not implemented")


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
    raise NotImplementedError("alter_namespace not implemented")


def drop_namespace(namespace: str, *args, purge: bool = False, **kwargs) -> None:
    """Drop a namespace and all of its tables from the catalog.

    Args:
        namespace: Name of the namespace to drop.
        purge: If True, permanently delete all tables in the namespace.
            If False, only remove from catalog.

    Returns:
        None
    """
    raise NotImplementedError("drop_namespace not implemented")


def default_namespace(*args, **kwargs) -> str:
    """Return the default namespace for the catalog.

    Returns:
        String name of the default namespace.
    """
    raise NotImplementedError("default_namespace not implemented")
