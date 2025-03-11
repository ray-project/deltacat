from typing import Any, Dict, List, Optional, Union

from deltacat.exceptions import (
    NamespaceAlreadyExistsError,
    StreamNotFoundError,
    TableAlreadyExistsError,
    TableVersionNotFoundError,
)
from deltacat.storage.model.partition import PartitionScheme
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortScheme
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace, NamespaceProperties
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.table import Table, TableProperties
from deltacat.storage.model.table_version import TableVersion
from deltacat.storage.model.types import (
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    StreamFormat,
)
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode
from deltacat.storage.main import impl as storage_impl
from deltacat.constants import (
    DEFAULT_NAMESPACE,
)


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
    raise NotImplementedError("Not implemented")


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
    raise NotImplementedError("Not implemented")


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

    storage_impl.update_table(
        *args,
        namespace=namespace,
        table_name=table,
        description=description,
        properties=properties,
        lifecycle_state=lifecycle_state,
        **kwargs,
    )

    table_version = storage_impl.get_latest_table_version(namespace, table, **kwargs)
    storage_impl.update_table_version(
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

    (table, table_version, stream) = storage_impl.create_table_version(
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
    storage_impl.delete_table(
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
    tables = storage_impl.list_tables(*args, namespace=namespace, **kwargs)
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
    table: Optional[Table] = storage_impl.get_table(
        *args, table_name=name, namespace=namespace, **kwargs
    )

    if table is None:
        return None

    table_version: Optional[TableVersion] = storage_impl.get_table_version(
        *args, namespace, name, table_version or table.latest_table_version, **kwargs
    )

    if table_version is None:
        raise TableVersionNotFoundError(
            f"TableVersion {namespace}.{name}.{table_version} does not exist."
        )

    stream = storage_impl.get_stream(
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
    storage_impl.update_table(
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
    return storage_impl.table_exists(
        *args, table_name=table, namespace=namespace, **kwargs
    )


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces.

    Args:
        catalog: Catalog properties instance.

    Returns:
        ListResult containing Namespace objects.
    """
    return storage_impl.list_namespaces(*args, **kwargs)


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Get metadata for a specific table namespace.

    Args:
        namespace: Name of the namespace to retrieve.

    Returns:
        Namespace object if the namespace exists, None otherwise.
    """
    return storage_impl.get_namespace(*args, namespace=namespace, **kwargs)


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Check if a namespace exists.

    Args:
        namespace: Name of the namespace to check.

    Returns:
        True if the namespace exists, False otherwise.
    """
    return storage_impl.namespace_exists(*args, namespace=namespace, **kwargs)


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
    if namespace_exists(namespace):
        raise NamespaceAlreadyExistsError(f"Namespace {namespace} already exists")

    return storage_impl.create_namespace(
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
    storage_impl.update_namespace(
        *args,
        namespace=namespace,
        properties=properties,
        new_namespace=new_namespace,
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

    storage_impl.delete_namespace(*args, namespace=namespace, purge=purge, **kwargs)


def default_namespace(*args, **kwargs) -> str:
    """Return the default namespace for the catalog.

    Returns:
        String name of the default namespace.
    """
    return DEFAULT_NAMESPACE  # table functions
