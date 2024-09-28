from typing import Any, Dict, List, Optional, Set, Union

from deltacat.catalog.model.catalog import get_catalog
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.partition import PartitionScheme
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
    SchemaConsistencyType,
)
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode


# table functions
def write_to_table(
    data: Union[LocalTable, LocalDataset, DistributedDataset],
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> None:
    """Write local or distributed data to a table. Raises an error if the
    table does not exist and the table write mode is not CREATE or AUTO.

    When creating a table, all `create_table` parameters may be optionally
    specified as additional keyword arguments. When appending to, or replacing,
    an existing table, all `alter_table` parameters may be optionally specified
    as additional keyword arguments."""
    catalog = get_catalog(catalog)
    catalog.impl.write_to_table(
        data,
        table,
        namespace,
        mode,
        content_type,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def read_table(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> DistributedDataset:
    """Read a table into a distributed dataset."""
    catalog = get_catalog(catalog)
    return catalog.impl.read_table(
        table,
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def alter_table(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    primary_keys: Optional[Set[str]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    *args,
    **kwargs,
) -> None:
    """Alter table definition."""
    catalog = get_catalog(catalog)
    catalog.impl.alter_table(
        table,
        namespace,
        lifecycle_state,
        schema_updates,
        partition_updates,
        primary_keys,
        sort_keys,
        description,
        properties,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def create_table(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Schema] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    primary_keys: Optional[Set[str]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    namespace_properties: Optional[NamespaceProperties] = None,
    content_types: Optional[List[ContentType]] = None,
    fail_if_exists: bool = True,
    *args,
    **kwargs,
) -> TableDefinition:
    """Create an empty table. Raises an error if the table already exists and
    `fail_if_exists` is True (default behavior)."""
    catalog = get_catalog(catalog)
    return catalog.impl.create_table(
        table,
        namespace,
        lifecycle_state,
        schema,
        schema_consistency,
        partition_scheme,
        primary_keys,
        sort_keys,
        description,
        table_properties,
        namespace_properties,
        content_types,
        fail_if_exists,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def drop_table(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    purge: bool = False,
    *args,
    **kwargs,
) -> None:
    """Drop a table from the catalog and optionally purge it. Raises an error
    if the table does not exist."""
    catalog = get_catalog(catalog)
    catalog.impl.drop_table(
        table,
        namespace,
        purge,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def refresh_table(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """Refresh metadata cached on the Ray cluster for the given table."""
    catalog = get_catalog(catalog)
    catalog.impl.refresh_table(
        table,
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def list_tables(
    namespace: Optional[str] = None, catalog: Optional[str] = None, *args, **kwargs
) -> ListResult[TableDefinition]:
    """List a page of table definitions. Raises an error if the given namespace
    does not exist."""
    catalog = get_catalog(catalog)
    return catalog.impl.list_tables(
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def get_table(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[TableDefinition]:
    """Get table definition metadata. Returns None if the given table does not
    exist."""
    catalog = get_catalog(catalog)
    return catalog.impl.get_table(
        table,
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def truncate_table(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """Truncate table data. Raises an error if the table does not exist."""
    catalog = get_catalog(catalog)
    catalog.impl.truncate_table(
        table,
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def rename_table(
    table: str,
    new_name: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """Rename a table."""
    catalog = get_catalog(catalog)
    catalog.impl.rename_table(
        table,
        new_name,
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def table_exists(
    table: str,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> bool:
    """Returns True if the given table exists, False if not."""
    catalog = get_catalog(catalog)
    return catalog.impl.table_exists(
        table,
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


# namespace functions
def list_namespaces(
    catalog: Optional[str] = None, *args, **kwargs
) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    catalog = get_catalog(catalog)
    return catalog.impl.list_namespaces(
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def get_namespace(
    namespace: str, catalog: Optional[str] = None, *args, **kwargs
) -> Optional[Namespace]:
    """Get table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    catalog = get_catalog(catalog)
    return catalog.impl.get_namespace(
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def namespace_exists(
    namespace: str, catalog: Optional[str] = None, *args, **kwargs
) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    catalog = get_catalog(catalog)
    return catalog.impl.namespace_exists(
        namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def create_namespace(
    namespace: str,
    properties: NamespaceProperties,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> Namespace:
    """Creates a table namespace with the given name and properties. Returns
    the created namespace. Raises an error if the namespace already exists."""
    catalog = get_catalog(catalog)
    return catalog.impl.create_namespace(
        namespace,
        properties,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def alter_namespace(
    namespace: str,
    catalog: Optional[str] = None,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """Alter table namespace definition."""
    catalog = get_catalog(catalog)
    catalog.impl.alter_namespace(
        namespace,
        properties,
        new_namespace,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def drop_namespace(
    namespace: str, catalog: Optional[str] = None, purge: bool = False, *args, **kwargs
) -> None:
    """Drop the given namespace and all of its tables from the catalog,
    optionally purging them."""
    catalog = get_catalog(catalog)
    catalog.impl.drop_namespace(
        namespace,
        purge,
        catalog=catalog.native_object,
        *args,
        **kwargs,
    )


def default_namespace(catalog: Optional[str] = None) -> str:
    """Returns the default namespace for the catalog."""
    catalog = get_catalog(catalog)
    return catalog.impl.default_namespace(catalog=catalog.native_object)
