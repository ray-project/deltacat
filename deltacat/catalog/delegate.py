from typing import Any, Dict, List, Optional, Union

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
    StreamFormat,
)
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode


# table functions
def write_to_table(
    data: Union[LocalTable, LocalDataset, DistributedDataset],
    table: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    **kwargs,
) -> None:
    """Write local or distributed data to a table. Raises an error if the
    table does not exist and the table write mode is not CREATE or AUTO.

    When creating a table, all `create_table` parameters may be optionally
    specified as additional keyword arguments. When appending to, or replacing,
    an existing table, all `alter_table` parameters may be optionally specified
    as additional keyword arguments."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.write_to_table(
        data,
        table,
        *args,
        namespace=namespace,
        mode=mode,
        content_type=content_type,
        inner=catalog_obj.inner,
        **kwargs,
    )


def read_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs,
) -> DistributedDataset:
    """Read a table into a distributed dataset."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.read_table(
        table,
        *args,
        namespace=namespace,
        inner=catalog_obj.inner,
        **kwargs,
    )


def alter_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    **kwargs,
) -> None:
    """Alter table definition."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.alter_table(
        table,
        *args,
        namespace=namespace,
        lifecycle_state=lifecycle_state,
        schema_updates=schema_updates,
        partition_updates=partition_updates,
        sort_keys=sort_keys,
        description=description,
        properties=properties,
        inner=catalog_obj.inner,
        **kwargs,
    )


def create_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
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
    """Create an empty table. Raises an error if the table already exists and
    `fail_if_exists` is True (default behavior)."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.create_table(
        name,
        *args,
        namespace=namespace,
        version=version,
        lifecycle_state=lifecycle_state,
        schema=schema,
        partition_scheme=partition_scheme,
        sort_keys=sort_keys,
        description=description,
        table_properties=table_properties,
        namespace_properties=namespace_properties,
        content_types=content_types,
        fail_if_exists=fail_if_exists,
        inner=catalog_obj.inner,
        **kwargs,
    )


def drop_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    table_version: Optional[str] = None,
    purge: bool = False,
    **kwargs,
) -> None:
    """Drop a table from the catalog and optionally purge it. Raises an error
    if the table does not exist."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.drop_table(
        name,
        *args,
        namespace=namespace,
        table_version=table_version,
        purge=purge,
        inner=catalog_obj.inner,
        **kwargs,
    )


def refresh_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs,
) -> None:
    """Refresh metadata cached on the Ray cluster for the given table."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.refresh_table(
        table,
        *args,
        namespace=namespace,
        inner=catalog_obj.inner,
        **kwargs,
    )


def list_tables(
    *args, namespace: Optional[str] = None, catalog: Optional[str] = None, **kwargs
) -> ListResult[TableDefinition]:
    """List a page of table definitions. Raises an error if the given namespace
    does not exist."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.list_tables(
        *args,
        namespace=namespace,
        inner=catalog_obj.inner,
        **kwargs,
    )


def get_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    **kwargs,
) -> Optional[TableDefinition]:
    """Get table definition metadata. Returns None if the given table does not
    exist."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.get_table(
        name,
        *args,
        namespace=namespace,
        table_version=table_version,
        stream_format=stream_format,
        inner=catalog_obj.inner,
        **kwargs,
    )


def truncate_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs,
) -> None:
    """Truncate table data. Raises an error if the table does not exist."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.truncate_table(
        table,
        *args,
        namespace=namespace,
        inner=catalog_obj.inner,
        **kwargs,
    )


def rename_table(
    table: str,
    new_name: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs,
) -> None:
    """Rename a table."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.rename_table(
        table,
        new_name,
        *args,
        namespace=namespace,
        inner=catalog_obj.inner,
        **kwargs,
    )


def table_exists(
    table: str,
    *args,
    namespace: Optional[str] = None,
    catalog: Optional[str] = None,
    **kwargs,
) -> bool:
    """Returns True if the given table exists, False if not."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.table_exists(
        table,
        *args,
        namespace=namespace,
        inner=catalog_obj.inner,
        **kwargs,
    )


# namespace functions
def list_namespaces(
    *args, catalog: Optional[str] = None, **kwargs
) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.list_namespaces(
        *args,
        inner=catalog_obj.inner,
        **kwargs,
    )


def get_namespace(
    namespace: str,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Namespace]:
    """Get table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.get_namespace(
        namespace,
        *args,
        inner=catalog_obj.inner,
        **kwargs,
    )


def namespace_exists(
    namespace: str,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.namespace_exists(
        namespace,
        *args,
        inner=catalog_obj.inner,
        **kwargs,
    )


def create_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    catalog: Optional[str] = None,
    *args,
    **kwargs,
) -> Namespace:
    """Creates a table namespace with the given name and properties. Returns
    the created namespace. Raises an error if the namespace already exists."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.create_namespace(
        namespace,
        *args,
        properties=properties,
        inner=catalog_obj.inner,
        **kwargs,
    )


def alter_namespace(
    namespace: str,
    *args,
    catalog: Optional[str] = None,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    **kwargs,
) -> None:
    """Alter table namespace definition."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.alter_namespace(
        namespace,
        *args,
        properties=properties,
        new_namespace=new_namespace,
        inner=catalog_obj.inner,
        **kwargs,
    )


def drop_namespace(
    namespace: str, *args, catalog: Optional[str] = None, purge: bool = False, **kwargs
) -> None:
    """Drop the given namespace and all of its tables from the catalog,
    optionally purging them."""
    catalog_obj = get_catalog(catalog)
    catalog_obj.impl.drop_namespace(
        namespace,
        *args,
        purge=purge,
        inner=catalog_obj.inner,
        **kwargs,
    )


def default_namespace(*args, catalog: Optional[str] = None, **kwargs) -> str:
    """Returns the default namespace for the catalog."""
    catalog_obj = get_catalog(catalog)
    return catalog_obj.impl.default_namespace(*args, inner=catalog_obj.inner, **kwargs)
