import pyarrow as pa

import ray

from deltacat import SortKey, TableWriteMode, ContentType, all_catalogs, \
    ListResult, Namespace, LifecycleState, SchemaConsistencyType, LocalTable, \
    LocalDataset, DistributedDataset, Catalog, TableDefinition
from typing import Any, Dict, List, Optional, Set, Union


def _get_catalog(name: Optional[str] = None) -> Catalog:
    if not all_catalogs:
        raise ValueError(
            "No catalogs available! Call "
            "`deltacat.init(catalogs={...})` to register one or more "
            "catalogs then retry.")
    catalog = ray.get(all_catalogs.get.remote(name)) if name \
        else ray.get(all_catalogs.default.remote())
    if not catalog:
        available_catalogs = ray.get(all_catalogs.all.remote()).values()
        raise ValueError(
            f"Catalog '{name}' not found. Available catalogs: "
            f"{available_catalogs}.")
    return catalog


# table functions
def write_to_table(
        data: Union[LocalTable, LocalDataset, DistributedDataset],
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        mode: TableWriteMode = TableWriteMode.AUTO,
        content_type: ContentType = ContentType.PARQUET,
        *args,
        **kwargs) -> None:
    """Write local or distributed data to a table. Raises an error if the
    table does not exist and the table write mode is not CREATE or AUTO.

    When creating a table, all `create_table` parameters may be optionally
    specified as additional keyword arguments. When appending to, or replacing,
    an existing table, all `alter_table` parameters may be optionally specified
    as additional keyword arguments."""
    _get_catalog(catalog).impl.write_to_table(
        data,
        table,
        namespace,
        mode,
        content_type,
        *args,
        **kwargs)


def read_table(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> DistributedDataset:
    """Read a table into a distributed dataset."""
    return _get_catalog(catalog).impl.read_table(
        table,
        namespace,
        *args,
        **kwargs)


def alter_table(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        lifecycle_state: Optional[LifecycleState] = None,
        schema_updates: Optional[Dict[str, Any]] = None,
        partition_updates: Optional[Dict[str, Any]] = None,
        primary_keys: Optional[Set[str]] = None,
        sort_keys: Optional[List[SortKey]] = None,
        description: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        *args,
        **kwargs) -> None:
    """Alter table definition."""
    _get_catalog(catalog).impl.alter_table(
        table,
        namespace,
        lifecycle_state,
        schema_updates,
        partition_updates,
        primary_keys,
        sort_keys,
        description,
        properties,
        *args,
        **kwargs)


def create_table(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        lifecycle_state: Optional[LifecycleState] = None,
        schema: Optional[Union[pa.Schema, str, bytes]] = None,
        schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
        partition_keys: Optional[List[Dict[str, Any]]] = None,
        primary_keys: Optional[Set[str]] = None,
        sort_keys: Optional[List[SortKey]] = None,
        description: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        permissions: Optional[Dict[str, Any]] = None,
        content_types: Optional[List[ContentType]] = None,
        replace_existing_table: bool = False,
        *args,
        **kwargs) -> TableDefinition:
    """Create an empty table. Raises an error if the table already exists and
    `replace_existing_table` is False."""
    return _get_catalog(catalog).impl.create_table(
        table,
        namespace,
        lifecycle_state,
        schema,
        schema_consistency,
        partition_keys,
        primary_keys,
        sort_keys,
        description,
        properties,
        permissions,
        content_types,
        replace_existing_table,
        *args,
        **kwargs)


def drop_table(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        purge: bool = False,
        *args,
        **kwargs) -> None:
    """Drop a table from the catalog and optionally purge it. Raises an error
    if the table does not exist."""
    _get_catalog(catalog).impl.drop_table(
        table,
        namespace,
        purge,
        *args,
        **kwargs)


def refresh_table(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Refresh metadata cached on the Ray cluster for the given table."""
    _get_catalog(catalog).impl.refresh_table(
        table,
        namespace,
        *args,
        **kwargs)


def list_tables(
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> ListResult[TableDefinition]:
    """List a page of table definitions. Raises an error if the given namespace
    does not exist."""
    return _get_catalog(catalog).impl.list_tables(
        namespace,
        *args,
        **kwargs)


def get_table(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> Optional[TableDefinition]:
    """Get table definition metadata. Returns None if the given table does not
    exist."""
    return _get_catalog(catalog).impl.get_table(
        table,
        namespace,
        *args,
        **kwargs)


def truncate_table(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Truncate table data. Raises an error if the table does not exist."""
    _get_catalog(catalog).impl.truncate_table(
        table,
        namespace,
        *args,
        **kwargs)


def rename_table(
        table: str,
        new_name: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Rename a table."""
    _get_catalog(catalog).impl.rename_table(
        table,
        new_name,
        namespace,
        *args,
        **kwargs)


def table_exists(
        table: str,
        namespace: Optional[str] = None,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> bool:
    """Returns True if the given table exists, False if not."""
    return _get_catalog(catalog).impl.table_exists(
        table,
        namespace,
        *args,
        **kwargs)


# namespace functions
def list_namespaces(
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    return _get_catalog(catalog).impl.list_namespaces(*args, **kwargs)


def get_namespace(
        namespace: str,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> Optional[Namespace]:
    """Get table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    return _get_catalog(catalog).impl.get_namespace(
        namespace,
        *args,
        **kwargs)


def namespace_exists(
        namespace: str,
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    return _get_catalog(catalog).impl.namespace_exists(
        namespace,
        *args,
        **kwargs)


def create_namespace(
        namespace: str,
        permissions: Dict[str, Any],
        catalog: Optional[str] = None,
        *args,
        **kwargs) -> Namespace:
    """Creates a table namespace with the given name and permissions. Returns
    the created namespace. Raises an error if the namespace already exists."""
    return _get_catalog(catalog).impl.create_namespace(
        namespace,
        permissions,
        *args,
        **kwargs)


def alter_namespace(
        namespace: str,
        catalog: Optional[str] = None,
        permissions: Optional[Dict[str, Any]] = None,
        new_namespace: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Alter table namespace definition."""
    _get_catalog(catalog).impl.alter_namespace(
        namespace,
        permissions,
        new_namespace,
        *args,
        **kwargs)


def drop_namespace(
        namespace: str,
        catalog: Optional[str] = None,
        purge: bool = False,
        *args,
        **kwargs) -> None:
    """Drop the given namespace and all of its tables from the catalog,
    optionally purging them."""
    _get_catalog(catalog).impl.drop_namespace(
        namespace,
        purge,
        *args,
        **kwargs)


def default_namespace(catalog: Optional[str] = None) -> str:
    """Returns the default namespace for the catalog."""
    return _get_catalog(catalog).impl.default_namespace()
