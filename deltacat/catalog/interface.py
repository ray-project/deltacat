from typing import Any, Dict, List, Optional, Set, Union

import pyarrow as pa

from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortKey
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace
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
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs
) -> None:
    """Write local or distributed data to a table. Raises an error if the
    table does not exist and the table write mode is not CREATE or AUTO.

    When creating a table, all `create_table` parameters may be optionally
    specified as additional keyword arguments. When appending to, or replacing,
    an existing table, all `alter_table` parameters may be optionally specified
    as additional keyword arguments."""
    raise NotImplementedError("write_to_table not implemented")


def read_table(
    table: str, namespace: Optional[str] = None, *args, **kwargs
) -> DistributedDataset:
    """Read a table into a distributed dataset."""
    raise NotImplementedError("read_table not implemented")


def alter_table(
    table: str,
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    primary_keys: Optional[Set[str]] = None,
    sort_keys: Optional[List[SortKey]] = None,
    description: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    *args,
    **kwargs
) -> None:
    """Alter table definition."""
    raise NotImplementedError("alter_table not implemented")


def create_table(
    table: str,
    namespace: Optional[str] = None,
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
    **kwargs
) -> TableDefinition:
    """Create an empty table. Raises an error if the table already exists and
    `replace_existing_table` is False."""
    raise NotImplementedError("create_table not implemented")


def drop_table(
    table: str, namespace: Optional[str] = None, purge: bool = False, *args, **kwargs
) -> None:
    """Drop a table from the catalog and optionally purge it. Raises an error
    if the table does not exist."""
    raise NotImplementedError("drop_table not implemented")


def refresh_table(table: str, namespace: Optional[str] = None, *args, **kwargs) -> None:
    """Refresh metadata cached on the Ray cluster for the given table."""
    raise NotImplementedError("refresh_table not implemented")


def list_tables(
    namespace: Optional[str] = None, *args, **kwargs
) -> ListResult[TableDefinition]:
    """List a page of table definitions. Raises an error if the given namespace
    does not exist."""
    raise NotImplementedError("list_tables not implemented")


def get_table(
    table: str, namespace: Optional[str] = None, *args, **kwargs
) -> Optional[TableDefinition]:
    """Get table definition metadata. Returns None if the given table does not
    exist."""
    raise NotImplementedError("get_table not implemented")


def truncate_table(
    table: str, namespace: Optional[str] = None, *args, **kwargs
) -> None:
    """Truncate table data. Raises an error if the table does not exist."""
    raise NotImplementedError("truncate_table not implemented")


def rename_table(
    table: str, new_name: str, namespace: Optional[str] = None, *args, **kwargs
) -> None:
    """Rename a table."""
    raise NotImplementedError("rename_table not implemented")


def table_exists(table: str, namespace: Optional[str] = None, *args, **kwargs) -> bool:
    """Returns True if the given table exists, False if not."""
    raise NotImplementedError("table_exists not implemented")


# namespace functions
def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    raise NotImplementedError("list_namespaces not implemented")


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    raise NotImplementedError("get_namespace not implemented")


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    raise NotImplementedError("namespace_exists not implemented")


def create_namespace(
    namespace: str, permissions: Dict[str, Any], *args, **kwargs
) -> Namespace:
    """Creates a table namespace with the given name and permissions. Returns
    the created namespace. Raises an error if the namespace already exists."""
    raise NotImplementedError("create_namespace not implemented")


def alter_namespace(
    namespace: str,
    permissions: Optional[Dict[str, Any]] = None,
    new_namespace: Optional[str] = None,
    *args,
    **kwargs
) -> None:
    """Alter table namespace definition."""
    raise NotImplementedError("alter_namespace not implemented")


def drop_namespace(namespace: str, purge: bool = False, *args, **kwargs) -> None:
    """Drop the given namespace and all of its tables from the catalog,
    optionally purging them."""
    raise NotImplementedError("drop_namespace not implemented")


def default_namespace() -> str:
    """Returns the default namespace for the catalog."""
    raise NotImplementedError("default_namespace not implemented")


# catalog functions
def initialize(*args, **kwargs) -> None:
    """Initializes the data catalog with the given arguments."""
    raise NotImplementedError("initialize not implemented")
