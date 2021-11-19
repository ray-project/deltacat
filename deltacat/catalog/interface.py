import pyarrow as pa
from deltacat import SortKey, TableWriteMode, ContentType, ListResult, \
    Namespace, LifecycleState, SchemaConsistencyType, LocalTable, \
    LocalDataset, DistributedDataset
from typing import Any, Dict, List, Optional, Set, Union


# table functions
def write_to_table(
        data: Union[LocalTable, LocalDataset, DistributedDataset],
        table: str,
        namespace: Optional[str] = None,
        mode: TableWriteMode = TableWriteMode.APPEND,
        content_type: ContentType = ContentType.PARQUET,
        *args,
        **kwargs) -> None:
    """Write local or distributed data to a table."""
    raise NotImplementedError


def read_table(
        table: str,
        namespace: Optional[str] = None,
        *args,
        **kwargs) -> DistributedDataset:
    """Read a table into a distributed dataset."""
    raise NotImplementedError


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
        **kwargs) -> None:
    """Alter table definition."""
    raise NotImplementedError


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
        **kwargs) -> Dict[str, Any]:
    """Create an empty table."""
    raise NotImplementedError


def drop_table(
        table: str,
        namespace: Optional[str] = None,
        purge: bool = False,
        *args,
        **kwargs) -> None:
    """Drop a table from the catalog and optionally purge it."""
    raise NotImplementedError


def refresh_table(
        table: str,
        namespace: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Refresh metadata cached on the Ray cluster for the given table."""
    raise NotImplementedError


def list_tables(
        namespace: Optional[str] = None,
        *args,
        **kwargs) -> ListResult[Dict[str, Any]]:
    """List all tables in the given namespace."""
    raise NotImplementedError


def get_table(
        table: str,
        namespace: Optional[str] = None,
        *args,
        **kwargs) -> Dict[str, Any]:
    """Get table metadata."""
    raise NotImplementedError


def truncate_table(
        table: str,
        namespace: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Truncate table data."""
    raise NotImplementedError


def rename_table(
        table: str,
        new_name: str,
        namespace: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Rename a table."""
    raise NotImplementedError


def table_exists(
        table: str,
        namespace: Optional[str] = None,
        *args,
        **kwargs) -> bool:
    """Returns True if the given table exists, False if not."""
    raise NotImplementedError


# namespace functions
def list_namespaces(
        *args,
        **kwargs) -> ListResult[Namespace]:
    """Lists a page of table namespaces. Namespaces are returned as list result
    items."""
    raise NotImplementedError("list_namespaces not implemented")


def get_namespace(
        namespace: str,
        *args,
        **kwargs) -> Optional[Namespace]:
    """Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    raise NotImplementedError("get_namespace not implemented")


def namespace_exists(
        namespace: str,
        *args,
        **kwargs) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    raise NotImplementedError("namespace_exists not implemented")


def create_namespace(
        namespace: str,
        permissions: Dict[str, Any],
        *args,
        **kwargs) -> Namespace:
    """Creates a table namespace with the given name and permissions. Returns
    the created namespace."""
    raise NotImplementedError("create_namespace not implemented")


def alter_namespace(
        namespace: str,
        permissions: Optional[Dict[str, Any]] = None,
        new_namespace: Optional[str] = None,
        *args,
        **kwargs) -> None:
    """Updates a table namespace's name and/or permissions. Raises an error if
    the given namespace does not exist."""
    raise NotImplementedError("alter_namespace not implemented")


def drop_namespace(
        namespace: str,
        purge: bool = False,
        *args,
        **kwargs) -> None:
    """Drop the given namespace and all of its tables from the catalog,
    optionally purging them."""
    raise NotImplementedError


def default_namespace() -> str:
    """Returns the default namespace for the catalog."""
    raise NotImplementedError


# catalog functions
def initialize(*args, **kwargs) -> None:
    """Initializes the data catalog with the given arguments."""
    raise NotImplementedError
