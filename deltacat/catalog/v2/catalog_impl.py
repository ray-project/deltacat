from typing import Any, Dict, List, Optional, Union

from deltacat.catalog.catalog_properties import (
    CatalogProperties,
)

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
)
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode
from deltacat.storage.main import impl as storage_impl
from deltacat.constants import (
    DEFAULT_NAMESPACE,
)


# table functions
def write_to_table(
    data: Union[LocalTable, LocalDataset, DistributedDataset],
    table: str,
    namespace: Optional[str] = None,
    mode: TableWriteMode = TableWriteMode.AUTO,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> None:
    raise NotImplementedError("Not implemented")


def read_table(
    table: str, namespace: Optional[str] = None, *args, **kwargs
) -> DistributedDataset:
    raise NotImplementedError("Not implemented")



def alter_table(
    table: str,
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    *args,
    **kwargs,
) -> None:
    """Alter table definition."""
    raise NotImplementedError("alter_table not implemented")


def create_table(
    table: str,
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
    """
    Create an empty table. Raises an error if the table already exists and
    `fail_if_exists` is True (default behavior).
    """
    raise NotImplementedError()


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
    raise NotImplementedError()


def get_table(
    table: str, namespace: Optional[str] = None, *args, **kwargs
) -> Optional[TableDefinition]:
    """
    Get table definition metadata. Returns None if the given table does not exist.

    """
    raise NotImplementedError()


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
    catalog = kwargs.get("catalog")
    if not isinstance(catalog, CatalogProperties):
        raise ValueError("Catalog must be a CatalogProperties instance")

    namespace = namespace or default_namespace()

    return storage_impl.table_exists(
        table_name=table, namespace=namespace, catalog=catalog
    )


# namespace functions
def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    catalog = kwargs.get("catalog")
    if not isinstance(catalog, CatalogProperties):
        raise ValueError("Catalog must be a CatalogProperties instance")

    return storage_impl.list_namespaces(catalog=catalog)


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Gets table namespace metadata for the specified table namespace.
    Returns None if the given namespace does not exist.
    """
    return storage_impl.get_namespace(namespace=namespace, **kwargs)


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Returns True if the given table namespace exists, False if not."""

    return storage_impl.namespace_exists(namespace=namespace, **kwargs)


def create_namespace(
    namespace: str, properties: Optional[NamespaceProperties], *args, **kwargs
) -> Namespace:
    """Creates a table namespace with the given name and properties. Returns
    the created namespace.

    :raises ValueError if the namespace already exists.
    """
    # Check if namespace already exists
    if namespace_exists(namespace):
        raise ValueError(f"Namespace {namespace} already exists")

    # Create namespace through storage layer
    return storage_impl.create_namespace(
        namespace=namespace, properties=properties, **kwargs
    )


def alter_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """Alter table namespace definition."""
    raise NotImplementedError("alter_namespace not implemented")


def drop_namespace(namespace: str, purge: bool = False, *args, **kwargs) -> None:
    """Drop the given namespace and all of its tables from the catalog,
    optionally purging them."""
    raise NotImplementedError("drop_namespace not implemented")


def default_namespace(*args, **kwargs) -> str:
    """Returns the default namespace for the catalog."""
    return DEFAULT_NAMESPACE
