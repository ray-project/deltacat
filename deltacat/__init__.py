import logging

import deltacat.logs
from deltacat.types.media import ContentType, ContentEncoding, TableType
from deltacat.types.tables import TableWriteMode
from deltacat.storage import ListResult, Namespace, LifecycleState, \
    SchemaConsistencyType, LocalTable, LocalDataset, DistributedDataset
from deltacat.compute.compactor import SortKey, SortOrder
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.catalog.model.catalog import Catalog, Catalogs, all_catalogs, \
    init
from deltacat.catalog.delegate import alter_table, create_table, drop_table, \
    refresh_table, list_tables, get_table, truncate_table, rename_table, \
    table_exists, list_namespaces, alter_namespace, create_namespace, \
    drop_namespace, default_namespace, get_namespace, namespace_exists, \
    write_to_table, read_table

logs.configure_deltacat_logger(logging.getLogger(__name__))

__version__ = "0.1.9.dev0"


__all__ = [
    "__version__",
    "all_catalogs",
    "alter_table",
    "create_table",
    "drop_table",
    "refresh_table",
    "list_tables",
    "get_table",
    "truncate_table",
    "rename_table",
    "table_exists",
    "list_namespaces",
    "get_namespace",
    "namespace_exists",
    "alter_namespace",
    "create_namespace",
    "drop_namespace",
    "default_namespace",
    "write_to_table",
    "read_table",
    "init",
    "Catalog",
    "ContentType",
    "ContentEncoding",
    "DistributedDataset",
    "LifecycleState",
    "ListResult",
    "LocalDataset",
    "LocalTable",
    "Namespace",
    "SchemaConsistencyType",
    "SortKey",
    "SortOrder",
    "TableDefinition",
    "TableType",
    "TableWriteMode",
]
