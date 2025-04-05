import logging

import deltacat.logs  # noqa: F401
from deltacat.catalog.delegate import (
    alter_namespace,
    alter_table,
    create_namespace,
    create_table,
    default_namespace,
    drop_namespace,
    drop_table,
    get_namespace,
    get_table,
    list_namespaces,
    list_tables,
    namespace_exists,
    read_table,
    refresh_table,
    rename_table,
    table_exists,
    truncate_table,
    write_to_table,
)
from deltacat.catalog.model.catalog import (  # noqa: F401
    Catalog,
    Catalogs,
    all_catalogs,
    init,
)
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage import (
    DistributedDataset,
    LifecycleState,
    ListResult,
    LocalDataset,
    LocalTable,
    Namespace,
    SchemaConsistencyType,
    SortKey,
    SortOrder,
)
from deltacat.types.media import ContentEncoding, ContentType, TableType
from deltacat.types.tables import TableWriteMode

deltacat.logs.configure_deltacat_logger(logging.getLogger(__name__))

__version__ = "1.1.34"


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
