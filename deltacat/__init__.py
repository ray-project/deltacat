import importlib
import logging

import deltacat.logs  # noqa: F401
from deltacat.api import (
    copy,
    get,
    put,
)
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
    is_initialized,
    init,
    get_catalog,
    put_catalog,
)
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage import (
    DistributedDataset,
    Field,
    LifecycleState,
    ListResult,
    LocalDataset,
    LocalTable,
    Namespace,
    PartitionKey,
    PartitionScheme,
    Schema,
    SchemaConsistencyType,
    SortKey,
    SortOrder,
    SortScheme,
    NullOrder,
)
from deltacat.storage.rivulet import Dataset, Datatype
from deltacat.types.media import ContentEncoding, ContentType, TableType
from deltacat.types.tables import TableWriteMode

__iceberg__ = []
if importlib.util.find_spec("pyiceberg") is not None:
    from deltacat.catalog.iceberg import impl as IcebergCatalog  # noqa: F401

    __iceberg__ = [
        "IcebergCatalog",
    ]

deltacat.logs.configure_deltacat_logger(logging.getLogger(__name__))

__version__ = "2.0.0b6"


__all__ = [
    "__version__",
    "copy",
    "get",
    "put",
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
    "get_catalog",
    "put_catalog",
    "is_initialized",
    "init",
    "Catalog",
    "ContentType",
    "ContentEncoding",
    "DistributedDataset",
    "Dataset",
    "Datatype",
    "Field",
    "LifecycleState",
    "ListResult",
    "LocalDataset",
    "LocalTable",
    "Namespace",
    "NullOrder",
    "PartitionKey",
    "PartitionScheme",
    "Schema",
    "SchemaConsistencyType",
    "SortKey",
    "SortOrder",
    "SortScheme",
    "TableDefinition",
    "TableType",
    "TableWriteMode",
]

__all__ += __iceberg__
