import importlib
import logging

import deltacat.logs  # noqa: F401
from deltacat.api import (
    copy,
    get,
    list,
    put,
)
from deltacat.catalog import (  # noqa: F401
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
    init,
    is_initialized,
    clear_catalogs,
    get_catalog,
    get_catalog_properties,
    pop_catalog,
    put_catalog,
    raise_if_not_initialized,
    Catalog,
    CatalogProperties,
    TableDefinition,
)
from deltacat.compute import (
    job_client,
    local_job_client,
)
from deltacat.storage import (
    Dataset,
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
from deltacat.types.media import (
    ContentEncoding,
    ContentType,
    DatasetType,
    DatastoreType,
)

from deltacat.types.tables import TableWriteMode
from deltacat.utils.url import DeltaCatUrl

__iceberg__ = []
if importlib.util.find_spec("pyiceberg") is not None:
    from deltacat.experimental.catalog.iceberg import (  # noqa: F401
        impl as IcebergCatalog,
    )

    __iceberg__ = [
        "IcebergCatalog",
    ]

deltacat.logs.configure_deltacat_logger(logging.getLogger(__name__))

__version__ = "2.0.0b11"


__all__ = [
    "__version__",
    "job_client",
    "local_job_client",
    "copy",
    "get",
    "list",
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
    "init",
    "is_initialized",
    "clear_catalogs",
    "get_catalog",
    "get_catalog_properties",
    "pop_catalog",
    "put_catalog",
    "raise_if_not_initialized",
    "Catalog",
    "CatalogProperties",
    "ContentType",
    "ContentEncoding",
    "Dataset",
    "DatasetType",
    "DatastoreType",
    "DeltaCatUrl",
    "DistributedDataset",
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
    "TableWriteMode",
]

__all__ += __iceberg__
