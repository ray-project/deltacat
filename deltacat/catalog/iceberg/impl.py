import logging

from typing import Any, Dict, List, Optional, Set, Union

from daft import DataFrame

from deltacat import logs
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.exceptions import TableAlreadyExistsError
from deltacat.storage.iceberg.impl import _get_native_catalog
from deltacat.storage.iceberg.model import PartitionSchemeMapper, SchemaMapper
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
)
from deltacat.storage.iceberg import impl as IcebergStorage
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode

from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import BucketTransform

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


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
    """Write local or distributed data to a table. Raises an error if the
    table does not exist and the table write mode is not CREATE or AUTO.

    When creating a table, all `create_table` parameters may be optionally
    specified as additional keyword arguments. When appending to, or replacing,
    an existing table, all `alter_table` parameters may be optionally specified
    as additional keyword arguments."""

    catalog = _get_native_catalog(**kwargs)

    # TODO (pdames): derive schema automatically from data if not
    #  explicitly specified in kwargs, and table needs to be created
    # kwargs["schema"] = kwargs["schema"] or derived_schema
    kwargs["fail_if_exists"] = (mode == TableWriteMode.CREATE)
    table_definition = create_table(
        table,
        namespace,
        **kwargs,
    ) if (mode == TableWriteMode.AUTO or mode == TableWriteMode.CREATE) \
        else get_table(table, namespace, catalog=catalog)

    # TODO(pdames): Use native DeltaCAT models to map from Iceberg partitioning to Daft partitioning...
    #  this lets us re-use a single model-mapper instead of different per-catalog model mappers
    schema = SchemaMapper.unmap(table_definition.table_version.schema)
    partition_spec = PartitionSchemeMapper.unmap(
        table_definition.table_version.partition_scheme,
        schema,
    )
    if isinstance(data, DataFrame):
        for partition_field in partition_spec.fields:
            if isinstance(partition_field.transform, BucketTransform):
                ice_bucket_transform: BucketTransform = partition_field.transform
                # TODO(pdames): Get a type-checked Iceberg Table automatically via unmap()
                table_location = table_definition.table.native_object.location()
                path = kwargs.get("path") or f"{table_location}/data"
                if content_type == ContentType.PARQUET:
                    source_field = schema.find_field(name_or_id=partition_field.source_id)
                    out_df = data.write_parquet(
                        path,
                        partition_cols=[
                            data[source_field.name].partitioning.iceberg_bucket(ice_bucket_transform.num_buckets),
                        ],
                    )
                    # TODO(pdames): only append s3:// to output file paths when writing to S3!
                    out_file_paths = [f"s3://{val}" for val in out_df.to_arrow()[0]]
                    from deltacat.catalog.iceberg import overrides
                    overrides.append(
                        table_definition.table.native_object,
                        out_file_paths,
                    )
                else:
                    raise NotImplementedError(f"iceberg writes not implemented for content type: {content_type}")
            else:
                raise NotImplementedError(f"daft partitioning not implemented for iceberg transform: {partition_field.transform}")
    else:
        raise NotImplementedError(f"iceberg write-back not implemented for data type: {type(data)}")


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
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Schema] = None,
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

    catalog = _get_native_catalog(**kwargs)
    existing_table = get_table(
        table,
        namespace,
        catalog=catalog,
    )
    if existing_table:
        if fail_if_exists:
            err_msg = (
                f"Table `{namespace}.{table}` already exists. "
                f"To suppress this error, rerun `create_table()` with "
                f"`fail_if_exists=False`."
            )
            raise TableAlreadyExistsError(err_msg)
        else:
            logger.debug(f"Returning existing table: `{namespace}.{table}`")
            return existing_table

    if not IcebergStorage.namespace_exists(
        namespace,
        catalog=catalog,
    ):
        logger.debug(f"Namespace {namespace} doesn't exist. Creating it...")
        IcebergStorage.create_namespace(
            namespace,
            properties=namespace_properties or {},
            catalog=catalog,
        )

    IcebergStorage.create_table_version(
        namespace=namespace,
        table_name=table,
        schema=schema,
        partition_scheme=partition_scheme,
        sort_keys=sort_keys,
        table_properties=table_properties,
        **kwargs,
    )
    return get_table(
        table,
        namespace,
        catalog=catalog,
    )


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
    catalog = _get_native_catalog(**kwargs)
    stream = IcebergStorage.get_stream(
        namespace=namespace,
        table_name=table,
        catalog=catalog
    )
    if not stream:
        return None
    table_obj = IcebergStorage.get_table(
        namespace=namespace,
        table_name=table,
        catalog=catalog
    )
    if not table_obj:
        return None
    table_version = IcebergStorage.get_latest_table_version(
        namespace=namespace,
        table_name=table,
        catalog=catalog
    )
    if not table_version:
        return None
    return TableDefinition.of(
        table=table_obj,
        table_version=table_version,
        stream=stream,
        native_object=table_obj.native_object,
    )


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
    return IcebergStorage.table_exists(namespace=namespace, table_name=table)


# namespace functions
def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    return IcebergStorage.list_namespaces(**kwargs)


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    return IcebergStorage.get_namespace(namespace)


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    return IcebergStorage.namespace_exists(namespace)


def create_namespace(
    namespace: str, properties: NamespaceProperties, *args, **kwargs
) -> Namespace:
    """Creates a table namespace with the given name and properties. Returns
    the created namespace. Raises an error if the namespace already exists."""
    raise NotImplementedError("create_namespace not implemented")


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


def default_namespace() -> str:
    """Returns the default namespace for the catalog."""
    raise NotImplementedError("default_namespace not implemented")


# catalog functions
def initialize(*args, **kwargs) -> Optional[Any]:
    """Initializes the data catalog by forwarding the given arguments to
    :meth:`pyiceberg.catalog.load_catalog`. Returns the Iceberg Catalog."""
    catalog = load_catalog(
        name=kwargs.get("name"),
        **kwargs.get("properties"),
    )
    return catalog
