import logging

from typing import Any, Dict, List, Optional, Union

from daft import DataFrame, context
from daft.daft import ScanOperatorHandle, StorageConfig
from daft.logical.builder import LogicalPlanBuilder

from deltacat import logs
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.daft.daft_scan import DeltaCatScanOperator
from deltacat.exceptions import TableAlreadyExistsError
from deltacat.storage.iceberg.iceberg_scan_planner import IcebergScanPlanner
from deltacat.storage.iceberg.model import PartitionSchemeMapper, SchemaMapper
from deltacat.storage.model.partition import PartitionScheme
from deltacat.storage.iceberg.impl import _get_native_catalog
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
from deltacat.storage.iceberg import impl as IcebergStorage
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode
from deltacat.constants import DEFAULT_NAMESPACE
from deltacat.catalog.iceberg.iceberg_catalog_config import IcebergCatalogConfig

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.transforms import BucketTransform

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


# catalog functions
def initialize(*args, config: IcebergCatalogConfig, **kwargs) -> Catalog:
    """
    Initializes an Iceberg catalog with the given config.

    NOTE: because PyIceberg catalogs are not pickle-able, we cannot accept them as catalog initialization parameters,
    since catalog initialization parameters are passed to Ray actors (see: :class:`deltacat.catalog.Catalogs`)

    Args:
        **kwargs: Arguments to be passed to PyIceberg Catalog.
            If 'catalog' is provided as a PyIceberg Catalog instance, it will be used directly.
            Otherwise, the arguments will be used to load a catalog via pyiceberg.catalog.load_catalog.

    Returns:
        IcebergCatalogConfig: Configuration wrapper containing the PyIceberg Catalog.
    """

    # If no catalog is provided, try to load one with PyIceberg

    load_catalog_args = {"type": config.type.value, **config.properties, **kwargs}
    catalog = load_catalog(**load_catalog_args)
    return catalog


# table functions
def write_to_table(
    data: Union[LocalTable, LocalDataset, DistributedDataset],
    table: str,
    *args,
    namespace: Optional[str] = None,
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

    # TODO (pdames): derive schema automatically from data if not
    #  explicitly specified in kwargs, and table needs to be created
    # kwargs["schema"] = kwargs["schema"] or derived_schema
    kwargs["fail_if_exists"] = mode == TableWriteMode.CREATE
    table_definition = (
        create_table(
            table,
            namespace=namespace,
            *args,
            **kwargs,
        )
        if (mode == TableWriteMode.AUTO or mode == TableWriteMode.CREATE)
        else get_table(table, namespace=namespace, *args, **kwargs)
    )

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
                    source_field = schema.find_field(
                        name_or_id=partition_field.source_id
                    )
                    out_df = data.write_parquet(
                        path,
                        partition_cols=[
                            data[source_field.name].partitioning.iceberg_bucket(
                                ice_bucket_transform.num_buckets
                            ),
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
                    raise NotImplementedError(
                        f"iceberg writes not implemented for content type: {content_type}"
                    )
            else:
                raise NotImplementedError(
                    f"daft partitioning not implemented for iceberg transform: {partition_field.transform}"
                )
    else:
        raise NotImplementedError(
            f"iceberg write-back not implemented for data type: {type(data)}"
        )


def read_table(
    table: str, *args, namespace: Optional[str] = None, **kwargs
) -> DistributedDataset:
    """Read a table into a distributed dataset."""
    # TODO: more proper IO configuration
    io_config = context.get_context().daft_planning_config.default_io_config
    multithreaded_io = context.get_context().get_or_create_runner().name != "ray"

    storage_config = StorageConfig(multithreaded_io, io_config)

    dc_table = get_table(name=table, namespace=namespace, **kwargs)
    dc_scan_operator = DeltaCatScanOperator(dc_table, storage_config)
    handle = ScanOperatorHandle.from_python_scan_operator(dc_scan_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


def alter_table(
    table: str,
    *args,
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    sort_keys: Optional[SortScheme] = None,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    **kwargs,
) -> None:
    """Alter table definition."""
    raise NotImplementedError("alter_table not implemented")


def create_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
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
    """Create an empty table in the catalog"""

    namespace = namespace or default_namespace()
    existing_table = get_table(
        name,
        *args,
        namespace=namespace,
        **kwargs,
    )
    if existing_table:
        if fail_if_exists:
            err_msg = (
                f"Table `{namespace}.{name}` already exists. "
                f"To suppress this error, rerun `create_table()` with "
                f"`fail_if_exists=False`."
            )
            raise TableAlreadyExistsError(err_msg)
        else:
            logger.debug(f"Returning existing table: `{namespace}.{name}`")
            return existing_table

    if not IcebergStorage.namespace_exists(namespace, **kwargs):
        logger.debug(f"Namespace {namespace} doesn't exist. Creating it...")
        IcebergStorage.create_namespace(
            namespace,
            properties=namespace_properties or {},
            **kwargs,
        )

    IcebergStorage.create_table_version(
        namespace=namespace,
        table_name=name,
        table_version=version,
        schema=schema,
        partition_scheme=partition_scheme,
        sort_keys=sort_keys,
        table_properties=table_properties,
        **kwargs,
    )

    return get_table(
        name,
        *args,
        namespace=namespace,
        **kwargs,
    )


def drop_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    purge: bool = False,
    **kwargs,
) -> None:
    """Drop a table from the catalog and optionally purge it. Raises an error
    if the table does not exist."""
    raise NotImplementedError("drop_table not implemented")


def refresh_table(table: str, *args, namespace: Optional[str] = None, **kwargs) -> None:
    """Refresh metadata cached on the Ray cluster for the given table."""
    raise NotImplementedError("refresh_table not implemented")


def list_tables(
    *args, namespace: Optional[str] = None, **kwargs
) -> ListResult[TableDefinition]:
    """List a page of table definitions. Raises an error if the given namespace
    does not exist."""
    raise NotImplementedError("list_tables not implemented")


def get_table(
    name: str,
    *args,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    **kwargs,
) -> Optional[TableDefinition]:
    """Get table definition metadata.

    Args:
        name: Name of the table to retrieve
        namespace: Optional namespace of the table. Uses default namespace if not specified.
        table_version: Optional specific version of the table to retrieve.
            If not specified, the latest version is used.
        stream_format: Optional stream format to retrieve

    Returns:
        Deltacat TableDefinition if the table exists, None otherwise.
    """
    namespace = namespace or default_namespace()
    stream = IcebergStorage.get_stream(namespace=namespace, table_name=name, **kwargs)
    if not stream:
        return None
    table_obj = IcebergStorage.get_table(namespace=namespace, table_name=name, **kwargs)
    if not table_obj:
        return None
    table_version_obj = None
    if table_version:
        table_version_obj = IcebergStorage.get_table_version(
            namespace=namespace, table_name=name, table_version=table_version, **kwargs
        )
    else:
        table_version_obj = IcebergStorage.get_latest_table_version(
            namespace=namespace, table_name=name, **kwargs
        )
    if not table_version_obj:
        return None
    scan_planner = IcebergScanPlanner(_get_native_catalog(**kwargs))
    return TableDefinition.of(
        table=table_obj,
        table_version=table_version_obj,
        stream=stream,
        native_object=table_obj.native_object,
        scan_planner=scan_planner,
    )


def truncate_table(
    table: str, *args, namespace: Optional[str] = None, **kwargs
) -> None:
    """Truncate table data. Raises an error if the table does not exist."""
    raise NotImplementedError("truncate_table not implemented")


def rename_table(
    table: str, new_name: str, *args, namespace: Optional[str] = None, **kwargs
) -> None:
    """Rename a table."""
    raise NotImplementedError("rename_table not implemented")


def table_exists(table: str, *args, namespace: Optional[str] = None, **kwargs) -> bool:
    """Returns True if the given table exists, False if not."""
    namespace = namespace or default_namespace()
    return IcebergStorage.table_exists(namespace=namespace, table_name=table, **kwargs)


# namespace functions
def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    return IcebergStorage.list_namespaces(**kwargs)


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    return IcebergStorage.get_namespace(namespace, **kwargs)


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    return IcebergStorage.namespace_exists(namespace, **kwargs)


def create_namespace(
    namespace: str, *args, properties: Optional[NamespaceProperties] = None, **kwargs
) -> Namespace:
    """Creates a table namespace with the given name and properties. Returns
    the created namespace. Raises an error if the namespace already exists."""
    raise NotImplementedError("create_namespace not implemented")


def alter_namespace(
    namespace: str,
    *args,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    **kwargs,
) -> None:
    """Alter table namespace definition."""
    raise NotImplementedError("alter_namespace not implemented")


def drop_namespace(namespace: str, *args, purge: bool = False, **kwargs) -> None:
    """Drop the given namespace and all of its tables from the catalog,
    optionally purging them."""
    raise NotImplementedError("drop_namespace not implemented")


def default_namespace(*args, **kwargs) -> str:
    """Returns the default namespace for the catalog."""
    return DEFAULT_NAMESPACE
