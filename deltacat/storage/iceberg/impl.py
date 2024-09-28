import logging
from typing import Any, Callable, Dict, List, Optional, Union

from pyiceberg.typedef import Identifier, EMPTY_DICT
from pyiceberg.table import Table as IcebergTable

from deltacat import logs
from deltacat.exceptions import TableVersionNotFoundError, StreamNotFoundError
from deltacat.storage import (
    Delta,
    DeltaLocator,
    DeltaProperties,
    DeltaType,
    DistributedDataset,
    LifecycleState,
    ListResult,
    LocalDataset,
    LocalTable,
    ManifestAuthor,
    Namespace,
    Partition,
    PartitionScheme,
    Schema,
    SchemaConsistencyType,
    Stream,
    StreamLocator,
    Table,
    TableProperties,
    TableVersion,
    TableVersionProperties,
    SortScheme,
    NamespaceLocator,
    NamespaceProperties,
)
from deltacat.storage.model.manifest import Manifest
from deltacat.storage.iceberg.model import (
    SchemaMapper,
    PartitionSchemeMapper,
    SortSchemeMapper,
    StreamMapper,
    TableVersionMapper,
    NamespaceMapper,
    TableMapper,
)
from deltacat.types.media import ContentType, StorageType, TableType
from deltacat.utils.common import ReadKwargsProvider

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _get_native_catalog(**kwargs) -> Catalog:
    catalog: Catalog = kwargs.get("catalog")
    if not isinstance(catalog, Catalog):
        err_msg = (
            f"unsupported `catalog` param type: `{type(Catalog)}`. "
            f"expected `catalog` param type: {Catalog}"
        )
        raise TypeError(err_msg)
    return catalog


def _to_identifier(namespace: str, table_name: str) -> Identifier:
    return tuple(namespace.split(".")) + (table_name,)


def _try_get_namespace(catalog: Catalog, namespace: str) -> Optional[Namespace]:
    try:
        properties = catalog.load_namespace_properties(namespace)
    except Exception as e:
        # NoSuchNamespaceError may be a child of another error like RESTError
        if "NoSuchNamespaceError" in str(repr(e)):
            logger.debug(f"Namespace `{namespace}` not found: {repr(e)}")
            return None
        raise e
    return Namespace.of(
        locator=NamespaceLocator.of(namespace=namespace),
        properties=properties,
    )


def _try_load_iceberg_table(
    catalog: Catalog, namespace: str, table_name: str
) -> Optional[IcebergTable]:
    identifier = _to_identifier(namespace, table_name)
    try:
        return catalog.load_table(identifier)
    except Exception as e:
        # NoSuchTableError may be a child of another error like RESTError
        if "NoSuchTableError" in str(repr(e)):
            logger.debug(f"Table `{namespace}.{table_name}` not found: {repr(e)}")
            return None
        raise e


def _try_get_table_version(
    table: Optional[IcebergTable],
    table_version: Optional[str] = None,
    catalog_properties: Dict[str, str] = EMPTY_DICT,
) -> Optional[TableVersion]:
    try:
        return TableVersionMapper.map(
            obj=table,
            timestamp=int(table_version) if table_version else None,
            catalog_properties=catalog_properties,
        )
    except TableVersionNotFoundError as e:
        logger.debug(f"Table version `{table_version}` not found.", e)
        return None


def _try_get_stream(
    table: Optional[IcebergTable],
    table_version: Optional[str] = None,
    stream_id: Optional[str] = None,
    catalog_properties: Dict[str, str] = EMPTY_DICT,
) -> Optional[TableVersion]:
    try:
        return StreamMapper.map(
            obj=table,
            metadata_timestamp=int(table_version) if table_version else None,
            snapshot_id=int(stream_id) if stream_id else None,
            catalog_properties=catalog_properties,
        )
    except StreamNotFoundError as e:
        logger.debug(f"Stream `{table_version}.{stream_id}` not found.", e)
        return None


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """
    Lists a page of table namespaces. Namespaces are returned as list result
    items.
    """
    catalog = _get_native_catalog(**kwargs)
    namespace = kwargs.get("namespace") or ()
    return ListResult.of(
        items=[NamespaceMapper.map(n) for n in catalog.list_namespaces(namespace)],
        pagination_key=None,
        next_page_provider=None,
    )


def list_tables(namespace: str, *args, **kwargs) -> ListResult[Table]:
    """
    Lists a page of tables for the given table namespace. Tables are returned as
    list result items. Raises an error if the given namespace does not exist.
    """
    raise NotImplementedError("list_tables not implemented")


def list_table_versions(
    namespace: str, table_name: str, *args, **kwargs
) -> ListResult[TableVersion]:
    """
    Lists a page of table versions for the given table. Table versions are
    returned as list result items. Raises an error if the given table does not
    exist.
    """
    raise NotImplementedError("list_table_versions not implemented")


def list_partitions(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> ListResult[Partition]:
    """
    Lists a page of partitions for the given table version. Partitions are
    returned as list result items. Table version resolves to the latest active
    table version if not specified. Raises an error if the table version does
    not exist.
    """
    raise NotImplementedError("list_partitions not implemented")


def list_stream_partitions(stream: Stream, *args, **kwargs) -> ListResult[Partition]:
    """
    Lists all partitions committed to the given stream.
    """
    raise NotImplementedError("list_stream_partitions not implemented")


def list_deltas(
    namespace: str,
    table_name: str,
    partition_values: Optional[List[Any]] = None,
    table_version: Optional[str] = None,
    first_stream_position: Optional[int] = None,
    last_stream_position: Optional[int] = None,
    ascending_order: Optional[bool] = None,
    include_manifest: bool = False,
    partition_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> ListResult[Delta]:
    """
    Lists a page of deltas for the given table version and committed partition.
    Deltas are returned as list result items. Deltas returned can optionally be
    limited to inclusive first and last stream positions. Deltas are returned by
    descending stream position by default. Table version resolves to the latest
    active table version if not specified. Partition values should not be
    specified for unpartitioned tables. Partition scheme ID resolves to the
    table version's current partition scheme by default. Raises an error if the
    given table version or partition does not exist.

    To conserve memory, the deltas returned do not include manifests by
    default. The manifests can either be optionally retrieved as part of this
    call or lazily loaded via subsequent calls to `get_delta_manifest`.
    """
    raise NotImplementedError("list_deltas not implemented")


def list_partition_deltas(
    partition: Partition, include_manifest: bool = False, *args, **kwargs
) -> ListResult[Delta]:
    """
    Lists a page of deltas committed to the given partition.

    To conserve memory, the deltas returned do not include manifests by
    default. The manifests can either be optionally retrieved as part of this
    call or lazily loaded via subsequent calls to `get_delta_manifest`.
    """
    raise NotImplementedError("list_partition_deltas not implemented")


def get_delta(
    namespace: str,
    table_name: str,
    stream_position: int,
    partition_values: Optional[List[Any]] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Delta]:
    """
    Gets the delta for the given table version, partition, and stream position.
    Table version resolves to the latest active table version if not specified.
    Partition values should not be specified for unpartitioned tables. Partition
    scheme ID resolves to the table version's current partition scheme by
    default. Raises an error if the given table version or partition does not
    exist.

    To conserve memory, the delta returned does not include a manifest by
    default. The manifest can either be optionally retrieved as part of this
    call or lazily loaded via a subsequent call to `get_delta_manifest`.
    """
    raise NotImplementedError("get_delta not implemented")


def get_latest_delta(
    namespace: str,
    table_name: str,
    partition_values: Optional[List[Any]] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Delta]:
    """
    Gets the latest delta (i.e. the delta with the greatest stream position) for
    the given table version and partition. Table version resolves to the latest
    active table version if not specified. Partition values should not be
    specified for unpartitioned tables. Partition scheme ID resolves to the
    table version's current partition scheme by default. Raises an error if the
    given table version or partition does not exist.

    To conserve memory, the delta returned does not include a manifest by
    default. The manifest can either be optionally retrieved as part of this
    call or lazily loaded via a subsequent call to `get_delta_manifest`.
    """
    raise NotImplementedError("get_latest_delta not implemented")


def download_delta(
    delta_like: Union[Delta, DeltaLocator],
    table_type: TableType = TableType.PYARROW,
    storage_type: StorageType = StorageType.DISTRIBUTED,
    max_parallelism: Optional[int] = None,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    *args,
    **kwargs,
) -> Union[LocalDataset, DistributedDataset]:
    """
    Download the given delta or delta locator into either a list of
    tables resident in the local node's memory, or into a dataset distributed
    across this Ray cluster's object store memory. Ordered table N of a local
    table list, or ordered block N of a distributed dataset, always contain
    the contents of ordered delta manifest entry N.
    """
    raise NotImplementedError("download_delta not implemented")


def download_delta_manifest_entry(
    delta_like: Union[Delta, DeltaLocator],
    entry_index: int,
    table_type: TableType = TableType.PYARROW,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    *args,
    **kwargs,
) -> LocalTable:
    """
    Downloads a single manifest entry into the specified table type for the
    given delta or delta locator. If a delta is provided with a non-empty
    manifest, then the entry is downloaded from this manifest. Otherwise, the
    manifest is first retrieved then the given entry index downloaded.
    """
    raise NotImplementedError("download_delta_manifest_entry not implemented")


def get_delta_manifest(
    delta_like: Union[Delta, DeltaLocator], *args, **kwargs
) -> Manifest:
    """
    Get the manifest associated with the given delta or delta locator. This
    always retrieves the authoritative remote copy of the delta manifest, and
    never the local manifest defined for any input delta.
    """
    raise NotImplementedError("get_delta_manifest not implemented")


def create_namespace(
    namespace: str, properties: NamespaceProperties, *args, **kwargs
) -> Namespace:
    """
    Creates a table namespace with the given name and properties. Returns
    the created namespace.
    """
    catalog = _get_native_catalog(**kwargs)
    catalog.create_namespace(namespace, properties=properties)
    return Namespace.of(
        NamespaceLocator.of(namespace),
        properties=properties,
    )


def update_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    new_namespace: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """
    Updates a table namespace's name and/or properties. Raises an error if the
    given namespace does not exist.
    """
    raise NotImplementedError("update_namespace not implemented")


def create_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    schema: Optional[Schema] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    primary_key_column_names: Optional[List[str]] = None,
    sort_keys: Optional[SortScheme] = None,
    table_version_description: Optional[str] = None,
    table_version_properties: Optional[TableVersionProperties] = None,
    table_description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    supported_content_types: Optional[List[ContentType]] = None,
    *args,
    **kwargs,
) -> Stream:
    """
    Create a table version with an unreleased lifecycle state and an empty delta
    stream. Unless an individual catalog implementation requires otherwise,
    table versions may be schemaless and unpartitioned, or partitioned by a list
    of partition key names and types.

    Individual catalog implementations can have different rules governing
    partition/primary/sort-key support, name case-sensitivity, defaults, and
    support for referencing keys not defined in the schema. Please refer to the
    documentation of your catalog's implementation for further information.

    Some catalogs also support using Schemas to inform the data consistency
    checks run for each field. If supported, the schema can be used to enforce
    the following column-level data consistency policies at table load time:

    None: No consistency checks are run. May be mixed with the below two
    policies by specifying column names to pass through together with
    column names to coerce/validate.

    Coerce: Coerce fields to fit the schema whenever possible. An explicit
    subset of column names to coerce may optionally be specified.

    Validate: Raise an error for any fields that don't fit the schema. An
    explicit subset of column names to validate may optionally be specified.

    Returns the stream for the created table version.
    Raises an error if the given namespace does not exist.
    """
    catalog = _get_native_catalog(**kwargs)
    location = kwargs.get("location")
    case_sensitive_col_names = kwargs.get("case_sensitive_column_names") or True
    if not isinstance(case_sensitive_col_names, bool):
        err_msg = (
            f"unsupported `case_sensitive_column_names` param type: "
            f"`{type(case_sensitive_col_names)}`. "
            f"expected `case_sensitive_column_names` param type: `{bool}`"
        )
        raise TypeError(err_msg)

    # TODO: ensure catalog.create_table() is idempotent
    # TODO: get table and commit new metadata if table already exists?
    identifier = _to_identifier(namespace, table_name)
    iceberg_schema = SchemaMapper.unmap(schema)
    sort_order = SortSchemeMapper.unmap(
        obj=sort_keys,
        schema=iceberg_schema,
        case_sensitive=case_sensitive_col_names,
    )
    partition_spec = PartitionSchemeMapper.unmap(
        obj=partition_scheme,
        schema=iceberg_schema,
        case_sensitive=case_sensitive_col_names,
    )
    table = catalog.create_table(
        identifier=identifier,
        schema=iceberg_schema,
        location=location,
        partition_spec=partition_spec or UNPARTITIONED_PARTITION_SPEC,
        sort_order=sort_order or UNSORTED_SORT_ORDER,
        properties=table_properties or EMPTY_DICT,
    )
    logger.info(f"Created table: {table}")
    # no snapshot is committed on table creation, so return an undefined stream
    return Stream.of(locator=None, partition_scheme=None)


def update_table(
    namespace: str,
    table_name: str,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    new_table_name: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """
    Update table metadata describing the table versions it contains. By default,
    a table's properties are empty, and its description is equal to that given
    when its first table version was created. Raises an error if the given
    table does not exist.
    """
    raise NotImplementedError("update_table not implemented")


def update_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Schema] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    description: Optional[str] = None,
    properties: Optional[TableVersionProperties] = None,
    *args,
    **kwargs,
) -> None:
    """
    Update a table version. Notably, updating an unreleased table version's
    lifecycle state to 'active' telegraphs that it is ready for external
    consumption, and causes all calls made to consume/produce streams,
    partitions, or deltas from/to its parent table to automatically resolve to
    this table version by default (i.e. when the client does not explicitly
    specify a different table version). Raises an error if the given table
    version does not exist.
    """
    raise NotImplementedError("update_table_version not implemented")


def stage_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Stream:
    """
    Stages a new delta stream for the given table version. Resolves to the
    latest active table version if no table version is given. Returns the
    staged stream. Raises an error if the table version does not exist.
    """
    raise NotImplementedError("stage_stream not implemented")


def commit_stream(stream: Stream, *args, **kwargs) -> Stream:
    """
    Registers a delta stream with a target table version, replacing any
    previous stream registered for the same table version. Returns the
    committed stream.
    """
    raise NotImplementedError("commit_stream not implemented")


def delete_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """
    Deletes the delta stream currently registered with the given table version.
    Resolves to the latest active table version if no table version is given.
    Raises an error if the table version does not exist.
    """
    raise NotImplementedError("delete_stream not implemented")


def get_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Stream]:
    """
    Gets the most recently committed stream for the given table version and
    partition key values. Resolves to the latest active table version if no
    table version is given. Returns None if the table version does not exist.
    """
    catalog = _get_native_catalog(**kwargs)
    table = _try_load_iceberg_table(catalog, namespace, table_name)
    return _try_get_stream(
        table=table,
        table_version=table_version,
        stream_id=None,
        catalog_properties=catalog.properties,
    )


def stage_partition(
    stream: Stream, partition_values: Optional[List[Any]] = None, *args, **kwargs
) -> Partition:
    """
    Stages a new partition for the given stream and partition values. Returns
    the staged partition. If this partition will replace another partition
    with the same partition values, then it will have its previous partition ID
    set to the ID of the partition being replaced. Partition keys should not be
    specified for unpartitioned tables.
    """
    raise NotImplementedError("stage_partition not implemented")


def commit_partition(partition: Partition, *args, **kwargs) -> Partition:
    """
    Commits the given partition to its associated table version stream,
    replacing any previous partition registered for the same stream and
    partition values. Returns the registered partition. If the partition's
    previous delta stream position is specified, then the commit will
    be rejected if it does not match the actual previous stream position of
    the partition being replaced. If the partition's previous partition ID is
    specified, then the commit will be rejected if it does not match the actual
    ID of the partition being replaced.
    """
    raise NotImplementedError("commit_partition not implemented")


def delete_partition(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    partition_values: Optional[List[Any]] = None,
    *args,
    **kwargs,
) -> None:
    """
    Deletes the given partition from the specified table version. Resolves to
    the latest active table version if no table version is given. Partition
    values should not be specified for unpartitioned tables. Raises an error
    if the table version or partition does not exist.
    """
    raise NotImplementedError("delete_partition not implemented")


def get_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[List[Any]] = None,
    *args,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the most recently committed partition for the given stream locator and
    partition key values. Returns None if no partition has been committed for
    the given table version and/or partition key values. Partition values
    should not be specified for unpartitioned tables.
    """
    raise NotImplementedError("get_partition not implemented")


def stage_delta(
    data: Union[LocalTable, LocalDataset, DistributedDataset, Manifest],
    partition: Partition,
    delta_type: DeltaType = DeltaType.UPSERT,
    max_records_per_entry: Optional[int] = None,
    author: Optional[ManifestAuthor] = None,
    properties: Optional[DeltaProperties] = None,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> Delta:
    """
    Writes the given table to 1 or more S3 files. Returns an unregistered
    delta whose manifest entries point to the uploaded files. Applies any
    schema consistency policies configured for the parent table version.
    """
    raise NotImplementedError("stage_delta not implemented")


def commit_delta(delta: Delta, *args, **kwargs) -> Delta:
    """
    Registers a new delta with its associated target table version and
    partition. Returns the registered delta. If the delta's previous stream
    position is specified, then the commit will be rejected if it does not match
    the target partition's actual previous stream position. If the delta's
    stream position is specified, it must be greater than the latest stream
    position in the target partition.
    """
    raise NotImplementedError("commit_delta not implemented")


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """
    Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist.
    """
    catalog = _get_native_catalog(**kwargs)
    return _try_get_namespace(catalog, namespace)


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table namespace exists, False if not.
    """
    catalog = _get_native_catalog(**kwargs)
    return True if _try_get_namespace(catalog, namespace) else False


def get_table(namespace: str, table_name: str, *args, **kwargs) -> Optional[Table]:
    """
    Gets table metadata for the specified table. Returns None if the given
    table does not exist.
    """
    catalog = _get_native_catalog(**kwargs)
    table = _try_load_iceberg_table(catalog, namespace, table_name)
    return TableMapper.map(table)


def table_exists(namespace: str, table_name: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table exists, False if not.
    """
    catalog = _get_native_catalog(**kwargs)
    return True if _try_load_iceberg_table(catalog, namespace, table_name) else False


def get_table_version(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the specified table version. Returns None
    if the given table version does not exist.
    """
    catalog = _get_native_catalog(**kwargs)
    table = _try_load_iceberg_table(catalog, namespace, table_name)
    return _try_get_table_version(table, table_version, catalog.properties)


def get_latest_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest version of the specified table.
    Returns None if no table version exists for the given table.
    """
    catalog = _get_native_catalog(**kwargs)
    table = _try_load_iceberg_table(catalog, namespace, table_name)
    return _try_get_table_version(table, None, catalog.properties)


def get_latest_active_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest active version of the specified
    table. Returns None if no active table version exists for the given table.
    """
    return get_latest_table_version(namespace, table_name, **kwargs)


def get_table_version_column_names(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[List[str]]:
    """
    Gets a list of column names for the specified table version, or for the
    latest active table version if none is specified. The index of each
    column name returned represents its ordinal position in a delimited text
    file or other row-oriented content type files appended to the table.
    Returns None for schemaless tables. Raises an error if the table version
    does not exist.
    """
    raise NotImplementedError("get_table_version_column_names not implemented")


def get_table_version_schema(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Schema]:
    """
    Gets the schema for the specified table version, or for the latest active
    table version if none is specified. Returns None if the table version is
    schemaless. Raises an error if the table version does not exist.
    """
    raise NotImplementedError("get_table_version_schema not implemented")


def table_version_exists(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> bool:
    """
    Returns True if the given table version exists, False if not.
    """
    raise NotImplementedError("table_version_exists not implemented")
