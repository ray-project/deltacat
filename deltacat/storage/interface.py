from typing import Any, Callable, Dict, List, Optional, Union, Tuple

from deltacat.storage import (
    EntryParams,
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
    NamespaceProperties,
    Partition,
    PartitionLocator,
    PartitionScheme,
    PartitionValues,
    Schema,
    SortScheme,
    Stream,
    StreamFormat,
    StreamLocator,
    Table,
    TableProperties,
    TableVersion,
    TableVersionLocator,
    TableVersionProperties,
)
from deltacat.storage.model.manifest import Manifest
from deltacat.types.media import (
    ContentType,
    DistributedDatasetType,
    StorageType,
    TableType,
)
from deltacat.utils.common import ReadKwargsProvider


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """
    Lists a page of table namespaces. Namespaces are returned as list result
    items.
    """
    raise NotImplementedError("list_namespaces not implemented")


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


def list_streams(
    namespace: str,
    table_name: str,
    table_version: str,
    *args,
    **kwargs,
) -> ListResult[Stream]:
    """
    Lists a page of streams for the given table version.
    Raises an error if the table version does not exist.
    """
    raise NotImplementedError("list_streams not implemented")


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
    partition_values: Optional[PartitionValues] = None,
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
    partition_like: Union[Partition, PartitionLocator],
    first_stream_position: Optional[int] = None,
    last_stream_position: Optional[int] = None,
    ascending_order: bool = False,
    include_manifest: bool = False,
    *args,
    **kwargs,
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
    partition_values: Optional[PartitionValues] = None,
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
    partition_values: Optional[PartitionValues] = None,
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
    distributed_dataset_type: DistributedDatasetType = DistributedDatasetType.RAY_DATASET,
    *args,
    **kwargs,
) -> Union[LocalDataset, DistributedDataset]:  # type: ignore
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

    NOTE: The entry will be downloaded in the current node's memory.
    """
    raise NotImplementedError("download_delta_manifest_entry not implemented")


def get_delta_manifest(
    delta_like: Union[Delta, DeltaLocator], *args, **kwargs
) -> Manifest:
    """
    Get the manifest associated with the given delta or delta locator. This
    always retrieves the authoritative durable copy of the delta manifest, and
    never the local manifest defined for any input delta. Raises an error if
    the delta can't be found, or if it doesn't contain a manifest.
    """
    raise NotImplementedError("get_delta_manifest not implemented")


def create_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    *args,
    **kwargs,
) -> Namespace:
    """
    Creates a table namespace with the given name and properties. Returns
    the created namespace.
    """
    raise NotImplementedError("create_namespace not implemented")


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
    partition_scheme: Optional[PartitionScheme] = None,
    # TODO(pdames): rename to `sort_scheme`
    sort_keys: Optional[SortScheme] = None,
    table_version_description: Optional[str] = None,
    table_version_properties: Optional[TableVersionProperties] = None,
    table_description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    supported_content_types: Optional[List[ContentType]] = None,
    *args,
    **kwargs,
) -> Tuple[Optional[Table], TableVersion, Stream]:
    """
    Create a table version with an unreleased lifecycle state and an empty delta
    stream. Table versions may be schemaless and unpartitioned to improve write
    performance, or have their writes governed by a schema and partition scheme
    to improve data consistency and read performance.

    Returns a tuple containing the created/updated table, table version, and
    stream (respectively).

    Raises an error if the given namespace does not exist.
    """
    raise NotImplementedError("create_table_version not implemented")


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
    description: Optional[str] = None,
    properties: Optional[TableVersionProperties] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    # TODO(pdames): rename to `sort_scheme`
    sort_keys: Optional[SortScheme] = None,
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
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    **kwargs,
) -> Stream:
    """
    Stages a new delta stream for the given table version. Resolves to the
    latest active table version if no table version is given. Resolves to the
    DeltaCAT stream format if no stream format is given. If this stream
    will replace another stream with the same format and scheme, then it will
    have its previous stream ID set to the ID of the stream being replaced.
    Returns the staged stream. Raises an error if the table version does not
    exist.
    """
    raise NotImplementedError("stage_stream not implemented")


def commit_stream(
    stream: Stream,
    *args,
    **kwargs,
) -> Stream:
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
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    **kwargs,
) -> None:
    """
    Deletes the delta stream currently registered with the given table version.
    Resolves to the latest active table version if no table version is given.
    Resolves to the deltacat stream format if no stream format is given.
    Raises an error if the stream does not exist.
    """
    raise NotImplementedError("delete_stream not implemented")


def delete_table(
    namespace: str,
    name: str,
    purge: bool = False,
    *args,
    **kwargs,
) -> None:
    """
    Drops the given table and all its contents (table versions, streams, partitions,
    and deltas). If purge is True, also removes all data files associated with the table.
    Raises an error if the given table does not exist.
    """
    raise NotImplementedError("delete_table not implemented")


def delete_namespace(
    namespace: str,
    purge: bool = False,
    *args,
    **kwargs,
) -> None:
    """
    Drops a table namespace and all its contents. If purge is True, then all
    tables, table versions, and deltas will be deleted. Otherwise, the namespace
    will be dropped only if it is empty. Raises an error if the given namespace
    does not exist.
    """
    raise NotImplementedError("drop_namespace not implemented")


def get_stream_by_id(
    table_version_locator: TableVersionLocator,
    stream_id: str,
    *args,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the stream for the given table version locator and stream ID.
    Returns None if the stream does not exist. Raises an error if the given
    table version locator does not exist.
    """
    raise NotImplementedError("get_stream_by_id not implemented")


def get_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    **kwargs,
) -> Optional[Stream]:
    """
    Gets the most recently committed stream for the given table version.
    Resolves to the latest active table version if no table version is given.
    Resolves to the deltacat stream format if no stream format is given.
    Returns None if the table version or stream format does not exist.
    """
    raise NotImplementedError("get_stream not implemented")


def stream_exists(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    **kwargs,
) -> bool:
    """
    Returns True if the given Stream exists, False if not.
    Resolves to the latest active table version if no table version is given.
    Resolves to the DeltaCAT stream format if no stream format is given.
    Returns None if the table version or stream format does not exist.
    """
    raise NotImplementedError("stream_exists not implemented")


def stage_partition(
    stream: Stream,
    partition_values: Optional[PartitionValues] = None,
    partition_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> Partition:
    """
    Stages a new partition for the given stream and partition values. Returns
    the staged partition. If this partition will replace another partition
    with the same partition values and scheme, then it will have its previous
    partition ID set to the ID of the partition being replaced. Partition values
    should not be specified for unpartitioned tables.

    The partition_values must represent the results of transforms in a partition
    spec specified in the stream.
    """
    raise NotImplementedError("stage_partition not implemented")


def commit_partition(
    partition: Partition,
    previous_partition: Optional[Partition] = None,
    *args,
    **kwargs,
) -> Partition:
    """
    Commits the staged partition to its associated table version stream,
    replacing any previous partition registered for the same stream and
    partition values.

    If previous partition is given then it will be replaced with its deltas
    prepended to the new partition being committed. Otherwise the latest
    committed partition with the same keys and partition scheme ID will be
    retrieved.

    Returns the registered partition. If the partition's
    previous delta stream position is specified, then the commit will
    be rejected if it does not match the actual previous stream position of
    the partition being replaced. If the partition's previous partition ID is
    specified, then the commit will be rejected if it does not match the actual
    ID of the partition being replaced.
    """
    raise NotImplementedError("commit_partition not implemented")


def delete_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[PartitionValues] = None,
    partition_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    """
    Deletes the given partition from the specified stream. Partition
    values should not be specified for unpartitioned tables. Raises an error
    if the partition does not exist.
    """
    raise NotImplementedError("delete_partition not implemented")


def get_partition_by_id(
    stream_locator: StreamLocator,
    partition_id: str,
    *args,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the partition for the given stream locator and partition ID.
    Returns None if the partition does not exist. Raises an error if the
    given stream locator does not exist.
    """
    raise NotImplementedError("get_partition_by_id not implemented")


def get_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[PartitionValues] = None,
    partition_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Partition]:
    """
    Gets the most recently committed partition for the given stream locator and
    partition key values. Returns None if no partition has been committed for
    the given table version and/or partition key values. Partition values
    should not be specified for unpartitioned tables. Partition scheme ID
    resolves to the table version's current partition scheme by default.
    Raises an error if the given stream locator does not exist.
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
    entry_params: Optional[EntryParams] = None,
    *args,
    **kwargs,
) -> Delta:
    """
    Writes the given table to 1 or more S3 files. Returns an unregistered
    delta whose manifest entries point to the uploaded files. Applies any
    schema consistency policies configured for the parent table version.

    The partition spec will be used to split the input table into
    multiple files. Optionally, partition_values can be provided to avoid
    this method to recompute partition_values from the provided data.

    Raises an error if the provided data does not conform to a unique ordered
    list of partition_values
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
    raise NotImplementedError("get_namespace not implemented")


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table namespace exists, False if not.
    """
    raise NotImplementedError("namespace_exists not implemented")


def get_table(namespace: str, table_name: str, *args, **kwargs) -> Optional[Table]:
    """
    Gets table metadata for the specified table. Returns None if the given
    table does not exist.
    """
    raise NotImplementedError("get_table not implemented")


def table_exists(namespace: str, table_name: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table exists, False if not.
    """
    raise NotImplementedError("table_exists not implemented")


def get_table_version(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the specified table version. Returns None
    if the given table version does not exist.
    """
    raise NotImplementedError("get_table_version not implemented")


def get_latest_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest version of the specified table.
    Returns None if no table version exists for the given table.
    """
    raise NotImplementedError("get_latest_table_version not implemented")


def get_latest_active_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest active version of the specified
    table. Returns None if no active table version exists for the given table.
    """
    raise NotImplementedError("get_latest_active_table_version not implemented")


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


def can_categorize(e: BaseException, *args, **kwargs) -> bool:
    """
    Return whether input error is from storage implementation layer.
    """
    raise NotImplementedError


def raise_categorized_error(e: BaseException, *args, **kwargs):
    """
    Raise and handle storage implementation layer specific errors.
    """
    raise NotImplementedError
