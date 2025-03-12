import uuid

from typing import Any, Callable, Dict, List, Optional, Union, Tuple

from deltacat.catalog import get_catalog_properties
from deltacat.constants import DEFAULT_TABLE_VERSION
from deltacat.exceptions import TableNotFoundError
from deltacat.storage.model.manifest import (
    EntryParams,
    ManifestAuthor,
)
from deltacat.storage.model.delta import (
    Delta,
    DeltaLocator,
    DeltaProperties,
    DeltaType,
)
from deltacat.storage.model.types import (
    CommitState,
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    TransactionType,
    TransactionOperationType,
    StreamFormat,
)
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import (
    Namespace,
    NamespaceLocator,
    NamespaceProperties,
)
from deltacat.storage.model.partition import (
    Partition,
    PartitionLocator,
    PartitionScheme,
    PartitionValues,
    UNPARTITIONED_SCHEME_ID,
    PartitionLocatorAlias,
)
from deltacat.storage.model.schema import (
    Schema,
)
from deltacat.storage.model.sort_key import (
    SortScheme,
)
from deltacat.storage.model.stream import (
    Stream,
    StreamLocator,
)
from deltacat.storage.model.table import (
    Table,
    TableProperties,
    TableLocator,
)
from deltacat.storage.model.table_version import (
    TableVersion,
    TableVersionProperties,
    TableVersionLocator,
)
from deltacat.storage.model.metafile import (
    Metafile,
)
from deltacat.storage.model.transaction import (
    TransactionOperation,
    Transaction,
    TransactionOperationList,
)
from deltacat.storage.model.manifest import Manifest
from deltacat.types.media import (
    ContentType,
    DistributedDatasetType,
    StorageType,
    TableType,
)
from deltacat.utils.common import ReadKwargsProvider


def _list(
    metafile: Metafile,
    txn_op_type: TransactionOperationType,
    *args,
    **kwargs,
) -> ListResult[Metafile]:
    catalog_properties = get_catalog_properties(**kwargs)
    limit = kwargs.get("limit") or None
    transaction = Transaction.of(
        txn_type=TransactionType.READ,
        txn_operations=[
            TransactionOperation.of(
                operation_type=txn_op_type,
                dest_metafile=metafile,
                read_limit=limit,
            )
        ],
    )
    list_results_per_op = transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return list_results_per_op[0]


def _latest(
    metafile: Metafile,
    *args,
    **kwargs,
) -> Optional[Metafile]:
    list_results = _list(
        *args,
        metafile=metafile,
        txn_op_type=TransactionOperationType.READ_LATEST,
        **kwargs,
    )
    results = list_results.all_items()
    return results[0] if results else None


def _exists(
    metafile: Metafile,
    *args,
    **kwargs,
) -> Optional[Metafile]:
    list_results = _list(
        *args,
        metafile=metafile,
        txn_op_type=TransactionOperationType.READ_EXISTS,
        **kwargs,
    )
    results = list_results.all_items()
    return True if results else False


def _resolve_partition_locator_alias(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    partition_values: Optional[PartitionValues] = None,
    partition_scheme_id: Optional[str] = None,
    *args,
    **kwargs,
) -> PartitionLocatorAlias:
    # TODO(pdames): A read shouldn't initiate N transactions that
    #  read against different catalog snapshots. To resolve this, add
    #  new "start", "step", and "end" methods to Transaction that
    #  support starting a txn, defining and executing a txn op, retrieve
    #  its results, then define and execute the next txn op. When
    #  stepping through a transaction its txn heartbeat timeout should
    #  be set manually.
    partition_locator = None
    if not partition_values:
        partition_scheme_id = UNPARTITIONED_SCHEME_ID
    elif not partition_scheme_id:
        # resolve latest partition scheme from the current
        # revision of its `deltacat` stream
        stream = get_stream(
            *args,
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            **kwargs,
        )
        if not stream:
            raise ValueError(
                f"Failed to resolve latest partition scheme for "
                f"`{namespace}.{table_name}` at table version "
                f"`{table_version or 'latest'}` (no stream found)."
            )
        partition_locator = PartitionLocator.of(
            stream_locator=stream.locator,
            partition_values=partition_values,
            partition_id=None,
        )
        partition_scheme_id = stream.partition_scheme.id
    if not partition_locator:
        partition_locator = PartitionLocator.at(
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            stream_id=None,
            stream_format=StreamFormat.DELTACAT,
            partition_values=partition_values,
            partition_id=None,
        )
    partition = Partition.of(
        locator=partition_locator,
        schema=None,
        content_types=None,
        partition_scheme_id=partition_scheme_id,
    )
    return partition.locator_alias


def _resolve_latest_active_table_version_id(
    namespace: str,
    table_name: str,
    fail_if_no_active_table_version: True,
    *args,
    **kwargs,
) -> Optional[str]:
    table = get_table(
        *args,
        namespace=namespace,
        table_name=table_name,
        **kwargs,
    )
    if not table:
        raise ValueError(f"Table does not exist: {namespace}.{table_name}")
    if fail_if_no_active_table_version and not table.latest_active_table_version:
        raise ValueError(f"Table has no active table version: {namespace}.{table_name}")
    return table.latest_active_table_version


def _resolve_latest_table_version_id(
    namespace: str,
    table_name: str,
    fail_if_no_active_table_version: True,
    *args,
    **kwargs,
) -> Optional[str]:
    table = get_table(
        *args,
        namespace=namespace,
        table_name=table_name,
        **kwargs,
    )
    if not table:
        raise ValueError(f"Table does not exist: {namespace}.{table_name}")
    if fail_if_no_active_table_version and not table.latest_table_version:
        raise ValueError(f"Table has no table version: {namespace}.{table_name}")
    return table.latest_table_version


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """
    Lists a page of table namespaces. Namespaces are returned as list result
    items.
    """
    return _list(
        *args,
        metafile=Namespace.of(NamespaceLocator.of("placeholder")),
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        **kwargs,
    )


def list_tables(namespace: str, *args, **kwargs) -> ListResult[Table]:
    """
    Lists a page of tables for the given table namespace. Tables are returned as
    list result items. Raises an error if the given namespace does not exist.
    """
    locator = TableLocator.at(namespace=namespace, table_name="placeholder")
    return _list(
        *args,
        metafile=Table.of(locator=locator),
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        **kwargs,
    )


def list_table_versions(
    namespace: str,
    table_name: str,
    *args,
    **kwargs,
) -> ListResult[TableVersion]:
    """
    Lists a page of table versions for the given table. Table versions are
    returned as list result items. Raises an error if the given table does not
    exist.
    """
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version="placeholder.0",
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=None,
    )
    return _list(
        *args,
        metafile=table_version,
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        **kwargs,
    )


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
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id="placeholder",
        stream_format=None,
    )
    stream = Stream.of(
        locator=locator,
        partition_scheme=None,
    )
    return _list(
        stream,
        TransactionOperationType.READ_SIBLINGS,
        *args,
        **kwargs,
    )


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
    locator = PartitionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=None,
        stream_format=StreamFormat.DELTACAT,
        partition_values=["placeholder"],
        partition_id="placeholder",
    )
    partition = Partition.of(
        locator=locator,
        schema=None,
        content_types=None,
    )
    return _list(
        *args,
        metafile=partition,
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        **kwargs,
    )


def list_stream_partitions(stream: Stream, *args, **kwargs) -> ListResult[Partition]:
    """
    Lists all partitions committed to the given stream.
    """
    if stream.stream_format != StreamFormat.DELTACAT:
        raise ValueError(
            f"Unsupported stream format: {stream.stream_format}"
            f"Expected stream format: {StreamFormat.DELTACAT}"
        )
    locator = PartitionLocator.of(
        stream_locator=stream.locator,
        partition_values=["placeholder"],
        partition_id="placeholder",
    )
    partition = Partition.of(
        locator=locator,
        schema=None,
        content_types=None,
    )
    return _list(
        *args,
        metafile=partition,
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        **kwargs,
    )


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
    # TODO(pdames): Delta listing should ideally either use an efficient
    #  range-limited dir listing of partition children between start and end
    #  positions, or should traverse using Partition.stream_position (to
    #  resolve last stream position) and Delta.previous_stream_position
    #  (down to first stream position).
    partition_locator_alias = _resolve_partition_locator_alias(
        *args,
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        **kwargs,
    )
    locator = DeltaLocator.of(locator=partition_locator_alias)
    delta = Delta.of(
        locator=locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    all_deltas_list_result: ListResult[Delta] = _list(
        *args,
        metafile=delta,
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        **kwargs,
    )
    all_deltas = all_deltas_list_result.all_items()
    filtered_deltas = [
        delta
        for delta in all_deltas
        if first_stream_position <= delta.stream_position <= last_stream_position
    ]
    if ascending_order:
        filtered_deltas.reverse()
    return filtered_deltas


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
    # TODO(pdames): Delta listing should ideally either use an efficient
    #  range-limited dir listing of partition children between start and end
    #  positions, or should traverse using Partition.stream_position (to
    #  resolve last stream position) and Delta.previous_stream_position
    #  (down to first stream position).
    locator = DeltaLocator.of(
        partition_locator=partition_like
        if isinstance(partition_like, PartitionLocator)
        else partition_like.locator,
        stream_position=None,
    )
    delta = Delta.of(
        locator=locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    all_deltas_list_result: ListResult[Delta] = _list(
        *args,
        metafile=delta,
        txn_op_type=TransactionOperationType.READ_SIBLINGS,
        **kwargs,
    )
    all_deltas = all_deltas_list_result.all_items()
    filtered_deltas = [
        delta
        for delta in all_deltas
        if first_stream_position <= delta.stream_position <= last_stream_position
    ]
    if ascending_order:
        filtered_deltas.reverse()
    return filtered_deltas


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
    # TODO(pdames): Honor `include_manifest` param.
    partition_locator_alias = _resolve_partition_locator_alias(
        *args,
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        **kwargs,
    )
    locator = DeltaLocator.of(
        locator=partition_locator_alias,
        stream_position=stream_position,
    )
    delta = Delta.of(
        locator=locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    return _latest(
        *args,
        metafile=delta,
        **kwargs,
    )


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
    # TODO(pdames): Wrap this method in 1 single txn.
    stream = get_stream(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    partition = get_partition(
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
    )
    locator = DeltaLocator.of(
        locator=partition.locator,
        stream_position=partition.stream_position,
    )
    delta = Delta.of(
        locator=locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    return _latest(
        *args,
        metafile=delta,
        **kwargs,
    )


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
    delta_like: Union[Delta, DeltaLocator],
    *args,
    **kwargs,
) -> Manifest:
    """
    Get the manifest associated with the given delta or delta locator. This
    always retrieves the authoritative durable copy of the delta manifest, and
    never the local manifest defined for any input delta. Raises an error if
    the delta can't be found, or if it doesn't contain a manifest.
    """
    if isinstance(delta_like, Delta):
        delta_locator = delta_like.locator
    elif isinstance(delta_like, DeltaLocator):
        delta_locator = delta_like
    else:
        raise ValueError(
            f"Expected delta or delta locator, but got: {type(delta_like)}"
        )
    delta = Delta.of(
        locator=delta_locator,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
    )
    latest_delta = _latest(
        metafile=delta,
        *args,
        **kwargs,
    )
    if not latest_delta or not latest_delta.manifest:
        raise ValueError(f"No manifest found for delta: {delta_locator}")
    return latest_delta.manifest


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
    namespace = Namespace.of(
        locator=NamespaceLocator.of(namespace=namespace),
        properties=properties,
    )
    transaction = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=namespace,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return namespace


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
    # TODO(pdames): Wrap get & update within a single txn.
    old_namespace = get_namespace(
        *args,
        namespace=namespace,
        **kwargs,
    )
    new_namespace: Namespace = Metafile.update_for(old_namespace)
    new_namespace.namespace = namespace
    new_namespace.properties = properties
    transaction = Transaction.of(
        txn_type=TransactionType.ALTER,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_namespace,
                src_metafile=old_namespace,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return namespace


def create_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    schema: Optional[Schema] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    sort_keys: Optional[SortScheme] = None,
    table_version_description: Optional[str] = None,
    table_version_properties: Optional[TableVersionProperties] = None,
    table_description: Optional[str] = None,
    table_properties: Optional[TableProperties] = None,
    supported_content_types: Optional[List[ContentType]] = None,
    *args,
    **kwargs,
) -> Tuple[Table, TableVersion, Stream]:
    """
    Create a table version with an unreleased lifecycle state and an empty delta
    stream. Table versions may be schemaless and unpartitioned to improve write
    performance, or have their writes governed by a schema and partition scheme
    to improve data consistency and read performance.

    Returns a tuple containing the created/updated table, table version, and
    stream (respectively).

    Raises an error if the given namespace does not exist.
    """
    if not namespace_exists(
        *args,
        namespace=namespace,
        **kwargs,
    ):
        raise ValueError(f"Namespace {namespace} does not exist")
    # check if a parent table and/or previous table version already exist
    prev_table_version = None
    prev_table = get_table(
        *args,
        namespace=namespace,
        table_name=table_name,
        **kwargs,
    )
    if not prev_table:
        # no parent table exists, so we'll create it in this transaction
        txn_type = TransactionType.APPEND
        table_txn_op_type = TransactionOperationType.CREATE
        prev_table = None
        new_table = Table.of(
            locator=TableLocator.at(namespace=namespace, table_name=table_name),
        )
        table_version = table_version or DEFAULT_TABLE_VERSION
    else:
        # the parent table exists, so we'll update it in this transaction
        txn_type = TransactionType.ALTER
        table_txn_op_type = TransactionOperationType.UPDATE
        new_table: Table = Metafile.update_for(prev_table)
        prev_table_version = prev_table.latest_table_version
        if not table_version:
            # generate the next table version ID
            table_version = TableVersion.next_version(prev_table_version)
        else:
            # ensure that the given table version number matches expectations
            expected_table_version = TableVersion.next_version(prev_table_version)
            _, version_number = TableVersion.parse_table_version(
                table_version,
            )
            _, expected_version_number = TableVersion.parse_table_version(
                expected_table_version,
            )
            if version_number != expected_version_number:
                raise ValueError(
                    f"Expected to create table version "
                    f"{expected_version_number} but found {version_number}.",
                )
    new_table.description = table_description or table_version_description
    new_table.properties = table_properties
    new_table.latest_table_version = table_version
    catalog_properties = get_catalog_properties(**kwargs)
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=schema,
        partition_scheme=partition_scheme,
        description=table_version_description,
        properties=table_version_properties,
        content_types=supported_content_types,
        sort_scheme=sort_keys,
        watermark=None,
        lifecycle_state=LifecycleState.CREATED,
        schemas=[schema] if schema else None,
        partition_schemes=[partition_scheme] if partition_scheme else None,
        sort_schemes=[sort_keys] if sort_keys else None,
        previous_table_version=prev_table_version,
    )
    # create the table version's default deltacat stream in this transaction
    stream_locator = StreamLocator.of(
        table_version_locator=locator,
        stream_id=str(uuid.uuid4()),
        stream_format=StreamFormat.DELTACAT,
    )
    stream = Stream.of(
        locator=stream_locator,
        partition_scheme=partition_scheme,
        state=CommitState.COMMITTED,
        previous_stream_id=None,
        watermark=None,
    )
    transaction = Transaction.of(
        txn_type=txn_type,
        txn_operations=[
            TransactionOperation.of(
                operation_type=table_txn_op_type,
                dest_metafile=new_table,
                src_metafile=prev_table,
            ),
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=table_version,
            ),
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=stream,
            ),
        ],
    )
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return new_table, table_version, stream


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
    old_table = get_table(
        *args,
        namespace=namespace,
        table_name=table_name,
        **kwargs,
    )
    if not old_table:
        raise TableNotFoundError(f"Table `{namespace}.{table_name}` does not exist.")
    new_table: Table = Metafile.update_for(old_table)
    new_table.description = description or old_table.description
    new_table.properties = properties or old_table.properties
    new_table.table_name = new_table_name or old_table.table_name
    transaction = Transaction.of(
        txn_type=TransactionType.ALTER,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_table,
                src_metafile=old_table,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )


def update_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Schema] = None,
    description: Optional[str] = None,
    properties: Optional[TableVersionProperties] = None,
    partition_scheme: Optional[PartitionScheme] = None,
    sort_keys: Optional[SortScheme] = None,
    *args,
    **kwargs,
) -> None:
    """
    Update a table version. Notably, updating an unreleased table version's
    lifecycle state to 'active' telegraphs that it is ready for external
    consumption, and causes all calls made to consume/produce streams,
    partitions, or deltas from/to its parent table to automatically resolve to
    this table version by default (i.e., when the client does not explicitly
    specify a different table version). Raises an error if the given table
    version does not exist.
    """
    # TODO(pdames): Wrap get & update within a single txn.
    old_table_version = get_table_version(
        *args,
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        **kwargs,
    )
    if not old_table_version:
        raise ValueError(
            f"Table version `{table_version}` does not exist for "
            f"table `{namespace}.{table_name}`."
        )
    new_table_version: TableVersion = Metafile.update_for(old_table_version)
    new_table_version.state = lifecycle_state or old_table_version.state
    # TODO(pdames): Use schema patch to check for backwards incompatible changes.
    #  By default, backwards incompatible changes should be pushed to a new
    #  table version unless the user explicitly forces the update to this
    #  table version (i.e., at the cost of potentially breaking consumers).
    update_schema = schema and not schema.equivalent_to(
        old_table_version.schema,
        True,
    )
    if update_schema and schema.id in [s.id for s in old_table_version.schemas]:
        raise ValueError(
            f"Schema ID `{schema.id}` already exists in "
            f"table version `{table_version}`."
        )
    new_table_version.schema = schema if update_schema else old_table_version.schema
    new_table_version.schemas = (
        old_table_version.schemas + [schema]
        if update_schema
        else old_table_version.schemas
    )
    new_table_version.description = (
        description if description is not None else old_table_version.description
    )
    new_table_version.properties = (
        properties if properties is not None else old_table_version.properties
    )
    new_table_version.partition_scheme = (
        partition_scheme or old_table_version.partition_scheme
    )
    # TODO(pdames): Check for backwards incompatible partition scheme changes.
    update_partition_scheme = partition_scheme and not partition_scheme.equivalent_to(
        old_table_version.partition_scheme,
        True,
    )
    if update_partition_scheme and partition_scheme.id in [
        ps.id for ps in old_table_version.partition_schemes
    ]:
        raise ValueError(
            f"Partition scheme ID `{partition_scheme.id}` already exists in "
            f"table version `{table_version}`."
        )
    new_table_version.partition_schemes = (
        old_table_version.partition_schemes + [partition_scheme]
        if update_partition_scheme
        else old_table_version.partition_schemes
    )
    # TODO(pdames): Check for backwards incompatible sort scheme changes.
    update_sort_scheme = sort_keys and not sort_keys.equivalent_to(
        old_table_version.sort_scheme,
        True,
    )
    if update_sort_scheme and sort_keys.id in [
        sk.id for sk in old_table_version.sort_schemes
    ]:
        raise ValueError(
            f"Sort scheme ID `{sort_keys.id}` already exists in "
            f"table version `{table_version}`."
        )
    new_table_version.sort_scheme = sort_keys or old_table_version.sort_scheme
    new_table_version.sort_schemes = (
        old_table_version.sort_schemes + [sort_keys]
        if update_sort_scheme
        else old_table_version.sort_schemes
    )
    old_table = get_table(
        *args,
        namespace=namespace,
        table_name=table_name,
        **kwargs,
    )
    txn_operations = []
    if (
        lifecycle_state == LifecycleState.ACTIVE
        and old_table_version.state != LifecycleState.ACTIVE
    ):
        _, old_version_number = (
            TableVersion.parse_table_version(
                old_table.latest_active_table_version,
            )
            if old_table.latest_active_table_version
            else (None, None)
        )
        _, new_version_number = TableVersion.parse_table_version(table_version)
        if old_version_number is None or old_version_number < new_version_number:
            # update the table's latest table version
            new_table: Table = Metafile.update_for(old_table)
            new_table.latest_active_table_version = table_version
            txn_operations.append(
                TransactionOperation.of(
                    operation_type=TransactionOperationType.UPDATE,
                    dest_metafile=new_table,
                    src_metafile=old_table,
                )
            )
    txn_operations.append(
        TransactionOperation.of(
            operation_type=TransactionOperationType.UPDATE,
            dest_metafile=new_table_version,
            src_metafile=old_table_version,
        ),
    )
    # TODO(pdames): Push changes down to non-deltacat streams via sync module.
    #   Also copy sort scheme changes down to deltacat child stream?
    if partition_scheme:
        old_stream = get_stream(
            *args,
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            **kwargs,
        )
        new_stream: Stream = Metafile.update_for(old_stream)
        new_stream.partition_scheme = partition_scheme
        txn_operations.append(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_stream,
                src_metafile=old_stream,
            )
        )
    transaction = Transaction.of(
        txn_type=TransactionType.ALTER,
        txn_operations=txn_operations,
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )


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
    # TODO(pdames): Support retrieving previously staged streams by ID.
    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            *args,
            namespace=namespace,
            table_name=table_name,
            **kwargs,
        )
    table_version_meta = get_table_version(
        *args,
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        **kwargs,
    )
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=str(uuid.uuid4()),
        stream_format=stream_format or StreamFormat.DELTACAT,
    )
    stream = Stream.of(
        locator=locator,
        partition_scheme=table_version_meta.partition_scheme,
        state=CommitState.STAGED,
        previous_stream_id=None,
        watermark=None,
    )
    prev_stream = get_stream(
        *args,
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        stream_format=stream.stream_format,
        **kwargs,
    )
    if prev_stream:
        if prev_stream.stream_id == stream.stream_id:
            raise ValueError(
                f"Stream to stage has the same ID as existing stream: {prev_stream}."
            )
        stream.previous_stream_id = prev_stream.stream_id
    transaction = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=stream,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return stream


def commit_stream(
    stream: Stream,
    *args,
    **kwargs,
) -> Stream:
    """
    Registers a staged delta stream with a target table version, replacing any
    previous stream registered for the same table version. Returns the
    committed stream.
    """
    if not stream.stream_id:
        raise ValueError("Stream ID to commit must be set to a staged stream ID.")
    if not stream.table_version_locator:
        raise ValueError(
            "Stream to commit must have its table version locator "
            "set to the parent of its staged stream ID."
        )
    prev_staged_stream = get_stream_by_id(
        *args,
        table_version_locator=stream.table_version_locator,
        stream_id=stream.stream_id,
        **kwargs,
    )
    if not prev_staged_stream:
        raise ValueError(
            f"Stream at table version {stream.table_version_locator} with ID "
            f"{stream.stream_id} not found."
        )
    if prev_staged_stream.state != CommitState.STAGED:
        raise ValueError(
            f"Expected to find a `{CommitState.STAGED}` stream at table version "
            f"{stream.table_version_locator} with ID {stream.stream_id},"
            f"but found a `{prev_staged_stream.state}` partition."
        )
    if not prev_staged_stream:
        raise ValueError(
            f"Stream at table_version {stream.table_version_locator} with ID "
            f"{stream.stream_id} not found."
        )
    if prev_staged_stream.state != CommitState.STAGED:
        raise ValueError(
            f"Expected to find a `{CommitState.STAGED}` stream at table version "
            f"{stream.table_version_locator} with ID {stream.stream_id},"
            f"but found a `{prev_staged_stream.state}` stream."
        )
    stream: Stream = Metafile.update_for(prev_staged_stream)
    stream.state = CommitState.COMMITTED
    prev_committed_stream = get_stream(
        *args,
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        stream_format=stream.stream_format,
        **kwargs,
    )
    # the first transaction operation updates the staged stream commit state
    txn_type = TransactionType.ALTER
    txn_ops = [
        TransactionOperation.of(
            operation_type=TransactionOperationType.UPDATE,
            dest_metafile=stream,
            src_metafile=prev_staged_stream,
        )
    ]
    if prev_committed_stream:
        if prev_committed_stream.stream_id != stream.previous_stream_id:
            raise ValueError(
                f"Previous stream ID mismatch Expected "
                f"{stream.previous_stream_id} but found "
                f"{prev_committed_stream.stream_id}."
            )
        if prev_committed_stream.stream_id == stream.stream_id:
            raise ValueError(
                f"Stream to commit has the same ID as existing stream: {prev_committed_stream}."
            )
        # there's a previously committed stream, so update the transaction
        # type to overwrite the previously committed stream, and add another
        # transaction operation to replace it with the staged stream
        txn_type = TransactionType.OVERWRITE
        txn_ops.append(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=stream,
                src_metafile=prev_committed_stream,
            )
        )
    transaction = Transaction.of(
        txn_type=txn_type,
        txn_operations=txn_ops,
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return stream


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
    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            *args,
            namespace=namespace,
            table_name=table_name,
            **kwargs,
        )
    stream_to_delete = get_stream(
        *args,
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_format=stream_format,
        **kwargs,
    )
    if not stream_to_delete:
        raise ValueError(
            f"Stream to delete not found: {namespace}.{table_name}"
            f".{table_version}.{stream_format}."
        )
    else:
        stream_to_delete.state = CommitState.DEPRECATED
    transaction = Transaction.of(
        txn_type=TransactionType.DELETE,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=stream_to_delete,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )


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

    TODO: Honor purge once garbage collection is implemented.
    """
    table: Optional[Table] = get_table(
        *args,
        namespace=namespace,
        table_name=name,
        **kwargs,
    )

    if not table:
        raise TableNotFoundError(f"Table `{namespace}.{name}` does not exist.")

    transaction = Transaction.of(
        txn_type=TransactionType.DELETE,
        txn_operations=TransactionOperationList.of(
            [
                TransactionOperation.of(
                    operation_type=TransactionOperationType.DELETE,
                    dest_metafile=table,
                )
            ]
        ),
    )

    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )


def delete_namespace(
    namespace: str,
    purge: bool = False,
    *args,
    **kwargs,
) -> None:
    """
    Drops the given table namespace and all its contents. Raises an error if the
    given namespace does not exist.
    """
    namespace: Optional[Namespace] = get_namespace(
        *args,
        namespace=namespace,
        **kwargs,
    )

    if not namespace:
        raise ValueError(f"Namespace `{namespace}` does not exist.")

    transaction = Transaction.of(
        txn_type=TransactionType.DELETE,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=namespace,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )


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
    locator = StreamLocator.of(
        table_version_locator=table_version_locator,
        stream_id=stream_id,
        stream_format=None,
    )
    return _latest(
        *args,
        metafile=Stream.of(locator=locator, partition_scheme=None),
        **kwargs,
    )


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
    Resolves to the DeltaCAT stream format if no stream format is given.
    Returns None if the table version or stream format does not exist.
    """
    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            *args,
            namespace=namespace,
            table_name=table_name,
            fail_if_no_active_table_version=False,
            **kwargs,
        )
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=None,
        stream_format=stream_format,
    )
    return _latest(
        *args,
        metafile=Stream.of(
            locator=locator,
            partition_scheme=None,
            state=CommitState.COMMITTED,
        ),
        **kwargs,
    )


def stream_exists(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_format: StreamFormat = StreamFormat.DELTACAT,
    *args,
    **kwargs,
) -> Optional[Stream]:
    """
    Returns True if the given Stream exists, False if not.
    Resolves to the latest active table version if no table version is given.
    Resolves to the DeltaCAT stream format if no stream format is given.
    Returns None if the table version or stream format does not exist.
    """
    if not table_version:
        table_version = _resolve_latest_active_table_version_id(
            *args,
            namespace=namespace,
            table_name=table_name,
            fail_if_no_active_table_version=False,
            **kwargs,
        )
    locator = StreamLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=None,
        stream_format=stream_format,
    )
    return _exists(
        *args,
        metafile=Stream.of(
            locator=locator,
            partition_scheme=None,
            state=CommitState.COMMITTED,
        ),
        **kwargs,
    )


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
    # TODO(pdames): Cache last retrieved metafile revisions in memory to resolve
    #   potentially high cost of staging many partitions.
    table_version = get_table_version(
        *args,
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        **kwargs,
    )
    if not table_version:
        raise ValueError(
            f"Table version not found: {stream.namespace}.{stream.table_name}."
            f"{stream.table_version}."
        )
    if not table_version.partition_schemes or partition_scheme_id not in [
        ps.id for ps in table_version.partition_schemes
    ]:
        raise ValueError(
            f"Invalid partition scheme ID `{partition_scheme_id}` (not found "
            f"in parent table version `{stream.namespace}.{stream.table_name}"
            f".{table_version.table_version}` partition scheme IDs)."
        )
    if stream.partition_scheme.id not in table_version.partition_schemes:
        # this should never happen, but just in case
        raise ValueError(
            f"Invalid stream partition scheme ID `{stream.partition_scheme.id}`"
            f"in parent table version `{stream.namespace}.{stream.table_name}"
            f".{table_version.table_version}` partition scheme IDs)."
        )
    locator = PartitionLocator.of(
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_id=str(uuid.uuid4()),
    )
    partition = Partition.of(
        locator=locator,
        schema=table_version.schema,
        content_types=table_version.content_types,
        state=CommitState.STAGED,
        previous_stream_position=None,
        partition_values=partition_values,
        previous_partition_id=None,
        stream_position=None,
        partition_scheme_id=partition_scheme_id,
    )
    prev_partition = get_partition(
        *args,
        stream_locator=stream.locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        **kwargs,
    )
    if prev_partition:
        if prev_partition.partition_id == partition.partition_id:
            raise ValueError(
                f"Partition to stage has the same ID as existing partition: {prev_partition}."
            )
        partition.previous_partition_id = prev_partition.partition_id
    transaction = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=partition,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return partition


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
    if previous_partition:
        raise NotImplementedError(
            f"delta prepending from previous partition {previous_partition} "
            f"is not yet implemented"
        )
    if not partition.partition_id:
        raise ValueError("Partition ID to commit must be set to a staged partition ID.")
    if not partition.stream_locator:
        raise ValueError(
            "Partition to commit must have its stream locator "
            "set to the parent of its staged partition ID."
        )
    prev_staged_partition = get_partition_by_id(
        *args,
        stream_locator=partition.stream_locator,
        partition_id=partition.partition_id,
        **kwargs,
    )
    if not prev_staged_partition:
        raise ValueError(
            f"Partition at stream {partition.stream_locator} with ID "
            f"{partition.partition_id} not found."
        )
    if prev_staged_partition.state != CommitState.STAGED:
        raise ValueError(
            f"Expected to find a `{CommitState.STAGED}` partition at stream "
            f"{partition.stream_locator} with ID {partition.partition_id},"
            f"but found a `{prev_staged_partition.state}` partition."
        )
    partition: Partition = Metafile.update_for(prev_staged_partition)
    partition.state = CommitState.COMMITTED
    prev_committed_partition = get_partition(
        *args,
        stream_locator=partition.stream_locator,
        partition_value=partition.partition_values,
        partition_scheme_id=partition.partition_scheme_id,
        **kwargs,
    )
    # the first transaction operation updates the staged partition commit state
    txn_type = TransactionType.ALTER
    txn_ops = [
        TransactionOperation.of(
            operation_type=TransactionOperationType.UPDATE,
            dest_metafile=partition,
            src_metafile=prev_staged_partition,
        )
    ]
    if prev_committed_partition:
        if prev_committed_partition.partition_id != partition.previous_partition_id:
            raise ValueError(
                f"Previous partition ID mismatch Expected "
                f"{partition.previous_partition_id} but found "
                f"{prev_committed_partition.partition_id}."
            )
        # TODO(pdames): Add previous partition stream position validation.
        if prev_committed_partition.partition_id == partition.partition_id:
            raise ValueError(
                f"Partition to commit has the same ID as existing partition: "
                f"{prev_committed_partition}."
            )
        # there's a previously committed partition, so update the transaction
        # type to overwrite the previously committed partition, and add another
        # transaction operation to replace it with the staged partition
        txn_type = TransactionType.OVERWRITE
        txn_ops.append(
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=partition,
                src_metafile=prev_committed_partition,
            )
        )
    transaction = Transaction.of(
        txn_type=txn_type,
        txn_operations=txn_ops,
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )
    return partition


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
    partition_to_delete = get_partition(
        *args,
        stream_locator=stream_locator,
        partition_values=partition_values,
        partition_scheme_id=partition_scheme_id,
        **kwargs,
    )
    if not partition_to_delete:
        raise ValueError(
            f"Partition with values {partition_values} and scheme "
            f"{partition_scheme_id} not found in stream: {stream_locator}"
        )
    else:
        partition_to_delete.state = CommitState.DEPRECATED
    transaction = Transaction.of(
        txn_type=TransactionType.DELETE,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                src_metafile=partition_to_delete,
            )
        ],
    )
    catalog_properties = get_catalog_properties(**kwargs)
    transaction.commit(
        catalog_root_dir=catalog_properties.root,
        filesystem=catalog_properties.filesystem,
    )


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
    locator = PartitionLocator.of(
        stream_locator=stream_locator,
        partition_values=None,
        partition_id=partition_id,
    )
    return _latest(
        *args,
        metafile=Partition.of(
            locator=locator,
            schema=None,
            content_types=None,
        ),
        **kwargs,
    )


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
    locator = PartitionLocator.of(
        stream_locator=stream_locator,
        partition_values=partition_values,
        partition_id=None,
    )
    if not partition_scheme_id:
        # resolve latest partition scheme from the current
        # revision of its `deltacat` stream
        stream = get_stream(
            *args,
            namespace=stream_locator.namespace,
            table_name=stream_locator.table_name,
            table_version=stream_locator.table_version,
            **kwargs,
        )
        if not stream:
            raise ValueError(f"Stream {stream_locator} not found.")
        partition_scheme_id = stream.partition_scheme.id
    return _latest(
        *args,
        metafile=Partition.of(
            locator=locator,
            schema=None,
            content_types=None,
            state=CommitState.COMMITTED,
            partition_scheme_id=partition_scheme_id,
        ),
        **kwargs,
    )


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
    return _latest(
        *args,
        metafile=Namespace.of(NamespaceLocator.of(namespace)),
        **kwargs,
    )


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table namespace exists, False if not.
    """
    return _exists(
        *args,
        metafile=Namespace.of(NamespaceLocator.of(namespace)),
        **kwargs,
    )


def get_table(namespace: str, table_name: str, *args, **kwargs) -> Optional[Table]:
    """
    Gets table metadata for the specified table. Returns None if the given
    table does not exist.
    """
    locator = TableLocator.at(namespace=namespace, table_name=table_name)
    return _latest(
        *args,
        metafile=Table.of(locator=locator),
        **kwargs,
    )


def table_exists(namespace: str, table_name: str, *args, **kwargs) -> bool:
    """
    Returns True if the given table exists, False if not.
    """
    locator = TableLocator.at(namespace=namespace, table_name=table_name)
    return _exists(
        *args,
        metafile=Table.of(locator=locator),
        **kwargs,
    )


def get_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    *args,
    **kwargs,
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the specified table version. Returns None
    if the given table version does not exist.
    """
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=None,
    )
    return _latest(
        *args,
        metafile=table_version,
        **kwargs,
    )


def get_latest_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest version of the specified table.
    Returns None if no table version exists for the given table. Raises
    an error if the given table doesn't exist.
    """
    table_version_id = _resolve_latest_table_version_id(
        *args,
        namespace=namespace,
        table_name=table_name,
        fail_if_no_active_table_version=False,
        **kwargs,
    )

    return (
        get_table_version(
            *args,
            namespace=namespace,
            table_name=table_name,
            table_version=table_version_id,
            **kwargs,
        )
        if table_version_id
        else None
    )


def get_latest_active_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Gets table version metadata for the latest active version of the specified
    table. Returns None if no active table version exists for the given table.
    Raises an error if the given table doesn't exist.
    """
    table_version_id = _resolve_latest_active_table_version_id(
        *args,
        namespace=namespace,
        table_name=table_name,
        fail_if_no_active_table_version=False,
        **kwargs,
    )
    return (
        get_table_version(
            *args,
            namespace=namespace,
            table_name=table_name,
            table_version=table_version_id,
            **kwargs,
        )
        if table_version_id
        else None
    )


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
    schema = get_table_version_schema(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    return schema.arrow.names if schema else None


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
    table_version = (
        get_table_version(
            *args,
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            **kwargs,
        )
        if table_version
        else get_latest_active_table_version(
            *args,
            namespace=namespace,
            table_name=table_name,
            **kwargs,
        )
    )
    return table_version.schema


def table_version_exists(
    namespace: str,
    table_name: str,
    table_version: str,
    *args,
    **kwargs,
) -> bool:
    """
    Returns True if the given table version exists, False if not.
    """
    locator = TableVersionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
    )
    table_version = TableVersion.of(
        locator=locator,
        schema=None,
    )
    return _exists(
        *args,
        metafile=table_version,
        **kwargs,
    )


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
