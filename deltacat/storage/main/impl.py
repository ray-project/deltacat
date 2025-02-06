"""
Implementation of storage interface

NOTE: THIS CURRENTLY DIVERGES FROM storage/interface.py!

After this implementation is done, we will make storage/interface_v2.py. We then have to migrate
  storage/iceberg/impl from storage/interface_v1.py to storage/interface_v2.py
"""

from deltacat.catalog.main.impl import PropertyCatalog

from typing import Any, Callable, Dict, List, Optional, Union, TypeVar

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
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    TransactionType,
    TransactionOperationType, StreamFormat,
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
)
from deltacat.storage.model.table_version import (
    TableVersion,
    TableVersionProperties,
)
from deltacat.storage.model.metafile import (
    Metafile,
)
from deltacat.storage.model.transaction import (
    TransactionOperation,
    Transaction,
)
from deltacat.storage.model.manifest import Manifest
from deltacat.types.media import (
    ContentType,
    DistributedDatasetType,
    StorageType,
    TableType,
)
from deltacat.utils.common import ReadKwargsProvider

METAFILE = TypeVar("METAFILE", bound=Metafile)

def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """
    Lists a page of table namespaces. Namespaces are returned as list result
    items.
    """
    placeholder_ns = Namespace.of(NamespaceLocator.of("placeholder"))
    return _list_metafiles(
        placeholder_ns,
        TransactionOperationType.READ_SIBLINGS,
        *args,
        **kwargs
    )

def list_tables(namespace: str, *args, **kwargs) -> ListResult[Table]:
    """
    Lists a page of tables for the given table namespace.
    Raises an error if the namespace does not exist.
    """
    ns = get_namespace(namespace, *args, **kwargs)
    if not ns:
        raise ValueError(f"Namespace '{namespace}' does not exist.")
    return _list_metafiles(
        ns,
        TransactionOperationType.READ_CHILDREN,
        *args,
        **kwargs
    )

def list_table_versions(
    namespace: str, table_name: str, *args, **kwargs
) -> ListResult[TableVersion]:
    """
    Lists a page of table versions for the given table.
    Raises an error if the table does not exist.
    """
    tbl = get_table(namespace, table_name, *args, **kwargs)
    if not tbl:
        raise ValueError(f"Table '{namespace}.{table_name}' does not exist.")
    return _list_metafiles(
        tbl,
        TransactionOperationType.READ_CHILDREN,
        *args,
        **kwargs
    )

def list_streams(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> ListResult[TableVersion]:
    """
    Lists a page of table versions for the given table.
    Raises an error if the table does not exist.
    """
    tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        raise ValueError(f"Table Version'{namespace}.{table_name}.{table_version}' does not exist.")
    return _list_metafiles(
        tv,
        TransactionOperationType.READ_CHILDREN,
        *args,
        **kwargs
    )

def list_partitions(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_id: Optional[str] = None,
    *args,
    **kwargs,
) -> ListResult[Partition]:
    """
    Lists a page of partitions for the given table version.
    Raises an error if the table version does not exist.
    """
    if table_version is None:
        tv = get_latest_active_table_version(namespace, table_name, *args, **kwargs)
    else:
        tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        raise ValueError(f"Table version '{namespace}.{table_name}.{table_version}' not found.")
    
    
    return _list_metafiles(tv, TransactionOperationType.READ_CHILDREN, *args, **kwargs)

def list_stream_partitions(stream: Stream, *args, **kwargs) -> ListResult[Partition]:
    """
    Lists all partitions committed to the given stream.
    """
    return _list_metafiles(
        stream,
        TransactionOperationType.READ_CHILDREN,
        *args,
        **kwargs
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
    Lists a page of deltas for the given table version & partition.
    In this simplified approach, we simply treat Deltas as children of the partition.
    """
    if not table_version:
        tv = get_latest_active_table_version(namespace, table_name, *args, **kwargs)
    else:
        tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        raise ValueError("No table version found.")
    stream = get_stream(namespace, table_name, tv.table_version, *args, **kwargs)
    if not stream:
        raise ValueError("No stream found.")
    part = get_partition(stream.locator, partition_values, *args, **kwargs)
    if not part:
        raise ValueError("Partition not found; cannot list deltas.")
    return _list_metafiles(part, TransactionOperationType.READ_CHILDREN, *args, **kwargs)

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
    Lists Deltas that are children of a Partition.
    This simple approach ignores advanced filtering by stream_position.
    """
    if isinstance(partition_like, Partition):
        part_obj = partition_like
    else:
        part_obj = Partition.of(locator=partition_like, schema=None, content_types=None)

    return _list_metafiles(
        part_obj,
        TransactionOperationType.READ_CHILDREN,
        *args,
        **kwargs
    )

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
    """
    if not table_version:
        tv = get_latest_active_table_version(namespace, table_name, *args, **kwargs)
    else:
        tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        return None
    stream = get_stream(namespace, table_name, tv.table_version, *args, **kwargs)
    if not stream:
        return None
    part = get_partition(stream.locator, partition_values, *args, **kwargs)
    if not part:
        return None

    from deltacat.storage.model.delta import Delta, DeltaLocator
    delta_loc = DeltaLocator.of(part.locator, stream_position)
    placeholder = Delta.of(
        locator=delta_loc,
        delta_type=None,
        meta=None,
        properties=None,
        manifest=None,
        previous_stream_position=None,
    )
    found = _read_latest_metafile(
        placeholder,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )
    return found

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
    Gets the latest (highest stream_position) delta in a partition by listing
    them and taking the last.
    """
    lr = list_deltas(
        namespace,
        table_name,
        partition_values,
        table_version,
        *args,
        **kwargs
    )
    all_deltas = lr.all_items()
    if not all_deltas:
        return None
    return all_deltas[-1]

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
) -> Union[LocalDataset, DistributedDataset]:
    """
    Simplified approach: We pretend to load from the Delta's manifest,
    but just return an empty list or None.
    """
    if isinstance(delta_like, Delta):
        delta_obj = delta_like
    else:
        from deltacat.storage.model.delta import Delta
        placeholder = Delta.of(locator=delta_like, delta_type=None, meta=None, properties=None, manifest=None)
        delta_obj = _read_latest_metafile(
            placeholder,
            TransactionOperationType.READ_LATEST,
            *args,
            **kwargs
        )
    if not delta_obj or not delta_obj.manifest:
        return [] if storage_type == StorageType.LOCAL else None

    # Real logic would parse delta_obj.manifest, read files, etc.
    if storage_type == StorageType.LOCAL:
        return []
    else:
        return None

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
    Simplified: returns None or empty data. Real logic would fetch the single
    manifest entry => read => return a local table (PyArrow Table, Pandas, etc.)
    """
    return None

def get_delta_manifest(
    delta_like: Union[Delta, DeltaLocator], *args, **kwargs
) -> Manifest:
    """
    Return the "authoritative" manifest from a newly-read Delta.
    Ignores any local delta.manifest in memory.
    """
    if isinstance(delta_like, Delta):
        d_loc = delta_like.locator
    else:
        d_loc = delta_like
    from deltacat.storage.model.delta import Delta
    placeholder = Delta.of(locator=d_loc, delta_type=None, meta=None, properties=None, manifest=None)
    fresh = _read_latest_metafile(
        placeholder,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )
    if not fresh or not fresh.manifest:
        raise ValueError("No manifest found on that delta.")
    return fresh.manifest

def create_namespace(
    namespace: str,
    properties: Optional[NamespaceProperties] = None,
    *args,
    **kwargs,
) -> Namespace:
    """
    Creates a table namespace with the given name and properties.
    """
    catalog = _get_catalog(**kwargs)
    ns_obj = Namespace.of(NamespaceLocator.of(namespace), properties=properties)
    txn = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=ns_obj,
            )
        ],
    )
    txn.commit(
        catalog_root_dir=catalog.root,
        filesystem=catalog.filesystem,
    )
    return ns_obj

def update_namespace(
    namespace: str,
    properties: NamespaceProperties = None,
    new_namespace: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    ns = get_namespace(namespace, *args, **kwargs)
    if not ns:
        raise ValueError(f"Namespace '{namespace}' does not exist.")

    updated_ns = Namespace.of(
        locator=ns.locator,
        properties=properties if properties else ns.properties,
    )
    if new_namespace:
        updated_ns.locator.namespace = new_namespace

    catalog = _get_catalog(**kwargs)
    tx = Transaction.of(
        txn_type=TransactionType.ALTER,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=updated_ns,
                src_metafile=ns,
            )
        ],
    )
    tx.commit(catalog_root_dir=catalog.root, filesystem=catalog.filesystem)

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
) -> Stream:
    """
    A convenience method that ensures the table is created if needed, then
    creates a new TableVersion, then creates & returns a default Stream.
    """
    ns = get_namespace(namespace, *args, **kwargs)
    if not ns:
        raise ValueError(f"Namespace '{namespace}' does not exist.")

    tbl = get_table(namespace, table_name, *args, **kwargs)
    if not tbl:
        # create the table
        from deltacat.storage.model.table import Table, TableLocator
        new_table = Table.of(
            TableLocator.at(namespace, table_name),
            description=table_description,
            properties=table_properties,
        )
        catalog = _get_catalog(**kwargs)
        c_tx = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=new_table,
                )
            ],
        )
        c_tx.commit(catalog_root_dir=catalog.root, filesystem=catalog.filesystem)
        tbl = new_table

    from deltacat.storage.model.table_version import TableVersion, TableVersionLocator
    tv_loc = TableVersionLocator.at(namespace, table_name, table_version)
    tv_obj = TableVersion.of(
        locator=tv_loc,
        schema=schema,
        partition_scheme=partition_scheme,
        sort_scheme=sort_keys,
        description=table_version_description,
        properties=table_version_properties,
        content_types=supported_content_types,
        lifecycle_state=LifecycleState.UNRELEASED,
    )
    # commit the TableVersion
    existing_tv = _read_latest_metafile(
        tv_obj,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )
    if existing_tv:
        raise ValueError(f"TableVersion '{table_version}' already exists on '{table_name}'.")
    catalog = _get_catalog(**kwargs)
    tx = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=tv_obj,
            )
        ],
    )
    tx.commit(catalog_root_dir=catalog.root, filesystem=catalog.filesystem)

    # now create & commit a Stream
    s = stage_stream(namespace, table_name, tv_obj.table_version, *args, **kwargs)
    commit_stream(s, *args, **kwargs)
    return s

def update_table(
    namespace: str,
    table_name: str,
    description: Optional[str] = None,
    properties: Optional[TableProperties] = None,
    new_table_name: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    t = get_table(namespace, table_name, *args, **kwargs)
    if not t:
        raise ValueError(f"Table '{namespace}.{table_name}' does not exist.")

    updated_tbl = Table.of(
        locator=t.locator,
        description=(description if description is not None else t.description),
        properties=(properties if properties is not None else t.properties),
    )
    if new_table_name:
        updated_tbl.locator.table_name = new_table_name

    catalog = _get_catalog(**kwargs)
    tx = Transaction.of(
        txn_type=TransactionType.ALTER,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=updated_tbl,
                src_metafile=t,
            )
        ],
    )
    tx.commit(catalog_root_dir=catalog.root, filesystem=catalog.filesystem)

def update_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Schema] = None,
    description: Optional[str] = None,
    properties: Optional[TableVersionProperties] = None,
    *args,
    **kwargs,
) -> None:
    tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        raise ValueError(f"TableVersion '{table_version}' not found on '{table_name}'.")

    updated_tv = TableVersion.of(
        locator=tv.locator,
        schema=(schema if schema is not None else tv.schema),
        partition_scheme=tv.partition_scheme,
        description=(description if description is not None else tv.description),
        properties=(properties if properties is not None else tv.properties),
        content_types=tv.content_types,
        sort_scheme=tv.sort_scheme,
        lifecycle_state=(lifecycle_state if lifecycle_state else tv.state),
    )

    catalog = _get_catalog(**kwargs)
    tx = Transaction.of(
        txn_type=TransactionType.ALTER,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=updated_tv,
                src_metafile=tv,
            )
        ],
    )
    tx.commit(catalog_root_dir=catalog.root, filesystem=catalog.filesystem)

def stage_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Stream:
    """
    Stages a new delta stream for the given table version by constructing
    an in-memory object. Not yet committed.

    TODO reconcile with logic in Patrick's branch
    """
    if table_version is None:
        tv = get_latest_active_table_version(namespace, table_name, *args, **kwargs)
    else:
        tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        raise ValueError("No table version found to stage a stream on.")

    from deltacat.storage.model.stream import Stream, StreamLocator
    s_loc = StreamLocator.at(
        namespace,
        table_name,
        tv.table_version,
        stream_id=None,
        stream_format=None,
    )
    st = Stream.of(
        s_loc,
        partition_scheme=tv.partition_scheme,
    )
    return st

def commit_stream(stream: Stream, *args, **kwargs) -> Stream:
    existing = _read_latest_metafile(
        stream,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )
    if existing:
        op_type = TransactionOperationType.UPDATE
    else:
        op_type = TransactionOperationType.CREATE

    catalog = _get_catalog(**kwargs)
    tx = Transaction.of(
        txn_type=TransactionType.ALTER if op_type == TransactionOperationType.UPDATE else TransactionType.APPEND,
        txn_operations=[
            TransactionOperation.of(
                operation_type=op_type,
                dest_metafile=stream,
                src_metafile=existing if op_type == TransactionOperationType.UPDATE else None,
            )
        ],
    )
    tx.commit(catalog_root_dir=catalog.root, filesystem=catalog.filesystem)
    return stream

def delete_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    stream = get_stream(namespace, table_name, table_version, *args, **kwargs)
    if not stream:
        raise ValueError("No stream is currently committed for that table version.")

    catalog = _get_catalog(**kwargs)
    tx = Transaction.of(
        txn_type=TransactionType.DELETE,
        txn_operations=[
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=stream,
            )
        ],
    )
    tx.commit(catalog_root_dir=catalog.root, filesystem=catalog.filesystem)

def get_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    stream_id: Optional[str] = None,
    stream_format: Optional[StreamFormat] = StreamFormat.DELTACAT,
    *args,
    **kwargs,
) -> Optional[Stream]:
    """
    Get a stream

    TODO support default aliases based on stream format
    """
    if table_version is None:
        tv = get_latest_active_table_version(namespace, table_name, *args, **kwargs)
    else:
        tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        return None
    from deltacat.storage.model.stream import Stream, StreamLocator
    s_loc = StreamLocator.at(
        namespace,
        table_name,
        tv.table_version,
        stream_id=None,
        stream_format=None,
    )
    placeholder = Stream.of(s_loc, partition_scheme=None)
    return _read_latest_metafile(
        placeholder,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )

def stage_partition(
    stream: Stream, partition_values: Optional[PartitionValues] = None, *args, **kwargs
) -> Partition:
   raise NotImplementedError("Not yet implemented")

def commit_partition(
    partition: Partition,
    previous_partition: Optional[Partition] = None,
    *args,
    **kwargs,
) -> Partition:
    raise NotImplementedError("Not yet implemented")

def delete_partition(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> None:
    raise NotImplementedError("Not yet implemented")

def get_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> Optional[Partition]:
    raise NotImplementedError("Not yet implemented")

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
    raise NotImplementedError("Not yet implemented")

def commit_delta(delta: Delta, *args, **kwargs) -> Delta:
    raise NotImplementedError("Not yet implemented")

def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """
    Gets table namespace metadata for the specified table namespace. Returns
    None if not found.
    """
    placeholder_ns = Namespace.of(NamespaceLocator.of(namespace))
    return _read_latest_metafile(
        placeholder_ns,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )

def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """
    Returns True if the namespace exists, else False.
    """
    placeholder_ns = Namespace.of(NamespaceLocator.of(namespace))
    check = _read_latest_metafile(
        placeholder_ns,
        TransactionOperationType.READ_EXISTS,
        *args,
        **kwargs
    )
    return check is not None

def get_table(namespace: str, table_name: str, *args, **kwargs) -> Optional[Table]:
    """
    Gets table metadata for the specified table. Returns None if not found.
    """
    ns = get_namespace(namespace, *args, **kwargs)
    if not ns:
        return None

    from deltacat.storage.model.table import Table, TableLocator
    placeholder_tbl = Table.of(
        locator=TableLocator.at(namespace, table_name),
    )
    return _read_latest_metafile(
        placeholder_tbl,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )

def table_exists(namespace: str, table_name: str, *args, **kwargs) -> bool:
    return get_table(namespace, table_name, *args, **kwargs) is not None

def get_table_version(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> Optional[TableVersion]:
    tbl = get_table(namespace, table_name, *args, **kwargs)
    if not tbl:
        return None

    from deltacat.storage.model.table_version import TableVersion, TableVersionLocator
    tv_loc = TableVersionLocator.at(namespace, table_name, table_version)
    placeholder_tv = TableVersion.of(locator=tv_loc, schema=None)
    return _read_latest_metafile(
        placeholder_tv,
        TransactionOperationType.READ_LATEST,
        *args,
        **kwargs
    )

def get_latest_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Lists all versions, returning the last in revision order.
    """
    lr = list_table_versions(namespace, table_name, *args, **kwargs)
    all_tvs = lr.all_items()
    if not all_tvs:
        return None
    return all_tvs[-1]

def get_latest_active_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    """
    Looks up all table versions, returns the last one whose state == ACTIVE.
    """
    lr = list_table_versions(namespace, table_name, *args, **kwargs)
    all_tvs = lr.all_items()
    if not all_tvs:
        return None
    active_vers = [tv for tv in all_tvs if tv.state == LifecycleState.ACTIVE]
    if not active_vers:
        return None
    return active_vers[-1]

def get_latest_stream(
        namespace: str, table_name: str, table_version: str, stream_format: Optional[StreamFormat]):
    lr = list_streams()
    # TODO finish


def get_table_version_column_names(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[List[str]]:
    """
    Returns a list of schema column names for the given table version.
    """
    if not table_version:
        tv = get_latest_active_table_version(namespace, table_name, *args, **kwargs)
    else:
        tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv or not tv.schema:
        return None
    return tv.schema.arrow.names

def get_table_version_schema(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Schema]:
    if not table_version:
        tv = get_latest_active_table_version(namespace, table_name, *args, **kwargs)
    else:
        tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    if not tv:
        raise ValueError(f"TableVersion not found for {namespace}.{table_name}.{table_version}")
    return tv.schema

def table_version_exists(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> bool:
    tv = get_table_version(namespace, table_name, table_version, *args, **kwargs)
    return tv is not None

def can_categorize(e: BaseException, *args, **kwargs) -> bool:
    raise NotImplementedError()

def raise_categorized_error(e: BaseException, *args, **kwargs):
    """
    Raise and handle storage implementation layer speci
    """
    raise NotImplementedError()


def _get_catalog(**kwargs) -> PropertyCatalog:
    """
    Utility: retrieves a 'catalog' object from kwargs, ensuring it is a
    PropertyCatalog. Adjust if you have a different catalog approach.
    """
    catalog: PropertyCatalog = kwargs.get("catalog")
    if not isinstance(catalog, PropertyCatalog):
        raise TypeError(f"Expected `catalog` param to be PropertyCatalog, got {type(catalog)}")
    return catalog

def _list_metafiles(
    metafile: Metafile,
    txn_op_type: TransactionOperationType,
    *args,
    **kwargs,
) -> ListResult[Metafile]:
    """
    Helper that runs a READ transaction1 operation to list siblings or children
    of a given metafile. E.g. use READ_SIBLINGS, READ_CHILDREN, READ_LATEST
    """
    catalog = _get_catalog(**kwargs)
    limit = kwargs.get("limit") or None
    txn = Transaction.of(
        txn_type=TransactionType.READ,
        txn_operations=[
            TransactionOperation.of(
                operation_type=txn_op_type,
                dest_metafile=metafile,
                read_limit=limit,
            )
        ],
    )
    list_results = txn.commit(
        catalog_root_dir=catalog.root,
        filesystem=catalog.filesystem,
    )
    return list_results[0]  # we only have one op

def _read_latest_metafile(
    metafile: Metafile,
    txn_op_type: TransactionOperationType,
    *args,
    **kwargs,
) -> Optional[Metafile]:
    """
    Helper that runs a READ transaction op to retrieve the "latest" or "exists"
    revision of a single metafile.
    """
    lr = _list_metafiles(metafile, txn_op_type, *args, **kwargs)
    items = lr.all_items()
    return items[0] if items else None

