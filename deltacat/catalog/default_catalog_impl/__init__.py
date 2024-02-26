from typing import Any, Dict, List, Optional, Set, Union, Tuple
import pyarrow as pa
import logging
from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.storage.model.sort_key import SortKey
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.namespace import Namespace
from deltacat.storage.model.types import (
    DistributedDataset,
    LifecycleState,
    LocalDataset,
    LocalTable,
    SchemaConsistencyType,
)
from deltacat.storage.model.partition import PartitionLocator, Partition
from deltacat.storage.model.table_version import TableVersion
from deltacat.compute.merge_on_read.model.merge_on_read_params import MergeOnReadParams
from deltacat.storage.model.delta import DeltaType
import deltacat.storage.interface as deltacat_storage
from deltacat.types.media import ContentType, TableType, DistributedDatasetType
from deltacat.types.tables import TableWriteMode
from deltacat.compute.merge_on_read import MERGE_FUNC_BY_DISTRIBUTED_DATASET_TYPE
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

STORAGE = None


# table functions
def write_to_table(
    data: Union[LocalTable, LocalDataset, DistributedDataset],  # type: ignore
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
    raise NotImplementedError("write_to_table not implemented")


def read_table(
    table: str,
    namespace: Optional[str] = None,
    table_version: Optional[str] = None,
    table_type: Optional[TableType] = TableType.PYARROW,
    distributed_dataset_type: Optional[
        DistributedDatasetType
    ] = DistributedDatasetType.RAY_DATASET,
    partition_filter: Optional[List[Union[Partition, PartitionLocator]]] = None,
    stream_position_range_inclusive: Optional[Tuple[int, int]] = None,
    merge_on_read: Optional[bool] = False,
    reader_kwargs: Optional[Dict[Any, Any]] = None,
    deltacat_storage_kwargs: Optional[Dict[Any, Any]] = None,
    *args,
    **kwargs,
) -> DistributedDataset:  # type: ignore
    """Read a table into a distributed dataset."""

    if reader_kwargs is None:
        reader_kwargs = {}

    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}

    _validate_read_table_args(
        namespace=namespace,
        table_type=table_type,
        distributed_dataset_type=distributed_dataset_type,
        merge_on_read=merge_on_read,
    )

    table_version_obj = _get_latest_or_given_table_version(
        namespace=namespace,
        table_name=table,
        table_version=table_version,
        **deltacat_storage_kwargs,
    )
    table_version = table_version_obj.table_version

    if (
        table_version_obj.content_types is None
        or len(table_version_obj.content_types) != 1
    ):
        raise ValueError(
            "Expected exactly one content type but "
            f"found {table_version_obj.content_types}."
        )

    logger.info(
        f"Reading metadata for table={namespace}/{table}/{table_version} "
        f"with partition_filters={partition_filter} and stream position"
        f" range={stream_position_range_inclusive}"
    )

    if partition_filter is None:
        logger.info(
            f"Reading all partitions metadata in the table={table} "
            "as partition_filter was None."
        )
        partition_filter = STORAGE.list_partitions(
            table_name=table,
            namespace=namespace,
            table_version=table_version,
            **deltacat_storage_kwargs,
        ).all_items()

    qualified_deltas = _get_deltas_from_partition_filter(
        stream_position_range_inclusive=stream_position_range_inclusive,
        partition_filter=partition_filter,
        **deltacat_storage_kwargs,
    )

    logger.info(
        f"Total qualified deltas={len(qualified_deltas)} "
        f"from {len(partition_filter)} partitions."
    )

    merge_on_read_params = MergeOnReadParams.of(
        {
            "deltas": qualified_deltas,
            "deltacat_storage": STORAGE,
            "deltacat_storage_kwargs": deltacat_storage_kwargs,
            "reader_kwargs": reader_kwargs,
        }
    )

    return MERGE_FUNC_BY_DISTRIBUTED_DATASET_TYPE[distributed_dataset_type.value](
        params=merge_on_read_params, **kwargs
    )


def alter_table(
    table: str,
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema_updates: Optional[Dict[str, Any]] = None,
    partition_updates: Optional[Dict[str, Any]] = None,
    primary_keys: Optional[Set[str]] = None,
    sort_keys: Optional[List[SortKey]] = None,
    description: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    *args,
    **kwargs,
) -> None:
    """Alter table definition."""
    raise NotImplementedError("alter_table not implemented")


def create_table(
    table: str,
    namespace: Optional[str] = None,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Union[pa.Schema, str, bytes]] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    partition_keys: Optional[List[Dict[str, Any]]] = None,
    primary_keys: Optional[Set[str]] = None,
    sort_keys: Optional[List[SortKey]] = None,
    description: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    permissions: Optional[Dict[str, Any]] = None,
    content_types: Optional[List[ContentType]] = None,
    replace_existing_table: bool = False,
    *args,
    **kwargs,
) -> TableDefinition:
    """Create an empty table. Raises an error if the table already exists and
    `replace_existing_table` is False."""
    raise NotImplementedError("create_table not implemented")


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
    raise NotImplementedError("get_table not implemented")


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
    raise NotImplementedError("table_exists not implemented")


# namespace functions
def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    """List a page of table namespaces."""
    raise NotImplementedError("list_namespaces not implemented")


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    """Gets table namespace metadata for the specified table namespace. Returns
    None if the given namespace does not exist."""
    raise NotImplementedError("get_namespace not implemented")


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    """Returns True if the given table namespace exists, False if not."""
    raise NotImplementedError("namespace_exists not implemented")


def create_namespace(
    namespace: str, permissions: Dict[str, Any], *args, **kwargs
) -> Namespace:
    """Creates a table namespace with the given name and permissions. Returns
    the created namespace. Raises an error if the namespace already exists."""
    raise NotImplementedError("create_namespace not implemented")


def alter_namespace(
    namespace: str,
    permissions: Optional[Dict[str, Any]] = None,
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
def initialize(ds: deltacat_storage, *args, **kwargs) -> None:
    """Initializes the data catalog with the given arguments."""
    global STORAGE
    STORAGE = ds


def _validate_read_table_args(
    namespace: Optional[str] = None,
    table_type: Optional[TableType] = None,
    distributed_dataset_type: Optional[DistributedDatasetType] = None,
    merge_on_read: Optional[bool] = None,
):
    if STORAGE is None:
        raise ValueError(
            "Catalog not initialized. Did you miss calling "
            "initialize(ds=<deltacat_storage>)?"
        )

    if merge_on_read:
        raise ValueError("Merge on read not supported currently.")

    if table_type is not TableType.PYARROW:
        raise ValueError("Only PYARROW table type is supported as of now")

    if distributed_dataset_type is not DistributedDatasetType.DAFT:
        raise ValueError("Only DAFT dataset type is supported as of now")

    if namespace is None:
        raise ValueError(
            "namespace must be passed to uniquely identify a table in the catalog."
        )


def _get_latest_or_given_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> TableVersion:
    table_version_obj = None
    if table_version is None:
        table_version_obj = STORAGE.get_latest_table_version(
            namespace=namespace, table_name=table_name, *args, **kwargs
        )
        table_version = table_version_obj.table_version
    else:
        table_version_obj = STORAGE.get_table_version(
            namespace=namespace,
            table_name=table_name,
            table_version=table_version,
            *args,
            **kwargs,
        )

    return table_version_obj


def _get_deltas_from_partition_filter(
    partition_filter: Optional[List[Union[Partition, PartitionLocator]]] = None,
    stream_position_range_inclusive: Optional[Tuple[int, int]] = None,
    *args,
    **kwargs,
):

    result_deltas = []
    start_stream_position, end_stream_position = stream_position_range_inclusive or (
        None,
        None,
    )
    for partition_like in partition_filter:
        deltas = STORAGE.list_partition_deltas(
            partition_like=partition_like,
            ascending_order=True,
            include_manifest=True,
            start_stream_position=start_stream_position,
            last_stream_position=end_stream_position,
            *args,
            **kwargs,
        ).all_items()

        for delta in deltas:
            if (
                start_stream_position is None
                or delta.stream_position >= start_stream_position
            ) and (
                end_stream_position is None
                or delta.stream_position <= end_stream_position
            ):
                if delta.type == DeltaType.DELETE:
                    raise ValueError("DELETE type deltas are not supported")
                result_deltas.append(delta)

    return result_deltas
