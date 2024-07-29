from typing import Any, Callable, Dict, List, Optional, Set, Union, Tuple

import pyarrow as pa
import daft
import json
import sqlite3
from sqlite3 import Cursor, Connection
import uuid
import ray
import io

from deltacat.tests.test_utils.storage import create_empty_delta
from deltacat.utils.common import current_time_ms


from deltacat.storage import (
    Delta,
    DeltaLocator,
    DeltaType,
    DistributedDataset,
    LifecycleState,
    ListResult,
    LocalDataset,
    LocalTable,
    Manifest,
    ManifestAuthor,
    Namespace,
    NamespaceLocator,
    Partition,
    SchemaConsistencyType,
    Stream,
    StreamLocator,
    Table,
    TableVersion,
    TableVersionLocator,
    TableLocator,
    CommitState,
    SortKey,
    PartitionLocator,
    ManifestMeta,
    ManifestEntry,
    ManifestEntryList,
    DeleteParameters,
    PartitionFilter,
    PartitionValues,
    DeltaPartitionSpec,
    StreamPartitionSpec,
    TransformName,
    IdentityTransformParameters,
)
from deltacat.types.media import (
    ContentType,
    StorageType,
    TableType,
    ContentEncoding,
    DistributedDatasetType,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.tests.local_deltacat_storage.exceptions import (
    InvalidNamespaceError,
    LocalStorageValidationError,
)

SQLITE_CUR_ARG = "sqlite3_cur"
SQLITE_CON_ARG = "sqlite3_con"
DB_FILE_PATH_ARG = "db_file_path"

STORAGE_TYPE = "SQLITE3"
STREAM_ID_PROPERTY = "stream_id"
CREATE_NAMESPACES_TABLE = (
    "CREATE TABLE IF NOT EXISTS namespaces(locator, value, PRIMARY KEY (locator))"
)
CREATE_TABLES_TABLE = (
    "CREATE TABLE IF NOT EXISTS tables(locator, namespace_locator, value, PRIMARY KEY (locator), "
    "FOREIGN KEY (namespace_locator) REFERENCES namespaces(locator))"
)
CREATE_TABLE_VERSIONS_TABLE = (
    "CREATE TABLE IF NOT EXISTS table_versions(locator, table_locator, value, PRIMARY KEY (locator), "
    "FOREIGN KEY (table_locator) REFERENCES tables(locator))"
)
CREATE_STREAMS_TABLE = (
    "CREATE TABLE IF NOT EXISTS streams(locator, table_version_locator, value, PRIMARY KEY(locator), "
    "FOREIGN KEY (table_version_locator) REFERENCES table_versions(locator))"
)
CREATE_PARTITIONS_TABLE = (
    "CREATE TABLE IF NOT EXISTS partitions(locator, stream_locator, value, PRIMARY KEY(locator), "
    "FOREIGN KEY (stream_locator) REFERENCES streams(locator))"
)
CREATE_DELTAS_TABLE = (
    "CREATE TABLE IF NOT EXISTS deltas(locator, partition_locator, value, PRIMARY KEY(locator), "
    "FOREIGN KEY (partition_locator) REFERENCES partitions(locator))"
)
CREATE_DATA_TABLE = "CREATE TABLE IF NOT EXISTS data(uri, value, PRIMARY KEY(uri))"


def _get_sqlite3_cursor_con(kwargs) -> Tuple[Cursor, Connection]:
    if SQLITE_CUR_ARG in kwargs and SQLITE_CON_ARG in kwargs:
        return kwargs[SQLITE_CUR_ARG], kwargs[SQLITE_CON_ARG]
    elif DB_FILE_PATH_ARG in kwargs:
        con = sqlite3.connect(kwargs[DB_FILE_PATH_ARG])
        cur = con.cursor()
        return cur, con

    raise ValueError(f"Invalid local db connection kwargs: {kwargs}")


def _get_manifest_entry_uri(manifest_entry_id: str) -> str:
    return f"cloudpickle://{manifest_entry_id}"


def _merge_and_promote(
    partition_deltas: List[Delta], previous_partition_deltas: List[Delta]
):
    previous_partition_deltas_spos_gt: List[Delta] = [
        delta
        for delta in previous_partition_deltas
        if delta.stream_position > partition_deltas[0].stream_position
    ]
    # handle the case if the previous partition deltas have a greater stream position than the partition_delta
    partition_deltas = previous_partition_deltas_spos_gt + partition_deltas
    return partition_deltas


def list_namespaces(*args, **kwargs) -> ListResult[Namespace]:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    res = cur.execute("SELECT * FROM namespaces")
    fetched = res.fetchall()
    result = []

    for item in fetched:
        result.append(Namespace(json.loads(item[1])))

    return ListResult.of(result, None, None)


def list_tables(namespace: str, *args, **kwargs) -> ListResult[Table]:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    params = (NamespaceLocator.of(namespace).canonical_string(),)
    res = cur.execute("SELECT * FROM tables WHERE namespace_locator = ?", params)
    fetched = res.fetchall()
    result = []

    for item in fetched:
        result.append(Table(json.loads(item[2])))

    return ListResult.of(result, None, None)


def list_table_versions(
    namespace: str, table_name: str, *args, **kwargs
) -> ListResult[TableVersion]:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    table_locator = TableLocator.of(NamespaceLocator.of(namespace), table_name)

    res = cur.execute(
        "SELECT * FROM table_versions WHERE table_locator = ?",
        (table_locator.canonical_string(),),
    )
    fetched = res.fetchall()
    result = []

    for item in fetched:
        result.append(TableVersion(json.loads(item[2])))

    return ListResult.of(result, None, None)


def list_partitions(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> ListResult[Partition]:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    stream = get_stream(namespace, table_name, table_version, *args, **kwargs)

    res = cur.execute(
        "SELECT * FROM partitions WHERE stream_locator = ?",
        (stream.locator.canonical_string(),),
    )

    fetched = res.fetchall()
    result = []
    for item in fetched:
        partition = Partition(json.loads(item[2]))
        if partition.state == CommitState.COMMITTED:
            result.append(partition)

    return ListResult.of(result, None, None)


def list_stream_partitions(stream: Stream, *args, **kwargs) -> ListResult[Partition]:
    return list_partitions(
        stream.namespace, stream.table_name, stream.table_version, *args, **kwargs
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
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> ListResult[Delta]:
    stream = get_stream(namespace, table_name, table_version, *args, **kwargs)
    if stream is None:
        return ListResult.of([], None, None)

    if partition_values is not None and partition_filter is not None:
        raise ValueError(
            "Only one of partition_values or partition_filter must be provided"
        )
    if partition_filter is not None:
        partition_values = partition_filter.partition_values

    partition = get_partition(stream.locator, partition_values, *args, **kwargs)

    all_deltas = list_partition_deltas(
        partition,
        first_stream_position=first_stream_position,
        last_stream_position=last_stream_position,
        ascending_order=ascending_order,
        include_manifest=include_manifest,
        *args,
        **kwargs,
    ).all_items()

    result = []

    for delta in all_deltas:
        if (
            not first_stream_position or first_stream_position < delta.stream_position
        ) and (
            not last_stream_position or delta.stream_position <= last_stream_position
        ):
            result.append(delta)

        if not include_manifest:
            delta.manifest = None

    result.sort(reverse=(not ascending_order), key=lambda d: d.stream_position)
    return ListResult.of(result, None, None)


def list_partition_deltas(
    partition_like: Union[Partition, PartitionLocator],
    first_stream_position: Optional[int] = None,
    last_stream_position: Optional[int] = None,
    ascending_order: bool = False,
    include_manifest: bool = False,
    *args,
    **kwargs,
) -> ListResult[Delta]:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    if partition_like is None:
        return ListResult.of([], None, None)

    if first_stream_position is None:
        first_stream_position = 0

    if last_stream_position is None:
        last_stream_position = float("inf")

    assert isinstance(partition_like, Partition) or isinstance(
        partition_like, PartitionLocator
    ), f"Expected a Partition or PartitionLocator as an input argument but found {partition_like}"

    partition_locator = None
    if isinstance(partition_like, Partition):
        partition_locator = partition_like.locator
    else:
        partition_locator = partition_like

    res = cur.execute(
        "SELECT * FROM deltas WHERE partition_locator = ?",
        (partition_locator.canonical_string(),),
    )

    serialized_items = res.fetchall()

    if not serialized_items:
        return ListResult.of([], None, None)

    result = []
    for item in serialized_items:
        current_delta = Delta(json.loads(item[2]))
        if (
            first_stream_position
            <= current_delta.stream_position
            <= last_stream_position
        ):
            result.append(current_delta)

        if not include_manifest:
            current_delta.manifest = None

    result.sort(reverse=(not ascending_order), key=lambda d: d.stream_position)
    return ListResult.of(result, None, None)


def get_delta(
    namespace: str,
    table_name: str,
    stream_position: int,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> Optional[Delta]:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    stream = get_stream(namespace, table_name, table_version, *args, **kwargs)

    if partition_values is not None and partition_filter is not None:
        raise ValueError(
            "Only one of partition_values or partition_filter must be provided"
        )

    if partition_filter is not None:
        partition_values = partition_filter.partition_values

    partition = get_partition(stream.locator, partition_values, *args, **kwargs)
    delta_locator = DeltaLocator.of(partition.locator, stream_position)

    res = cur.execute(
        "SELECT * FROM deltas WHERE locator = ?", (delta_locator.canonical_string(),)
    )

    serialized_delta = res.fetchone()
    if serialized_delta is None:
        return None

    delta = Delta(json.loads(serialized_delta[2]))

    if not include_manifest:
        delta.manifest = None

    return delta


def get_latest_delta(
    namespace: str,
    table_name: str,
    partition_values: Optional[PartitionValues] = None,
    table_version: Optional[str] = None,
    include_manifest: bool = False,
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> Optional[Delta]:

    deltas = list_deltas(
        namespace=namespace,
        table_name=table_name,
        partition_values=partition_values,
        table_version=table_version,
        first_stream_position=None,
        last_stream_position=None,
        ascending_order=False,
        include_manifest=include_manifest,
        partition_filter=partition_filter,
        *args,
        **kwargs,
    ).all_items()

    if not deltas:
        return None

    return deltas[0]


def download_delta(
    delta_like: Union[Delta, DeltaLocator],
    table_type: TableType = TableType.PYARROW,
    storage_type: StorageType = StorageType.DISTRIBUTED,
    max_parallelism: Optional[int] = None,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: DistributedDatasetType = DistributedDatasetType.RAY_DATASET,
    partition_filter: Optional[PartitionFilter] = None,
    *args,
    **kwargs,
) -> Union[LocalDataset, DistributedDataset]:  # type: ignore
    result = []
    if isinstance(delta_like, Delta) and delta_like.manifest is not None:
        manifest = Delta(delta_like).manifest
    else:
        manifest = get_delta_manifest(delta_like, *args, **kwargs)
    partition_values: PartitionValues = None
    if partition_filter is not None:
        partition_values = partition_filter.partition_values
    for entry_index in range(len(manifest.entries)):
        if (
            partition_values is not None
            and partition_values != manifest.entries[entry_index].meta.partition_values
        ):
            continue

        result.append(
            download_delta_manifest_entry(
                delta_like=delta_like,
                entry_index=entry_index,
                table_type=table_type,
                columns=columns,
                file_reader_kwargs_provider=file_reader_kwargs_provider,
                *args,
                **kwargs,
            )
        )

    if storage_type == StorageType.DISTRIBUTED:
        if distributed_dataset_type is DistributedDatasetType.DAFT:
            return daft.from_arrow(result)
        elif distributed_dataset_type is DistributedDatasetType.RAY_DATASET:
            return ray.data.from_arrow(result)
        else:
            raise ValueError(f"Dataset type {distributed_dataset_type} not supported!")

    return result


def download_delta_manifest_entry(
    delta_like: Union[Delta, DeltaLocator],
    entry_index: int,
    table_type: TableType = TableType.PYARROW,
    columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    *args,
    **kwargs,
) -> LocalTable:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    if isinstance(delta_like, Delta) and delta_like.manifest is not None:
        manifest = Delta(delta_like).manifest
    else:
        manifest = get_delta_manifest(delta_like, *args, **kwargs)
    if entry_index >= len(manifest.entries):
        raise IndexError(
            f"Manifest entry index {entry_index} does not exist. "
            f"Valid values: [0, {len(manifest.entries)}]"
        )

    entry = manifest.entries[entry_index]

    res = cur.execute("SELECT value FROM data WHERE uri = ?", (entry.uri,))
    serialized_data = res.fetchone()

    if serialized_data is None:
        raise ValueError(
            f"Invalid value of delta locator: {delta_like.canonical_string()}"
        )

    serialized_data = serialized_data[0]
    if entry.meta.content_type == ContentType.PARQUET:
        if table_type == TableType.PYARROW_PARQUET:
            table = pa.parquet.ParquetFile(io.BytesIO(serialized_data))
        else:
            table = pa.parquet.read_table(io.BytesIO(serialized_data), columns=columns)
    elif entry.meta.content_type == ContentType.UNESCAPED_TSV:
        assert (
            table_type != TableType.PYARROW_PARQUET
        ), f"uTSV table cannot be read as {table_type}"
        parse_options = pa.csv.ParseOptions(delimiter="\t")
        convert_options = pa.csv.ConvertOptions(
            null_values=[""], strings_can_be_null=True, include_columns=columns
        )
        table = pa.csv.read_csv(
            io.BytesIO(serialized_data),
            parse_options=parse_options,
            convert_options=convert_options,
        )
    else:
        raise ValueError(f"Content type: {entry.meta.content_type} not supported.")

    if table_type == TableType.PYARROW:
        return table
    elif table_type == TableType.PYARROW_PARQUET:
        return table
    elif table_type == TableType.NUMPY:
        raise NotImplementedError(f"Table type={table_type} not supported")
    elif table_type == TableType.PANDAS:
        return table.to_pandas()

    return table


def get_delta_manifest(
    delta_like: Union[Delta, DeltaLocator], *args, **kwargs
) -> Optional[Manifest]:
    delta = get_delta(
        namespace=delta_like.namespace,
        table_name=delta_like.table_name,
        stream_position=delta_like.stream_position,
        partition_values=delta_like.partition_values,
        table_version=delta_like.table_version,
        include_manifest=True,
        *args,
        **kwargs,
    )
    if not delta:
        return None

    return delta.manifest


def create_namespace(
    namespace: str, permissions: Dict[str, Any], *args, **kwargs
) -> Namespace:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    locator = NamespaceLocator.of(namespace)
    result = Namespace.of(locator, permissions)
    params = (locator.canonical_string(), json.dumps(result))
    cur.execute(CREATE_NAMESPACES_TABLE)
    cur.execute(CREATE_TABLES_TABLE)
    cur.execute(CREATE_TABLE_VERSIONS_TABLE)
    cur.execute(CREATE_STREAMS_TABLE)
    cur.execute(CREATE_PARTITIONS_TABLE)
    cur.execute(CREATE_DELTAS_TABLE)
    cur.execute(CREATE_DATA_TABLE)
    cur.execute("INSERT OR IGNORE INTO namespaces VALUES(?, ?)", params)
    con.commit()
    return result


def update_namespace(
    namespace: str,
    permissions: Optional[Dict[str, Any]] = None,
    new_namespace: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    assert new_namespace is None, "namespace name cannot be changed"
    cur, con = _get_sqlite3_cursor_con(kwargs)
    locator = NamespaceLocator.of(namespace)
    result = Namespace.of(locator, permissions)
    params = (json.dumps(result), locator.canonical_string())
    cur.execute("UPDATE namespaces SET value = ? WHERE locator = ?", params)
    con.commit()


def create_table_version(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    schema: Optional[Union[pa.Schema, str, bytes]] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    partition_keys: Optional[List[Dict[str, Any]]] = None,
    primary_key_column_names: Optional[Set[str]] = None,
    sort_keys: Optional[List[SortKey]] = None,
    table_version_description: Optional[str] = None,
    table_version_properties: Optional[Dict[str, str]] = None,
    table_permissions: Optional[Dict[str, Any]] = None,
    table_description: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    supported_content_types: Optional[List[ContentType]] = None,
    partition_spec: Optional[StreamPartitionSpec] = None,
    *args,
    **kwargs,
) -> Stream:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    if partition_keys is not None and partition_spec is not None:
        raise ValueError(
            "Only one of partition_keys or partition_spec must be provided"
        )
    if partition_spec is not None:
        assert (
            partition_spec.ordered_transforms is not None
        ), "Ordered transforms must be specified when partition_spec is specified"
        partition_keys = []
        for transform in partition_spec.ordered_transforms:
            assert transform.name == TransformName.IDENTITY, (
                "Local DeltaCAT storage does not support creating table versions "
                "with non identity transform partition spec"
            )
            transform_params: IdentityTransformParameters = transform.parameters
            partition_keys.append(transform_params.column_name)

    latest_version = get_latest_table_version(namespace, table_name, *args, **kwargs)
    if (
        table_version is not None
        and latest_version
        and int(latest_version.table_version) + 1 != int(table_version)
    ):
        raise AssertionError(
            f"Table version can only be incremented. Last version={latest_version.table_version}"
        )
    elif table_version is None:
        table_version = (
            (int(latest_version.table_version) + 1) if latest_version else "1"
        )

    table_locator = TableLocator.of(NamespaceLocator.of(namespace), table_name)
    table_obj = Table.of(
        table_locator, table_permissions, table_description, table_properties
    )
    table_version_locator = TableVersionLocator.of(
        table_locator=table_locator, table_version=table_version
    )

    stream_id = uuid.uuid4().__str__()

    if table_version_properties is None:
        table_version_properties = {}

    properties = {**table_version_properties, STREAM_ID_PROPERTY: stream_id}
    table_version_obj = TableVersion.of(
        table_version_locator,
        schema=schema,
        partition_keys=partition_keys,
        primary_key_columns=primary_key_column_names,
        description=table_version_description,
        properties=properties,
        sort_keys=sort_keys,
        content_types=supported_content_types,
    )
    stream_locator = StreamLocator.of(
        table_version_obj.locator, stream_id=stream_id, storage_type=STORAGE_TYPE
    )
    result_stream = Stream.of(
        stream_locator, partition_keys=partition_keys, state=CommitState.COMMITTED
    )

    params = (
        table_locator.canonical_string(),
        table_locator.namespace_locator.canonical_string(),
        json.dumps(table_obj),
    )
    cur.execute("INSERT OR IGNORE INTO tables VALUES (?, ?, ?)", params)
    params = (
        table_version_locator.canonical_string(),
        table_locator.canonical_string(),
        json.dumps(table_version_obj),
    )
    cur.execute("INSERT OR IGNORE INTO table_versions VALUES (?, ?, ?)", params)

    params = (
        stream_locator.canonical_string(),
        table_version_locator.canonical_string(),
        json.dumps(result_stream),
    )
    cur.execute("INSERT OR IGNORE INTO streams VALUES (?, ?, ?)", params)
    con.commit()
    return result_stream


def update_table(
    namespace: str,
    table_name: str,
    permissions: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    new_table_name: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    table_locator = TableLocator.of(NamespaceLocator.of(namespace), table_name)
    table_obj = Table.of(table_locator, permissions, description, properties)

    params = (table_locator.canonical_string(),)
    cur.execute("DELETE FROM tables WHERE locator = ?", params)
    params = (
        table_locator.canonical_string(),
        table_locator.namespace_locator.canonical_string(),
        json.dumps(table_obj),
    )
    cur.execute("INSERT INTO tables VALUES (?, ?, ?)", params)
    con.commit()


def update_table_version(
    namespace: str,
    table_name: str,
    table_version: str,
    lifecycle_state: Optional[LifecycleState] = None,
    schema: Optional[Union[pa.Schema, str, bytes]] = None,
    schema_consistency: Optional[Dict[str, SchemaConsistencyType]] = None,
    description: Optional[str] = None,
    properties: Optional[Dict[str, str]] = None,
    *args,
    **kwargs,
) -> None:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    table_locator = TableLocator.of(NamespaceLocator.of(namespace), table_name)
    table_version_locator = TableVersionLocator.of(
        table_locator=table_locator, table_version=table_version
    )

    res = cur.execute(
        "SELECT * from table_versions WHERE locator = ?",
        (table_version_locator.canonical_string(),),
    )
    serialized_table_version = res.fetchone()
    assert (
        serialized_table_version is not None
    ), f"Table version not found with locator={table_version_locator.canonical_string()}"
    current_table_version_obj = TableVersion(json.loads(serialized_table_version[2]))

    if properties is None:
        properties = {}

    current_props = (
        current_table_version_obj.properties
        if current_table_version_obj.properties
        else {}
    )

    tv_properties = {**properties, **current_props}
    table_version_obj = TableVersion.of(
        table_version_locator,
        schema=schema,
        partition_keys=current_table_version_obj.partition_keys,
        primary_key_columns=current_table_version_obj.primary_keys,
        description=description,
        properties=tv_properties,
        sort_keys=current_table_version_obj.sort_keys,
        content_types=current_table_version_obj.content_types,
    )

    params = (
        table_locator.canonical_string(),
        json.dumps(table_version_obj),
        table_version_locator.canonical_string(),
    )
    cur.execute(
        "UPDATE table_versions SET table_locator = ?, value = ? WHERE locator = ?",
        params,
    )
    con.commit()


def stage_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Stream:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    existing_table_version = get_table_version(
        namespace, table_name, table_version, *args, **kwargs
    )
    existing_stream = get_stream(namespace, table_name, table_version, *args, **kwargs)

    stream_id = uuid.uuid4().__str__()
    new_stream_locator = StreamLocator.of(
        existing_table_version.locator, stream_id, STORAGE_TYPE
    )
    new_stream = Stream.of(
        new_stream_locator,
        existing_stream.partition_keys,
        CommitState.STAGED,
        existing_stream.locator.canonical_string(),
    )

    params = (
        new_stream_locator.canonical_string(),
        existing_table_version.locator.canonical_string(),
        json.dumps(new_stream),
    )
    cur.execute("INSERT INTO streams VALUES (?, ?, ?)", params)
    con.commit()

    return new_stream


def commit_stream(stream: Stream, *args, **kwargs) -> Stream:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    existing_table_version = get_table_version(
        stream.namespace, stream.table_name, stream.table_version, *args, **kwargs
    )
    stream_to_commit = Stream.of(
        stream.locator,
        stream.partition_keys,
        CommitState.COMMITTED,
        stream.previous_stream_digest,
    )

    existing_table_version.properties[
        STREAM_ID_PROPERTY
    ] = stream_to_commit.locator.stream_id

    params = (
        json.dumps(existing_table_version),
        existing_table_version.locator.canonical_string(),
    )
    cur.execute("UPDATE table_versions SET value = ? WHERE locator = ?", params)
    params = (json.dumps(stream_to_commit), stream_to_commit.locator.canonical_string())
    cur.execute("UPDATE streams SET value = ? WHERE locator = ?", params)
    con.commit()

    return stream_to_commit


def delete_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> None:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    table_version_locator = TableVersionLocator.of(
        TableLocator.of(NamespaceLocator.of(namespace), table_name), table_version
    )

    res = cur.execute(
        "SELECT locator FROM streams WHERE table_version_locator = ?",
        (table_version_locator.canonical_string(),),
    )
    locators = res.fetchall()
    cur.executemany("DELETE FROM streams WHERE locator = ?", locators)
    cur.execute(
        "DELETE FROM table_versions WHERE locator = ?",
        (table_version_locator.canonical_string(),),
    )

    con.commit()


def stage_partition(
    stream: Stream, partition_values: Optional[PartitionValues] = None, *args, **kwargs
) -> Partition:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    partition_id = uuid.uuid4().__str__()
    partition_locator = PartitionLocator.of(
        stream.locator, partition_values=partition_values, partition_id=partition_id
    )

    tv = get_table_version(
        stream.namespace, stream.table_name, stream.table_version, *args, **kwargs
    )

    pv_partition = get_partition(
        stream.locator, partition_values=partition_values, *args, **kwargs
    )

    stream_position = current_time_ms()
    partition = Partition.of(
        partition_locator,
        schema=tv.schema,
        content_types=tv.content_types,
        state=CommitState.STAGED,
        previous_stream_position=pv_partition.stream_position if pv_partition else None,
        previous_partition_id=pv_partition.partition_id if pv_partition else None,
        stream_position=stream_position,
    )

    params = (
        partition.locator.canonical_string(),
        partition.stream_locator.canonical_string(),
        json.dumps(partition),
    )
    cur.execute("INSERT INTO partitions VALUES (?, ?, ?)", params)
    con.commit()

    return partition


def commit_partition(
    partition: Partition,
    previous_partition: Optional[Partition] = None,
    *args,
    **kwargs,
) -> Partition:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    pv_partition: Optional[Partition] = previous_partition or get_partition(
        partition.stream_locator,
        partition_values=partition.partition_values,
        *args,
        **kwargs,
    )
    # deprecate old partition and commit new one
    if pv_partition:
        pv_partition.state = CommitState.DEPRECATED
        params = (json.dumps(pv_partition), pv_partition.locator.canonical_string())
        cur.execute("UPDATE partitions SET value = ? WHERE locator = ?", params)
    previous_partition_deltas = (
        list_partition_deltas(
            pv_partition, ascending_order=False, *args, **kwargs
        ).all_items()
        or []
    )

    partition_deltas: Optional[List[Delta]] = (
        list_partition_deltas(
            partition, ascending_order=False, *args, **kwargs
        ).all_items()
        or []
    )

    # if previous_partition is passed in, table is in-place compacted and we need to run merge-and-promote
    if previous_partition:
        partition_deltas = _merge_and_promote(
            partition_deltas, previous_partition_deltas
        )

    stream_position = (
        partition_deltas[0].stream_position
        if partition_deltas
        else partition.stream_position
    )

    partition.stream_position = stream_position
    if partition_deltas:
        partition.locator = partition_deltas[0].partition_locator

    partition.state = CommitState.COMMITTED
    partition.previous_stream_position = (
        pv_partition.stream_position if pv_partition else None
    )
    params = (json.dumps(partition), partition.locator.canonical_string())
    cur.execute("UPDATE partitions SET value = ? WHERE locator = ?", params)
    con.commit()

    return partition


def delete_partition(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> None:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    stream = get_stream(namespace, table_name, table_version, *args, **kwargs)
    partition = get_partition(stream.locator, partition_values, *args, **kwargs)

    partition.state = CommitState.DEPRECATED
    params = (json.dumps(partition), partition.locator.canonical_string())

    cur.execute("UPDATE partitions SET value = ? WHERE locator = ?", params)
    con.commit()


def get_partition(
    stream_locator: StreamLocator,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> Optional[Partition]:
    cur, con = _get_sqlite3_cursor_con(kwargs)

    res = cur.execute(
        "SELECT * FROM partitions WHERE stream_locator = ?",
        (stream_locator.canonical_string(),),
    )

    serialized_partitions = res.fetchall()

    if not serialized_partitions:
        return None

    if partition_values is None:
        partition_values = []

    prior_pv = ",".join(partition_values)

    for item in serialized_partitions:
        partition = Partition(json.loads(item[2]))
        pv = ",".join(partition.partition_values if partition.partition_values else [])

        if pv == prior_pv and partition.state == CommitState.COMMITTED:
            return partition

    return None


def stage_delta(
    data: Union[LocalTable, LocalDataset, DistributedDataset, Manifest],
    partition: Partition,
    delta_type: DeltaType = DeltaType.UPSERT,
    max_records_per_entry: Optional[int] = None,
    author: Optional[ManifestAuthor] = None,
    properties: Optional[Dict[str, str]] = None,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    content_type: ContentType = ContentType.PARQUET,
    delete_parameters: Optional[DeleteParameters] = None,
    partition_spec: Optional[DeltaPartitionSpec] = None,
    partition_values: Optional[PartitionValues] = None,
    *args,
    **kwargs,
) -> Delta:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    manifest_id = uuid.uuid4().__str__()
    uri = _get_manifest_entry_uri(manifest_id)

    if data is None:
        delta = create_empty_delta(
            partition,
            delta_type,
            author,
            properties=properties,
            manifest_entry_id=manifest_id,
        )
        cur.execute("INSERT OR IGNORE INTO data VALUES (?, ?)", (uri, None))
        params = (delta.locator.canonical_string(), "staged_delta", json.dumps(delta))
        cur.execute("INSERT OR IGNORE INTO deltas VALUES (?, ?, ?)", params)
        con.commit()
        return delta

    if partition_spec:
        assert partition_values is not None, (
            "partition_values must be provided as local "
            "storage does not support computing it from input data"
        )

    serialized_data = None
    if content_type == ContentType.PARQUET:
        buffer = io.BytesIO()
        pa.parquet.write_table(data, buffer)
        serialized_data = buffer.getvalue()
    elif content_type == ContentType.UNESCAPED_TSV:
        buffer = io.BytesIO()
        write_options = pa.csv.WriteOptions(
            include_header=True, delimiter="\t", quoting_style="none"
        )
        pa.csv.write_csv(data, buffer, write_options=write_options)
        serialized_data = buffer.getvalue()
    else:
        raise ValueError(f"Unsupported content type: {content_type}")

    stream_position = current_time_ms()
    delta_locator = DeltaLocator.of(partition.locator, stream_position=stream_position)

    meta = ManifestMeta.of(
        len(data),
        len(serialized_data),
        content_type=content_type,
        content_encoding=ContentEncoding.IDENTITY,
        source_content_length=data.nbytes,
        partition_values=partition_values,
    )

    manifest = Manifest.of(
        entries=ManifestEntryList.of(
            [
                ManifestEntry.of(
                    uri=uri, url=uri, meta=meta, mandatory=True, uuid=manifest_id
                )
            ]
        ),
        author=author,
        uuid=manifest_id,
    )

    delta = Delta.of(
        delta_locator,
        delta_type=delta_type,
        meta=meta,
        properties=properties,
        manifest=manifest,
        previous_stream_position=partition.stream_position,
        delete_parameters=delete_parameters,
    )

    params = (uri, serialized_data)
    cur.execute("INSERT OR IGNORE INTO data VALUES (?, ?)", params)

    params = (delta_locator.canonical_string(), "staged_delta", json.dumps(delta))
    cur.execute("INSERT OR IGNORE INTO deltas VALUES (?, ?, ?)", params)

    con.commit()
    return delta


def commit_delta(delta: Delta, *args, **kwargs) -> Delta:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    delta_stream_position: Optional[int] = delta.stream_position
    delta.locator.stream_position = delta_stream_position or current_time_ms()

    params = (
        delta.locator.canonical_string(),
        delta.partition_locator.canonical_string(),
        json.dumps(delta),
    )

    cur.execute("INSERT OR IGNORE INTO deltas VALUES (?, ?, ?)", params)

    params = (
        delta.partition_locator.canonical_string(),
        json.dumps(delta),
        delta.locator.canonical_string(),
    )
    cur.execute(
        "UPDATE deltas SET partition_locator = ?, value = ? WHERE locator = ?", params
    )
    con.commit()
    return delta


def get_namespace(namespace: str, *args, **kwargs) -> Optional[Namespace]:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    locator = NamespaceLocator.of(namespace)

    res = cur.execute(
        "SELECT * FROM namespaces WHERE locator = ?", (locator.canonical_string(),)
    )
    serialized_result = res.fetchone()

    if serialized_result is None:
        return None

    return Namespace(json.loads(serialized_result[1]))


def namespace_exists(namespace: str, *args, **kwargs) -> bool:
    obj = get_namespace(namespace, *args, **kwargs)

    return obj is not None


def get_table(namespace: str, table_name: str, *args, **kwargs) -> Optional[Table]:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    locator = TableLocator.of(NamespaceLocator.of(namespace), table_name)

    res = cur.execute(
        "SELECT * FROM tables WHERE locator = ?", (locator.canonical_string(),)
    )
    serialized_result = res.fetchone()

    if serialized_result is None:
        return None

    return Table(json.loads(serialized_result[2]))


def table_exists(namespace: str, table_name: str, *args, **kwargs) -> bool:
    obj = get_table(namespace, table_name, *args, **kwargs)

    return obj is not None


def get_table_version(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> Optional[TableVersion]:
    cur, con = _get_sqlite3_cursor_con(kwargs)
    locator = TableVersionLocator.of(
        TableLocator.of(NamespaceLocator.of(namespace), table_name), table_version
    )

    res = cur.execute(
        "SELECT * FROM table_versions WHERE locator = ?", (locator.canonical_string(),)
    )
    serialized_table_version = res.fetchone()

    if serialized_table_version is None:
        return None

    return TableVersion(json.loads(serialized_table_version[2]))


def get_latest_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:
    table_versions = list_table_versions(
        namespace, table_name, *args, **kwargs
    ).all_items()
    if not table_versions:
        return None

    table_versions.sort(reverse=True, key=lambda v: int(v.table_version))
    return table_versions[0]


def get_latest_active_table_version(
    namespace: str, table_name: str, *args, **kwargs
) -> Optional[TableVersion]:

    # This module does not support table version lifecycle state
    return get_latest_table_version(namespace, table_name, *args, **kwargs)


def get_table_version_schema(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Union[pa.Schema, str, bytes]]:
    obj = get_table_version(namespace, table_name, table_version, *args, **kwargs)

    return obj.schema


def table_version_exists(
    namespace: str, table_name: str, table_version: str, *args, **kwargs
) -> bool:
    obj = get_table_version(namespace, table_name, table_version, *args, **kwargs)

    return obj is not None


def get_stream(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[Stream]:
    assert not isinstance(table_version, int), f"Passed an integer as the table version"
    obj = get_table_version(namespace, table_name, table_version, *args, **kwargs)

    if obj is None:
        return None

    stream_id = obj.properties.get(STREAM_ID_PROPERTY)
    if stream_id is None:
        return None

    cur, con = _get_sqlite3_cursor_con(kwargs)
    stream_locator = StreamLocator.of(
        obj.locator, stream_id=stream_id, storage_type=STORAGE_TYPE
    )
    res = cur.execute(
        "SELECT * FROM streams WHERE locator = ?", (stream_locator.canonical_string(),)
    )

    serialized_stream = res.fetchone()
    if serialized_stream is None:
        return None

    return Stream(json.loads(serialized_stream[2]))


def get_table_version_column_names(
    namespace: str,
    table_name: str,
    table_version: Optional[str] = None,
    *args,
    **kwargs,
) -> Optional[List[str]]:
    raise NotImplementedError("Fetching column names is not supported")


def can_categorize(e: BaseException, **kwargs) -> bool:
    if isinstance(e, InvalidNamespaceError):
        return True
    else:
        return False


def raise_categorized_error(e: BaseException, **kwargs):
    if isinstance(e, InvalidNamespaceError):
        raise LocalStorageValidationError("Namespace provided is invalid!")
