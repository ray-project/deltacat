from typing import List, Optional, Union
import pyarrow as pa
from deltacat.storage import Delta, Partition, PartitionLocator, DeltaLocator
from deltacat.storage import metastore
from deltacat.types.media import StorageType, ContentType
from deltacat.storage.model.schema import Schema


def create_delta_from_csv_file(
    namespace: str,
    file_paths: List[str],
    table_name: Optional[str] = None,
    table_version: int = 1,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> Delta:
    assert file_paths is not None, "file_paths cannot be empty"
    pa_table = create_table_from_csv_file_paths(file_paths)
    schema = Schema.of(pa_table.schema)
    staged_partition = stage_partition_from_file_paths(
        namespace,
        file_paths,
        schema,
        *args,
        table_name=table_name,
        table_version=table_version,
        **kwargs,
    )
    committed_delta = commit_delta_to_staged_partition(
        staged_partition,
        pa_table,
        content_type,
        *args,
        **kwargs,
    )
    return committed_delta


def create_table_from_csv_file_paths(
    file_paths: List[str],
) -> pa.Table:
    tables = []
    for file_path in file_paths:
        table = pa.csv.read_csv(file_path)
        tables.append(table)
    return pa.concat_tables(tables)


def stage_partition_from_file_paths(
    namespace: str,
    file_paths: List[str],
    schema: Schema,
    table_name: Optional[str] = None,
    table_version: int = 1,
    *args,
    **kwargs,
) -> Partition:
    if not metastore.namespace_exists(namespace, **kwargs):
        metastore.create_namespace(namespace, **kwargs)
    if table_name is None:
        table_name = "-".join(file_paths).replace("/", "_")
    metastore.create_table_version(
        namespace,
        table_name,
        str(table_version),
        schema=schema,
        **kwargs,
    )
    stream = metastore.get_stream(
        namespace,
        table_name,
        str(table_version),
        **kwargs,
    )
    staged_partition = metastore.stage_partition(stream, **kwargs)
    return staged_partition


def commit_delta_to_staged_partition(
    staged_partition,
    pa_table: pa.Table,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> Delta:
    committed_delta = commit_delta_to_partition(
        staged_partition,
        pa_table,
        content_type,
        *args,
        **kwargs,
    )
    metastore.commit_partition(staged_partition, **kwargs)
    return committed_delta


def download_delta(delta_like: Union[Delta, DeltaLocator], *args, **kwargs) -> Delta:
    return pa.concat_tables(
        metastore.download_delta(
            delta_like,
            storage_type=StorageType.LOCAL,
            *args,
            **kwargs,
        )
    )


def commit_delta_to_partition(
    partition: Union[Partition, PartitionLocator],
    pa_table: pa.Table = None,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> Delta:

    if isinstance(partition, PartitionLocator):
        partition = metastore.get_partition(
            partition.stream_locator, partition.partition_values, *args, **kwargs
        )

    staged_delta = metastore.stage_delta(
        pa_table,
        partition,
        content_type=content_type,
        **kwargs,
    )

    return metastore.commit_delta(staged_delta, **kwargs)
