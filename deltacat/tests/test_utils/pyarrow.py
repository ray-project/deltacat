from typing import List, Optional, Union
import pyarrow as pa
from deltacat.storage import Delta, Partition, PartitionLocator, DeltaLocator
import deltacat.tests.local_deltacat_storage as ds
from deltacat.types.media import StorageType, ContentType


def create_delta_from_csv_file(
    namespace: str,
    file_paths: List[str],
    table_name: Optional[str] = None,
    table_version: int = 1,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> Delta:
    staged_partition = stage_partition_from_file_paths(
        namespace,
        file_paths,
        *args,
        table_name=table_name,
        table_version=table_version,
        **kwargs,
    )
    committed_delta = commit_delta_to_staged_partition(
        staged_partition, file_paths, content_type=content_type, *args, **kwargs
    )
    return committed_delta


def stage_partition_from_file_paths(
    namespace: str,
    file_paths: List[str],
    table_name: Optional[str] = None,
    table_version: int = 1,
    *args,
    **kwargs,
) -> Partition:
    ds.create_namespace(namespace, {}, **kwargs)
    if table_name is None:
        table_name = "-".join(file_paths).replace("/", "_")
    ds.create_table_version(namespace, table_name, str(table_version), **kwargs)
    stream = ds.get_stream(namespace, table_name, str(table_version), **kwargs)
    staged_partition = ds.stage_partition(stream, [], **kwargs)
    return staged_partition


def commit_delta_to_staged_partition(
    staged_partition,
    file_paths: List[str] = None,
    pa_table: pa.Table = None,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> Delta:
    committed_delta = commit_delta_to_partition(
        staged_partition,
        *args,
        file_paths=file_paths,
        content_type=content_type,
        pa_table=pa_table,
        **kwargs,
    )
    ds.commit_partition(staged_partition, **kwargs)
    return committed_delta


def download_delta(delta_like: Union[Delta, DeltaLocator], *args, **kwargs) -> Delta:
    return pa.concat_tables(
        ds.download_delta(
            delta_like,
            storage_type=StorageType.LOCAL,
            *args,
            **kwargs,
        )
    )


def commit_delta_to_partition(
    partition: Union[Partition, PartitionLocator],
    file_paths: List[str] = None,
    pa_table: pa.Table = None,
    content_type: ContentType = ContentType.PARQUET,
    *args,
    **kwargs,
) -> Delta:

    if isinstance(partition, PartitionLocator):
        partition = ds.get_partition(
            partition.stream_locator, partition.partition_values, *args, **kwargs
        )
    if pa_table is None:
        assert file_paths is not None, "One of pa_table or file_paths must be passed."
        tables = []
        for file_path in file_paths:
            table = pa.csv.read_csv(file_path)
            tables.append(table)

        pa_table = pa.concat_tables(tables)

    staged_delta = ds.stage_delta(
        pa_table, partition, content_type=content_type, **kwargs
    )

    return ds.commit_delta(staged_delta, **kwargs)
