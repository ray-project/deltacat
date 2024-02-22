from typing import List, Optional
import pyarrow as pa
from deltacat.storage import Delta, Partition, PartitionLocator
import deltacat.tests.local_deltacat_storage as ds


def create_delta_from_csv_file(
    namespace: str,
    file_paths: List[str],
    table_name: Optional[str] = None,
    *args,
    **kwargs
) -> Delta:
    staged_partition = stage_partition_from_file_paths(
        namespace, file_paths, *args, table_name=table_name, **kwargs
    )

    committed_delta = commit_delta_to_staged_partition(
        staged_partition, file_paths, *args, **kwargs
    )

    return committed_delta


def stage_partition_from_file_paths(
    namespace: str,
    file_paths: List[str],
    table_name: Optional[str] = None,
    *args,
    **kwargs
) -> Partition:
    ds.create_namespace(namespace, {}, **kwargs)
    if table_name is None:
        table_name = "-".join(file_paths).replace("/", "_")
    ds.create_table_version(namespace, table_name, "1", **kwargs)
    stream = ds.get_stream(namespace, table_name, "1", **kwargs)
    staged_partition = ds.stage_partition(stream, [], **kwargs)
    return staged_partition


def commit_delta_to_staged_partition(
    staged_partition, file_paths: List[str], *args, **kwargs
) -> Delta:
    committed_delta = commit_delta_to_partition(
        staged_partition, file_paths=file_paths, *args, **kwargs
    )
    ds.commit_partition(staged_partition, **kwargs)
    return committed_delta


def commit_delta_to_partition(
    partition: Partition, file_paths: List[str], *args, **kwargs
) -> Delta:
    tables = []

    if isinstance(partition, PartitionLocator):
        partition = ds.get_partition(
            partition.stream_locator, partition.partition_values, *args, **kwargs
        )

    for file_path in file_paths:
        table = pa.csv.read_csv(file_path)
        tables.append(table)

    table = pa.concat_tables(tables)
    staged_delta = ds.stage_delta(table, partition, **kwargs)

    return ds.commit_delta(staged_delta, **kwargs)
