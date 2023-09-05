from typing import List
import pyarrow as pa
from deltacat.storage import Delta
import deltacat.tests.local_deltacat_storage as ds


def create_delta_from_csv_file(
    namespace: str, file_paths: List[str], *args, **kwargs
) -> Delta:
    tables = []

    for file_path in file_paths:
        table = pa.csv.read_csv(file_path)
        tables.append(table)

    ds.create_namespace(namespace, {}, **kwargs)
    table_name = "-".join(file_paths).replace("/", "_")
    ds.create_table_version(namespace, table_name, "1", **kwargs)
    stream = ds.get_stream(namespace, table_name, "1", **kwargs)
    staged_partition = ds.stage_partition(stream, [], **kwargs)

    deltas = []

    for table in tables:
        delta = ds.stage_delta(table, staged_partition, **kwargs)
        deltas.append(delta)

    merged_delta = Delta.merge_deltas(deltas=deltas)
    committed_delta = ds.commit_delta(merged_delta, **kwargs)
    ds.commit_partition(staged_partition, **kwargs)

    return committed_delta
