import itertools
import uuid

import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Iterable, Iterator, List

from pyarrow.fs import FileSystem

from pyiceberg.io.pyarrow import schema_to_pyarrow, fill_parquet_file_metadata, \
    compute_statistics_plan, parquet_path_to_id_mapping
from pyiceberg.table import Table, _MergingSnapshotProducer, WriteTask
from pyiceberg.table.snapshots import Operation
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.types import StructType, NestedField, IntegerType
from pyiceberg.typedef import Record


def append(table: Table, paths: List[str]) -> None:
    """
    Append files to the table.
    """
    #if len(table.sort_order().fields) > 0:
    #    raise ValueError("Cannot write to tables with a sort-order")

    data_files = write_file(table, paths)
    merge = _MergingSnapshotProducer(operation=Operation.APPEND, table=table)
    for data_file in data_files:
        merge.append_data_file(data_file)

    merge.commit()


def append(table: Table, paths: List[str]) -> None:
    """
    Append files to the table.
    """
    #if len(table.sort_order().fields) > 0:
    #    raise ValueError("Cannot write to tables with a sort-order")

    data_files = write_file(table, paths)
    merge = _MergingSnapshotProducer(operation=Operation.APPEND, table=table)
    for data_file in data_files:
        merge.append_data_file(data_file)

    merge.commit()


def write_file(table: Table, paths: Iterator[str]) -> Iterator[DataFile]:
    data_files = []
    for file_path in paths:
        partition_dir = file_path.split("/")[-2]
        partition_value = int(partition_dir.split("=")[-1])
        fs_tuple = FileSystem.from_uri(file_path)
        fs = fs_tuple[0]
        fs_path = fs_tuple[1]
        with fs.open_input_file(fs_path) as native_file:
            parquet_metadata = pq.read_metadata(native_file)
            data_file = DataFile(
                content=DataFileContent.DATA,
                file_path=file_path,
                file_format=FileFormat.PARQUET,
                partition=Record(**{"struct": StructType(NestedField(0, table.spec().fields[0].name, IntegerType(), required=False)), **{table.spec().fields[0].name: partition_value}}),
                file_size_in_bytes=native_file.size(),
                sort_order_id=None,
                spec_id=table.spec().spec_id,
                equality_ids=None,
                key_metadata=None,
            )
            fill_parquet_file_metadata(
                data_file=data_file,
                parquet_metadata=parquet_metadata,
                stats_columns=compute_statistics_plan(table.schema(), table.properties),
                parquet_column_mapping=parquet_path_to_id_mapping(table.schema()),
            )
            data_files.append(data_file)
    return data_files

