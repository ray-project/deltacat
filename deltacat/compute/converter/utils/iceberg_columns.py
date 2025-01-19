import pyarrow as pa
from typing import Union


def _get_iceberg_col_name(suffix):
    return f"{suffix}"


_ORDERED_RECORD_IDX_COLUMN_NAME = _get_iceberg_col_name("pos")
_ORDERED_RECORD_IDX_COLUMN_TYPE = pa.int64()
_ORDERED_RECORD_IDX_COLUMN_FIELD = pa.field(
    _ORDERED_RECORD_IDX_COLUMN_NAME,
    _ORDERED_RECORD_IDX_COLUMN_TYPE,
)


def get_record_index_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _ORDERED_RECORD_IDX_COLUMN_TYPE,
    )


def append_record_idx_col(table: pa.Table, ordered_record_indices) -> pa.Table:

    table = table.append_column(
        _ORDERED_RECORD_IDX_COLUMN_FIELD,
        get_record_index_column_array(ordered_record_indices),
    )
    return table


_FILE_PATH_COLUMN_NAME = _get_iceberg_col_name("file_path")
_FILE_PATH_COLUMN_TYPE = pa.string()
_FILE_PATH_COLUMN_FIELD = pa.field(
    _FILE_PATH_COLUMN_NAME,
    _FILE_PATH_COLUMN_TYPE,
)
