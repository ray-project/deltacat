import pyarrow as pa
from typing import Union
import numpy as np

# Refer to: https://iceberg.apache.org/spec/#reserved-field-ids for reserved field ids
ICEBERG_RESERVED_FIELD_ID_FOR_FILE_PATH_COLUMN = 2147483546

# Refer to: https://iceberg.apache.org/spec/#reserved-field-ids for reserved field ids
ICEBERG_RESERVED_FIELD_ID_FOR_POS_COLUMN = 2147483545


def _get_iceberg_col_name(suffix):
    return suffix


_ORDERED_RECORD_IDX_COLUMN_NAME = _get_iceberg_col_name("pos")
_ORDERED_RECORD_IDX_COLUMN_TYPE = pa.int64()
_ORDERED_RECORD_IDX_FIELD_METADATA = {
    b"PARQUET:field_id": f"{ICEBERG_RESERVED_FIELD_ID_FOR_POS_COLUMN}"
}
_ORDERED_RECORD_IDX_COLUMN_FIELD = pa.field(
    _ORDERED_RECORD_IDX_COLUMN_NAME,
    _ORDERED_RECORD_IDX_COLUMN_TYPE,
    metadata=_ORDERED_RECORD_IDX_FIELD_METADATA,
    nullable=False,
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
_FILE_PATH_FIELD_METADATA = {
    b"PARQUET:field_id": f"{ICEBERG_RESERVED_FIELD_ID_FOR_FILE_PATH_COLUMN}"
}
_FILE_PATH_COLUMN_FIELD = pa.field(
    _FILE_PATH_COLUMN_NAME,
    _FILE_PATH_COLUMN_TYPE,
    metadata=_FILE_PATH_FIELD_METADATA,
    nullable=False,
)


def append_file_path_column(table: pa.Table, file_path: str):
    table = table.append_column(
        _FILE_PATH_COLUMN_FIELD,
        pa.array(np.repeat(file_path, len(table)), _FILE_PATH_COLUMN_TYPE),
    )
    return table


_GLOBAL_RECORD_IDX_COLUMN_NAME = _get_iceberg_col_name("global_record_index")
_GLOBAL_RECORD_IDX_COLUMN_TYPE = pa.int64()
_GLOBAL_RECORD_IDX_COLUMN_FIELD = pa.field(
    _GLOBAL_RECORD_IDX_COLUMN_NAME,
    _GLOBAL_RECORD_IDX_COLUMN_TYPE,
)


def append_global_record_idx_column(
    table: pa.Table, ordered_record_indices
) -> pa.Table:

    table = table.append_column(
        _GLOBAL_RECORD_IDX_COLUMN_NAME,
        pa.array(ordered_record_indices, _GLOBAL_RECORD_IDX_COLUMN_TYPE),
    )
    return table
