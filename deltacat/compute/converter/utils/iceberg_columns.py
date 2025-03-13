import pyarrow as pa
from typing import Union

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
