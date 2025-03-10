from itertools import repeat
from typing import Union

import numpy as np
import pyarrow as pa

from deltacat.compute.compactor import DeltaFileEnvelope
from deltacat.storage import DeltaType

_SYS_COL_UUID = "4000f124-dfbd-48c6-885b-7b22621a6d41"


def _get_sys_col_name(suffix):
    return f"{_SYS_COL_UUID}_{suffix}"


_PK_HASH_DIGEST_BYTE_WIDTH = 20
_PK_HASH_COLUMN_NAME = _get_sys_col_name("hash")
_PK_HASH_COLUMN_TYPE = pa.binary(_PK_HASH_DIGEST_BYTE_WIDTH)
_PK_HASH_COLUMN_FIELD = pa.field(
    _PK_HASH_COLUMN_NAME,
    _PK_HASH_COLUMN_TYPE,
)

_PK_HASH_STRING_COLUMN_NAME = _get_sys_col_name("hash_str")
_PK_HASH_STRING_COLUMN_TYPE = pa.string()
_PK_HASH_STRING_COLUMN_FIELD = pa.field(
    _PK_HASH_STRING_COLUMN_NAME,
    _PK_HASH_STRING_COLUMN_TYPE,
)

_DEDUPE_TASK_IDX_COLUMN_NAME = _get_sys_col_name("dedupe_task_idx")
_DEDUPE_TASK_IDX_COLUMN_TYPE = pa.int32()
_DEDUPE_TASK_IDX_COLUMN_FIELD = pa.field(
    _DEDUPE_TASK_IDX_COLUMN_NAME,
    _DEDUPE_TASK_IDX_COLUMN_TYPE,
)

_PARTITION_STREAM_POSITION_COLUMN_NAME = _get_sys_col_name("stream_position")
_PARTITION_STREAM_POSITION_COLUMN_TYPE = pa.int64()
_PARTITION_STREAM_POSITION_COLUMN_FIELD = pa.field(
    _PARTITION_STREAM_POSITION_COLUMN_NAME,
    _PARTITION_STREAM_POSITION_COLUMN_TYPE,
)

_HASH_BUCKET_IDX_COLUMN_NAME = _get_sys_col_name("hash_bucket_idx")
_HASH_BUCKET_IDX_COLUMN_TYPE = pa.int32()
_HASH_BUCKET_IDX_COLUMN_FIELD = pa.field(
    _HASH_BUCKET_IDX_COLUMN_NAME, _HASH_BUCKET_IDX_COLUMN_TYPE
)

_ORDERED_FILE_IDX_COLUMN_NAME = _get_sys_col_name("file_index")
_ORDERED_FILE_IDX_COLUMN_TYPE = pa.int32()
_ORDERED_FILE_IDX_COLUMN_FIELD = pa.field(
    _ORDERED_FILE_IDX_COLUMN_NAME,
    _ORDERED_FILE_IDX_COLUMN_TYPE,
)

_ORDERED_RECORD_IDX_COLUMN_NAME = _get_sys_col_name("record_index")
_ORDERED_RECORD_IDX_COLUMN_TYPE = pa.int64()
_ORDERED_RECORD_IDX_COLUMN_FIELD = pa.field(
    _ORDERED_RECORD_IDX_COLUMN_NAME,
    _ORDERED_RECORD_IDX_COLUMN_TYPE,
)

_DELTA_TYPE_COLUMN_NAME = _get_sys_col_name("delta_type")
_DELTA_TYPE_COLUMN_TYPE = pa.bool_()
_DELTA_TYPE_COLUMN_FIELD = pa.field(
    _DELTA_TYPE_COLUMN_NAME,
    _DELTA_TYPE_COLUMN_TYPE,
)

_IS_SOURCE_COLUMN_NAME = _get_sys_col_name("is_source")
_IS_SOURCE_COLUMN_TYPE = pa.bool_()
_IS_SOURCE_COLUMN_FIELD = pa.field(
    _IS_SOURCE_COLUMN_NAME,
    _IS_SOURCE_COLUMN_TYPE,
)

_FILE_RECORD_COUNT_COLUMN_NAME = _get_sys_col_name("file_record_count")
_FILE_RECORD_COUNT_COLUMN_TYPE = pa.int64()
_FILE_RECORD_COUNT_COLUMN_FIELD = pa.field(
    _FILE_RECORD_COUNT_COLUMN_NAME,
    _FILE_RECORD_COUNT_COLUMN_TYPE,
)


def get_pk_hash_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(obj, _PK_HASH_COLUMN_TYPE)


def get_pk_hash_string_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(obj, _PK_HASH_STRING_COLUMN_TYPE)


def pk_hash_column_np(table: pa.Table) -> np.ndarray:
    return table[_PK_HASH_COLUMN_NAME].to_numpy()


def pk_hash_string_column_np(table: pa.Table) -> np.ndarray:
    return table[_PK_HASH_STRING_COLUMN_NAME].to_numpy()


def pk_hash_column(table: pa.Table) -> pa.ChunkedArray:
    return table[_PK_HASH_COLUMN_NAME]


def delta_type_column_np(table: pa.Table) -> np.ndarray:
    return table[_DELTA_TYPE_COLUMN_NAME].to_numpy()


def delta_type_column(table: pa.Table) -> pa.ChunkedArray:
    return table[_DELTA_TYPE_COLUMN_NAME]


def get_dedupe_task_idx_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _DEDUPE_TASK_IDX_COLUMN_TYPE,
    )


def get_stream_position_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _PARTITION_STREAM_POSITION_COLUMN_TYPE,
    )


def stream_position_column_np(table: pa.Table) -> np.ndarray:
    return table[_PARTITION_STREAM_POSITION_COLUMN_NAME].to_numpy()


def get_file_index_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _ORDERED_FILE_IDX_COLUMN_TYPE,
    )


def file_index_column_np(table: pa.Table) -> np.ndarray:
    return table[_ORDERED_FILE_IDX_COLUMN_NAME].to_numpy()


def get_record_index_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _ORDERED_RECORD_IDX_COLUMN_TYPE,
    )


def record_index_column_np(table: pa.Table) -> np.ndarray:
    return table[_ORDERED_RECORD_IDX_COLUMN_NAME].to_numpy()


def is_source_column_np(table: pa.Table) -> np.ndarray:
    return table[_IS_SOURCE_COLUMN_NAME].to_numpy()


def get_delta_type_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _DELTA_TYPE_COLUMN_TYPE,
    )


def get_hash_bucket_idx_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(obj, _HASH_BUCKET_IDX_COLUMN_TYPE)


def get_is_source_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _IS_SOURCE_COLUMN_TYPE,
    )


def file_record_count_column_np(table: pa.Table) -> np.ndarray:
    return table[_FILE_RECORD_COUNT_COLUMN_NAME].to_numpy()


def get_file_record_count_column_array(obj) -> Union[pa.Array, pa.ChunkedArray]:
    return pa.array(
        obj,
        _FILE_RECORD_COUNT_COLUMN_TYPE,
    )


def project_delta_file_metadata_on_table(
    delta_file_envelope: DeltaFileEnvelope,
) -> pa.Table:

    table = delta_file_envelope.table

    # append ordered file number column
    ordered_file_number = delta_file_envelope.file_index
    ordered_file_number_iterator = repeat(
        int(ordered_file_number),
        len(table),
    )
    table = append_file_idx_column(table, ordered_file_number_iterator)

    # append event timestamp column
    stream_position = delta_file_envelope.stream_position
    stream_position_iterator = repeat(
        int(stream_position),
        len(table),
    )
    table = append_stream_position_column(table, stream_position_iterator)

    # append delta type column
    delta_type = delta_file_envelope.delta_type
    delta_type_iterator = repeat(
        delta_type_to_field(delta_type),
        len(table),
    )
    table = append_delta_type_col(table, delta_type_iterator)

    # append is source column
    is_source_iterator = repeat(
        True if delta_file_envelope.is_src_delta else False,
        len(table),
    )
    table = append_is_source_col(table, is_source_iterator)

    # append row count column
    file_record_count_iterator = repeat(
        delta_file_envelope.file_record_count, len(table)
    )
    table = append_file_record_count_col(table, file_record_count_iterator)
    return table


def append_stream_position_column(table: pa.Table, stream_positions):

    table = table.append_column(
        _PARTITION_STREAM_POSITION_COLUMN_FIELD,
        get_stream_position_column_array(stream_positions),
    )
    return table


def append_file_idx_column(table: pa.Table, ordered_file_indices):

    table = table.append_column(
        _ORDERED_FILE_IDX_COLUMN_FIELD,
        get_file_index_column_array(ordered_file_indices),
    )
    return table


def append_pk_hash_column(table: pa.Table, pk_hashes) -> pa.Table:

    table = table.append_column(
        _PK_HASH_COLUMN_FIELD, get_pk_hash_column_array(pk_hashes)
    )
    return table


def append_pk_hash_string_column(table: pa.Table, pk_hashes) -> pa.Table:

    table = table.append_column(
        _PK_HASH_STRING_COLUMN_FIELD, get_pk_hash_string_column_array(pk_hashes)
    )
    return table


def append_hash_bucket_idx_col(table: pa.Table, hash_bucket_indexes) -> pa.Table:

    table = table.append_column(
        _HASH_BUCKET_IDX_COLUMN_FIELD,
        get_hash_bucket_idx_column_array(hash_bucket_indexes),
    )

    return table


def append_record_idx_col(table: pa.Table, ordered_record_indices) -> pa.Table:

    table = table.append_column(
        _ORDERED_RECORD_IDX_COLUMN_FIELD,
        get_record_index_column_array(ordered_record_indices),
    )
    return table


def append_dedupe_task_idx_col(table: pa.Table, dedupe_task_indices) -> pa.Table:

    table = table.append_column(
        _DEDUPE_TASK_IDX_COLUMN_FIELD,
        get_dedupe_task_idx_column_array(dedupe_task_indices),
    )
    return table


def delta_type_to_field(delta_type: DeltaType) -> bool:
    return True if delta_type is DeltaType.UPSERT else False


def delta_type_from_field(delta_type_field: bool) -> DeltaType:
    return DeltaType.UPSERT if delta_type_field else DeltaType.DELETE


def append_delta_type_col(table: pa.Table, delta_types) -> pa.Table:

    table = table.append_column(
        _DELTA_TYPE_COLUMN_FIELD,
        get_delta_type_column_array(delta_types),
    )
    return table


def append_is_source_col(table: pa.Table, booleans) -> pa.Table:

    table = table.append_column(
        _IS_SOURCE_COLUMN_FIELD,
        get_is_source_column_array(booleans),
    )
    return table


def append_file_record_count_col(table: pa.Table, file_record_count):
    table = table.append_column(
        _FILE_RECORD_COUNT_COLUMN_FIELD,
        get_file_record_count_column_array(file_record_count),
    )
    return table


def get_minimal_hb_schema() -> pa.schema:
    return pa.schema(
        [
            _PK_HASH_COLUMN_FIELD,
            _ORDERED_RECORD_IDX_COLUMN_FIELD,
            _ORDERED_FILE_IDX_COLUMN_FIELD,
            _PARTITION_STREAM_POSITION_COLUMN_FIELD,
            _DELTA_TYPE_COLUMN_FIELD,
            _IS_SOURCE_COLUMN_FIELD,
        ]
    )
