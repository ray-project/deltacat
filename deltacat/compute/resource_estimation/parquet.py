import logging
from typing import Optional
from deltacat import logs
from pyarrow.parquet import ColumnChunkMetaData
from deltacat.constants import NULL_SIZE_BYTES

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _observed_string_size(min_value: str, max_value: str) -> float:
    """
    Pyarrow uses few additional bytes to store each string.
    """
    return (len(min_value) + len(max_value)) / 2 + 4


def _int96_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    return column_chunk_metadata.num_values * 12


def _int64_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    return column_chunk_metadata.num_values * 8


def _int32_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    return column_chunk_metadata.num_values * 4


def _boolean_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    return column_chunk_metadata.num_values


def _double_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    return column_chunk_metadata.num_values * 8


def _float_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    return column_chunk_metadata.num_values * 4


def _byte_array_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    uncompressed_size = column_chunk_metadata.total_uncompressed_size
    if column_chunk_metadata.is_stats_set:
        statistics = column_chunk_metadata.statistics
        if (
            statistics.has_min_max
            and isinstance(statistics.min, str)
            and isinstance(statistics.max, str)
        ):
            return max(
                uncompressed_size,
                (
                    statistics.num_values
                    * _observed_string_size(statistics.min, statistics.max)
                    + statistics.null_count * NULL_SIZE_BYTES
                ),
            )
        else:
            # A case of decimal
            return max(column_chunk_metadata.num_values * 16, uncompressed_size)
    else:
        return uncompressed_size


def _fixed_len_byte_array_size_estimator(
    column_chunk_metadata: ColumnChunkMetaData,
) -> float:
    return _byte_array_size_estimator(column_chunk_metadata)


_PHYSICAL_TYPE_TO_SIZE_ESTIMATOR = {
    "INT96": _int96_size_estimator,
    "INT64": _int64_size_estimator,
    "INT32": _int32_size_estimator,
    "BOOLEAN": _boolean_size_estimator,
    "DOUBLE": _double_size_estimator,
    "FLOAT": _float_size_estimator,
    "BYTE_ARRAY": _byte_array_size_estimator,
    "FIXED_LEN_BYTE_ARRAY": _fixed_len_byte_array_size_estimator,
}


def parquet_column_chunk_size_estimator(
    column_meta: ColumnChunkMetaData,
) -> Optional[float]:
    physical_type = column_meta.physical_type
    if physical_type in _PHYSICAL_TYPE_TO_SIZE_ESTIMATOR:
        return _PHYSICAL_TYPE_TO_SIZE_ESTIMATOR[physical_type](column_meta)
    else:
        logger.warning(
            f"Unsupported physical type: {physical_type}. "
            "Returning total_uncompressed_size."
        )
        return column_meta.total_uncompressed_size
