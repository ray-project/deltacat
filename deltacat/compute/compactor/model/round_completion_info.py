from typing import Any, Dict


def of(
        hash_bucket_count: int,
        high_watermark: int,
        compacted_delta_locator: Dict[str, Any],
        compacted_file_count: int,
        compacted_file_bytes: int,
        compacted_pyarrow_bytes: int,
        compacted_records: int,
        pk_index_file_count: int,
        pk_index_file_bytes: int,
        pk_index_pyarrow_bytes: int,
        pk_index_records: int,
        primary_key_index_version: int) -> Dict[str, Any]:

    return {
        "hashBuckets": hash_bucket_count,
        "highWatermark": high_watermark,
        "compactedDeltaLocator": compacted_delta_locator,
        "compactedFiles": compacted_file_count,
        "compactedFileBytes": compacted_file_bytes,
        "compactedPyarrowBytes": compacted_pyarrow_bytes,
        "compactedRecords": compacted_records,
        "pkIndexFiles": pk_index_file_count,
        "pkIndexFileBytes": pk_index_file_bytes,
        "pkIndexPyarrowBytes": pk_index_pyarrow_bytes,
        "pkIndexRecords": pk_index_records,
        "primaryKeyIndexVersion": primary_key_index_version,
    }


def get_hash_bucket_count(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["hashBuckets"]


def get_high_watermark(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["highWatermark"]


def get_compacted_delta_locator(round_completion_info: Dict[str, Any]) \
        -> Dict[str, Any]:

    return round_completion_info["compactedDeltaLocator"]


def get_compacted_files(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["compactedFiles"]


def get_compacted_file_bytes(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["compactedFileBytes"]


def get_compacted_pyarrow_bytes(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["compactedPyarrowBytes"]


def get_compacted_records(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["compactedRecords"]


def get_pk_index_files(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["pkIndexFiles"]


def get_pk_index_file_bytes(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["pkIndexFileBytes"]


def get_pk_index_pyarrow_bytes(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["pkIndexPyarrowBytes"]


def get_pk_index_records(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["pkIndexRecords"]


def get_primary_key_index_version(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["primaryKeyIndexVersion"]
