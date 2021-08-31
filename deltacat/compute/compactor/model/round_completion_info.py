from typing import Any, Dict


def of(
        high_watermark: int,
        compacted_delta_locator: Dict[str, Any],
        compacted_pyarrow_write_result: Dict[str, Any],
        pk_index_pyarrow_write_result: Dict[str, Any],
        sort_keys_bit_width: int,
        primary_key_index_version_locator: Dict[str, Any]) -> Dict[str, Any]:

    return {
        "highWatermark": high_watermark,
        "compactedDeltaLocator": compacted_delta_locator,
        "compactedPyarrowWriteResult": compacted_pyarrow_write_result,
        "pkIndexPyarrowWriteResult": pk_index_pyarrow_write_result,
        "sortKeysBitWidth": sort_keys_bit_width,
        "primaryKeyIndexVersionLocator": primary_key_index_version_locator,
    }


def get_high_watermark(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["highWatermark"]


def get_compacted_delta_locator(round_completion_info: Dict[str, Any]) \
        -> Dict[str, Any]:

    return round_completion_info["compactedDeltaLocator"]


def get_compacted_pyarrow_write_result(round_completion_info: Dict[str, Any]) \
        -> Dict[str, Any]:

    return round_completion_info["compactedPyarrowWriteResult"]


def get_pk_index_pyarrow_write_result(round_completion_info: Dict[str, Any]) \
        -> Dict[str, Any]:

    return round_completion_info["pkIndexPyarrowWriteResult"]


def get_sort_keys_bit_width(round_completion_info: Dict[str, Any]) -> int:
    return round_completion_info["sortKeysBitWidth"]


def get_primary_key_index_version_locator(
        round_completion_info: Dict[str, Any]) -> Dict[str, Any]:
    return round_completion_info["primaryKeyIndexVersionLocator"]
