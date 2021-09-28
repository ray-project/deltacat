import logging
import pyarrow as pa

from enum import Enum

from deltacat import logs
from deltacat.storage.model import partition_locator as pl, \
    stream_locator as sl
from deltacat.compute.compactor.model import sort_key as sk
from deltacat.storage import interface as unimplemented_deltacat_storage

from ray.data.impl.arrow_block import SortKeyT
from typing import Any, Dict, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

MAX_SORT_KEYS_BIT_WIDTH: int = 256


class SortOrder(str, Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


def of(key_name: str, sort_order: SortOrder=SortOrder.ASCENDING) \
        -> Tuple[str, str]:
    """
    Create a sort key from a field name to use as the sort key, and
    the sort order for this key. If no sort order is specified, then the
    data will be sorted in ascending order by default. Note that compaction
    always keeps the LAST occurrence of this key post-sort. For example, if you
    used an integer column as your sort key which contained the values
    [2, 1, 3] specifying SortOrder.ASCENDING would ensure that the
    value [3] is kept over [2, 1], and specifying SortOrder.DESCENDING would
    ensure that [1] is kept over [2, 3].
    """
    return key_name, sort_order.value


def get_key_name(sort_key: Tuple[str, str]) -> str:
    return sort_key[0]


def get_sort_order(sort_key: Tuple[str, str]) -> str:
    return sort_key[1]


def is_valid_sort_order(sort_order: str) -> bool:
    try:
        SortOrder(sort_order)
        return True
    except ValueError:
        return False


def validate_sort_keys(
        source_partition_locator: Dict[str, Any],
        sort_keys: SortKeyT,
        deltacat_storage=unimplemented_deltacat_storage) -> int:
    """
    Validates the input sort keys to ensure that they are unique, are using
    a valid sort key model, are all fixed-width data types, and that the sum of
    bit widths across sort key data types is less-than-or-equal-to 256. Returns
    the sum of bit widths across all sort keys.
    """
    total_sort_keys_bit_width = 0
    if sort_keys:
        sort_key_names = [sk.get_key_name(key) for key in sort_keys]
        assert len(sort_key_names) == len(set(sort_key_names)), \
            f"Sort key names must be unique: {sort_key_names}"
        for sort_key in sort_keys:
            assert(sk.is_valid_sort_order(sk.get_sort_order(sort_key)),
                   f"Sort order for key '{sort_key}' must be one of: "
                   f"{[member.value for member in sk.SortOrder]}")
        stream_locator = pl.get_stream_locator(source_partition_locator)
        table_version_schema = deltacat_storage.get_table_version_schema(
            sl.get_namespace(stream_locator),
            sl.get_table_name(stream_locator),
            sl.get_table_version(stream_locator),
        )
        if isinstance(table_version_schema, pa.Schema):
            for sort_key_name in sort_key_names:
                pa_field: pa.Field = pa.Schema.field(sort_key_name)
                pa_type: pa.DataType = pa_field.type
                try:
                    total_sort_keys_bit_width += pa_type.bit_width
                    if total_sort_keys_bit_width > MAX_SORT_KEYS_BIT_WIDTH:
                        raise ValueError(
                            f"Total length of sort keys "
                            f"({total_sort_keys_bit_width}) is greater than "
                            f"the max supported bit width for all sort keys "
                            f"{MAX_SORT_KEYS_BIT_WIDTH}")
                except ValueError as e:
                    raise ValueError(
                        f"Unable to get bit width of sort key: {pa_field}. "
                        f"Please ensure that all sort keys are fixed-size "
                        f"PyArrow data types.") from e
        else:
            raise NotImplementedError(
               f"Schema type {type(table_version_schema)} does not support "
               f"compaction with custom sort keys. Either remove the sort "
               f"keys, or provide a PyArrow schema.")
    return total_sort_keys_bit_width
