# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
import pyarrow as pa

from enum import Enum

from deltacat.storage import PartitionLocator
from deltacat import logs

from typing import List

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

MAX_SORT_KEYS_BIT_WIDTH: int = 256


class SortOrder(str, Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class SortKey(tuple):
    @staticmethod
    def of(key_name: str, sort_order: SortOrder = SortOrder.ASCENDING) \
            -> SortKey:
        """
        Create a sort key from a field name to use as the sort key, and
        the sort order for this key. If no sort order is specified, then the
        data will be sorted in ascending order by default. Note that compaction
        always keeps the LAST occurrence of this key post-sort. For example, if
        you used an integer column as your sort key which contained the values
        [2, 1, 3] specifying SortOrder.ASCENDING would ensure that the
        value [3] is kept over [2, 1], and specifying SortOrder.DESCENDING
        would ensure that [1] is kept over [2, 3].
        """
        return SortKey((key_name, sort_order.value))

    @staticmethod
    def validate_sort_keys(
            source_partition_locator: PartitionLocator,
            sort_keys: List[SortKey],
            deltacat_storage) -> int:
        """
        Validates the input sort keys to ensure that they are unique, are using
        a valid sort key model, are all fixed-width data types, and that the
        sum of bit widths across sort key data types is less-than-or-equal-to
        256. Returns the sum of bit widths across all sort keys.
        """
        total_sort_keys_bit_width = 0
        if sort_keys:
            sort_key_names = [key.key_name for key in sort_keys]
            assert len(sort_key_names) == len(set(sort_key_names)), \
                f"Sort key names must be unique: {sort_key_names}"
            stream_locator = source_partition_locator.stream_locator
            table_version_schema = deltacat_storage.get_table_version_schema(
                stream_locator.namespace,
                stream_locator.table_name,
                stream_locator.table_version,
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
                                f"({total_sort_keys_bit_width}) is greater "
                                f"than the max supported bit width for all "
                                f"sort keys ({MAX_SORT_KEYS_BIT_WIDTH})")
                    except ValueError as e:
                        raise ValueError(
                            f"Unable to get bit width of sort key: {pa_field}. "
                            f"Please ensure that all sort keys are fixed-size "
                            f"PyArrow data types.") from e
            else:
                logger.warning(
                    f"Unable to estimate sort key bit width for schema type "
                    f"{type(table_version_schema)}. This compaction job run "
                    f"may run out of memory, run more slowly, or underutilize "
                    f"available resources. To fix this, either remove the "
                    f"sort keys or provide a PyArrow schema.")
                total_sort_keys_bit_width = MAX_SORT_KEYS_BIT_WIDTH
        return total_sort_keys_bit_width

    @property
    def key_name(self) -> str:
        return self[0]

    @property
    def sort_order(self) -> SortOrder:
        return SortOrder(self[1])
