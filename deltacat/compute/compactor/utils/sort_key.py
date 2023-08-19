import pyarrow as pa
from typing import List
from deltacat.storage import PartitionLocator, SortKey

MAX_SORT_KEYS_BIT_WIDTH = 256


def validate_sort_keys(
    source_partition_locator: PartitionLocator,
    sort_keys: List[SortKey],
    deltacat_storage,
    deltacat_storage_kwargs,
    **kwargs,
) -> int:
    """
    Validates the input sort keys to ensure that they are unique, are using
    a valid sort key model, are all fixed-width data types, and that the
    sum of bit widths across sort key data types is less-than-or-equal-to
    256. Returns the sum of bit widths across all sort keys.
    """
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}
    total_sort_keys_bit_width = 0
    if sort_keys:
        sort_key_names = [key.key_name for key in sort_keys]
        assert len(sort_key_names) == len(
            set(sort_key_names)
        ), f"Sort key names must be unique: {sort_key_names}"
        stream_locator = source_partition_locator.stream_locator
        table_version_schema = deltacat_storage.get_table_version_schema(
            stream_locator.namespace,
            stream_locator.table_name,
            stream_locator.table_version,
            **deltacat_storage_kwargs,
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
                            f"sort keys ({MAX_SORT_KEYS_BIT_WIDTH})"
                        )
                except ValueError as e:
                    raise ValueError(
                        f"Unable to get bit width of sort key: {pa_field}. "
                        f"Please ensure that all sort keys are fixed-size "
                        f"PyArrow data types."
                    ) from e
        else:
            total_sort_keys_bit_width = MAX_SORT_KEYS_BIT_WIDTH
    return total_sort_keys_bit_width
