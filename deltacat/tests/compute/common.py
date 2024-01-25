# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from enum import Enum
from typing import List
import datetime as dt
from datetime import timezone

TEST_S3_RCF_BUCKET_NAME = "test-compaction-artifacts-bucket"
# REBASE  src = spark compacted table to create an initial version of ray compacted table
BASE_TEST_SOURCE_NAMESPACE = "source_test_namespace"
BASE_TEST_SOURCE_TABLE_NAME = "test_table"
BASE_TEST_SOURCE_TABLE_VERSION = "1"

BASE_TEST_DESTINATION_NAMESPACE = "destination_test_namespace"
BASE_TEST_DESTINATION_TABLE_NAME = "destination_test_table_RAY"
BASE_TEST_DESTINATION_TABLE_VERSION = "1"

HASH_BUCKET_COUNT: int = 1

MAX_RECORDS_PER_FILE: int = 1

UTC_ISO_8601_FORMAT_WITHOUT_MILLIS = "%Y-%m-%dT%H:%M:%SZ"  # '2018-09-05T14:09:03Z'


class PartitionKeyType(str, Enum):
    INT = "int"
    STRING = "string"
    TIMESTAMP = "timestamp"


def setup_sort_and_partition_keys(sort_keys_param, partition_keys_param):
    from deltacat.storage.model.sort_key import SortKey, SortScheme
    from deltacat.storage.model.partition import PartitionScheme, PartitionKey

    sort_keys, partition_keys = None, None
    if sort_keys_param is not None:
        sort_keys = SortScheme.of(
            keys=[SortKey.of(sort_key["key_name"]) for sort_key in sort_keys_param],
        )

    if partition_keys_param is not None:
        partition_keys = PartitionScheme.of(
            keys=[
                PartitionKey.of(
                    partition_key["key_name"],
                    PartitionKeyType(partition_key["key_type"]).value,
                )
                for partition_key in partition_keys_param
            ]
        )
    return sort_keys, partition_keys


def offer_iso8601_timestamp_list(
    periods: int,
    unit_of_time: str,
    end_time=dt.datetime(2023, 5, 3, 10, 0, 0, 0, tzinfo=timezone.utc),
) -> List[str]:
    """
    Returns a list of ISO 8601 timestamps, each periods units of time before the start time.

    Args:
    periods: The number of timestamps to return.
    unit_of_time: The unit of time to use for the timestamps. Must be one of "seconds", "minutes", "hours", "days", or "weeks".
    end_time: The end time for the timestamps. Defaults to 2023-05-03T10:00:00Z.

    Returns:
    A list of ISO 8601 timestamps, each periods units of time before the start time.

    Raises:
    ValueError: If the unit_of_time argument is not one of "seconds", "minutes", "hours", "days", or "weeks".
    """
    import datetime as dt

    acceptable_units_of_time = ["seconds", "minutes", "hours", "days", "weeks"]
    if unit_of_time not in acceptable_units_of_time:
        raise ValueError(
            f"unit_of_time {unit_of_time} is not supported. Please use one of these time units: {acceptable_units_of_time}"
        )
    res = []
    for i in range(periods):
        kwarg = {unit_of_time: i}
        res.append(
            (end_time - dt.timedelta(**kwarg)).strftime(
                UTC_ISO_8601_FORMAT_WITHOUT_MILLIS
            )
        )
    return res
