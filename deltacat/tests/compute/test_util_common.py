# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional
import pyarrow as pa
import datetime as dt
import json
from boto3.resources.base import ServiceResource
from datetime import timezone

from deltacat.tests.compute.test_util_constant import (
    TEST_S3_RCF_BUCKET_NAME,
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
    BASE_TEST_DESTINATION_TABLE_VERSION,
)


class PartitionKeyType(str, Enum):
    INT = "int"
    STRING = "string"
    TIMESTAMP = "timestamp"


class PartitionKey(dict):
    @staticmethod
    def of(key_name: str, key_type: PartitionKeyType) -> PartitionKey:
        return PartitionKey({"keyName": key_name, "keyType": key_type.value})

    @property
    def key_name(self) -> str:
        return self["keyName"]

    @property
    def key_type(self) -> PartitionKeyType:
        key_type = self["keyType"]
        return None if key_type is None else PartitionKeyType(key_type)


"""
UTILS
"""


def get_compacted_delta_locator_from_rcf(
    s3_resource: ServiceResource, rcf_file_s3_uri: str
):
    from deltacat.tests.test_utils.utils import read_s3_contents
    from deltacat.compute.compactor import (
        RoundCompletionInfo,
    )

    _, rcf_object_key = rcf_file_s3_uri.rsplit("/", 1)
    rcf_file_output: Dict[str, Any] = read_s3_contents(
        s3_resource, TEST_S3_RCF_BUCKET_NAME, rcf_object_key
    )
    round_completion_info = RoundCompletionInfo(**rcf_file_output)
    print(f"rcf_file_output: {json.dumps(rcf_file_output, indent=2)}")
    compacted_delta_locator = round_completion_info.compacted_delta_locator
    return compacted_delta_locator


def setup_partition_keys(partition_keys_param) -> Optional[PartitionKey]:
    partition_keys = None
    if partition_keys_param is not None:
        partition_keys = [
            PartitionKey.of(
                partition_key["key_name"], PartitionKeyType(partition_key["key_type"])
            )
            for partition_key in partition_keys_param
        ]
    return partition_keys


def setup_sort_and_partition_keys(sort_keys_param, partition_keys_param):
    from deltacat.storage.model.sort_key import SortKey

    sort_keys, partition_keys = None, None
    if sort_keys_param is not None:
        sort_keys = [SortKey.of(sort_key["key_name"]) for sort_key in sort_keys_param]
    if partition_keys_param is not None:
        partition_keys = [
            PartitionKey.of(
                partition_key["key_name"], PartitionKeyType(partition_key["key_type"])
            )
            for partition_key in partition_keys_param
        ]
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

    UTC_ISO_8601_FORMAT_WITHOUT_MILLIS = "%Y-%m-%dT%H:%M:%SZ"  # '2018-09-05T14:09:03Z'

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
