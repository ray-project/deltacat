# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import datetime as dt
from boto3.resources.base import ServiceResource
from datetime import timezone

from deltacat.tests.compute.test_util_constant import (
    TEST_S3_RCF_BUCKET_NAME,
)
from deltacat.tests.compute.test_util_constant import (
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
    BASE_TEST_DESTINATION_TABLE_VERSION,
    REBASING_NAMESPACE,
    REBASING_TABLE_NAME,
    REBASING_TABLE_VERSION,
)
from deltacat.compute.compactor import (
    RoundCompletionInfo,
)
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)

from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.model.stream import StreamLocator
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.table import TableLocator
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.compute.compactor.model.compactor_version import CompactorVersion


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


def get_test_partition_locator(partition_id):
    tv_locator = TableVersionLocator.of(
        TableLocator.of(NamespaceLocator.of("default"), "test_table"), "1"
    )
    stream_locator = StreamLocator.of(tv_locator, "test_stream_id", "local")
    partition_locator = PartitionLocator.of(
        stream_locator, partition_id=partition_id, partition_values=[]
    )

    return partition_locator


def _create_table(
    namespace: str,
    table_name: str,
    table_version: str,
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType

    ds.create_namespace(namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        namespace,
        table_name,
        table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    return namespace, table_name, table_version


def create_src_table(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    source_namespace: str = BASE_TEST_SOURCE_NAMESPACE
    source_table_name: str = BASE_TEST_SOURCE_TABLE_NAME
    source_table_version: str = BASE_TEST_SOURCE_TABLE_VERSION
    return _create_table(
        source_namespace,
        source_table_name,
        source_table_version,
        primary_keys,
        sort_keys,
        partition_keys,
        ds_mock_kwargs,
    )


def create_destination_table(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    destination_namespace: str = BASE_TEST_DESTINATION_NAMESPACE
    destination_table_name: str = BASE_TEST_DESTINATION_TABLE_NAME
    destination_table_version: str = BASE_TEST_DESTINATION_TABLE_VERSION
    return _create_table(
        destination_namespace,
        destination_table_name,
        destination_table_version,
        primary_keys,
        sort_keys,
        partition_keys,
        ds_mock_kwargs,
    )


def create_rebase_table(
    primary_keys: Set[str],
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    rebasing_namespace = REBASING_NAMESPACE
    rebasing_table_name = REBASING_TABLE_NAME
    rebasing_table_version = REBASING_TABLE_VERSION
    return _create_table(
        rebasing_namespace,
        rebasing_table_name,
        rebasing_table_version,
        primary_keys,
        sort_keys,
        partition_keys,
        ds_mock_kwargs,
    )


def get_rcf(s3_resource, rcf_file_s3_uri: str) -> RoundCompletionInfo:
    from deltacat.tests.test_utils.utils import read_s3_contents

    _, rcf_object_key = rcf_file_s3_uri.strip("s3://").split("/", 1)
    rcf_file_output: Dict[str, Any] = read_s3_contents(
        s3_resource, TEST_S3_RCF_BUCKET_NAME, rcf_object_key
    )
    return RoundCompletionInfo(**rcf_file_output)


def get_compacted_delta_locator_from_rcf(
    s3_resource: ServiceResource, rcf_file_s3_uri: str
):
    from deltacat.storage import DeltaLocator

    round_completion_info: RoundCompletionInfo = get_rcf(s3_resource, rcf_file_s3_uri)

    compacted_delta_locator: DeltaLocator = (
        round_completion_info.compacted_delta_locator
    )
    return compacted_delta_locator


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


def assert_compaction_audit(
    compactor_version: CompactorVersion, compaction_audit: CompactionSessionAuditInfo
):
    if compactor_version == CompactorVersion.V2:
        audit_entries = [
            compaction_audit.deltacat_version,
            compaction_audit.ray_version,
            compaction_audit.audit_url,
            compaction_audit.hash_bucket_count,
            compaction_audit.compactor_version,
            compaction_audit.input_size_bytes,
            compaction_audit.input_file_count,
            compaction_audit.estimated_in_memory_size_bytes_during_discovery,
            compaction_audit.uniform_deltas_created,
            compaction_audit.delta_discovery_time_in_seconds,
            compaction_audit.hash_bucket_time_in_seconds,
            compaction_audit.hash_bucket_invoke_time_in_seconds,
            compaction_audit.hash_bucket_result_size_bytes,
            compaction_audit.total_cluster_object_store_memory_bytes,
            compaction_audit.total_cluster_memory_bytes,
            compaction_audit.total_object_store_memory_used_bytes,
            compaction_audit.hash_bucket_post_object_store_memory_used_bytes,
            compaction_audit.hash_bucket_result_wait_time_in_seconds,
            compaction_audit.peak_memory_used_bytes_per_hash_bucket_task,
            compaction_audit.hash_bucket_processed_size_bytes,
            compaction_audit.input_records,
            compaction_audit.merge_time_in_seconds,
            compaction_audit.merge_invoke_time_in_seconds,
            compaction_audit.merge_result_size,
            compaction_audit.merge_post_object_store_memory_used_bytes,
            compaction_audit.merge_result_wait_time_in_seconds,
            compaction_audit.peak_memory_used_bytes_per_merge_task,
            compaction_audit.records_deduped,
            compaction_audit.records_deleted,
            compaction_audit.compaction_time_in_seconds,
            compaction_audit.peak_memory_used_bytes_by_compaction_session_process,
            compaction_audit.untouched_file_count,
            compaction_audit.untouched_file_ratio,
            compaction_audit.untouched_record_count,
            compaction_audit.untouched_size_bytes,
            compaction_audit.output_file_count,
            compaction_audit.output_size_bytes,
            compaction_audit.output_size_pyarrow_bytes,
            compaction_audit.peak_memory_used_bytes_per_task,
            compaction_audit.pyarrow_version,
            compaction_audit.telemetry_time_in_seconds,
            compaction_audit.observed_input_inflation,
            compaction_audit.observed_input_average_record_size_bytes,
        ]
        for entry in audit_entries:
            assert entry is not None
    return True


def assert_compaction_audit_no_hash_bucket(
    compactor_version: CompactorVersion, compaction_audit: CompactionSessionAuditInfo
):
    if compactor_version == CompactorVersion.V2:
        audit_entries = [
            compaction_audit.deltacat_version,
            compaction_audit.ray_version,
            compaction_audit.audit_url,
            compaction_audit.hash_bucket_count,
            compaction_audit.compactor_version,
            compaction_audit.input_size_bytes,
            compaction_audit.input_file_count,
            compaction_audit.estimated_in_memory_size_bytes_during_discovery,
            compaction_audit.uniform_deltas_created,
            compaction_audit.delta_discovery_time_in_seconds,
            compaction_audit.total_cluster_object_store_memory_bytes,
            compaction_audit.total_cluster_memory_bytes,
            compaction_audit.total_object_store_memory_used_bytes,
            compaction_audit.input_records,
            compaction_audit.merge_time_in_seconds,
            compaction_audit.merge_invoke_time_in_seconds,
            compaction_audit.merge_result_size,
            compaction_audit.merge_post_object_store_memory_used_bytes,
            compaction_audit.merge_result_wait_time_in_seconds,
            compaction_audit.peak_memory_used_bytes_per_merge_task,
            compaction_audit.records_deduped,
            compaction_audit.records_deleted,
            compaction_audit.compaction_time_in_seconds,
            compaction_audit.peak_memory_used_bytes_by_compaction_session_process,
            compaction_audit.untouched_file_count,
            compaction_audit.untouched_file_ratio,
            compaction_audit.untouched_record_count,
            compaction_audit.untouched_size_bytes,
            compaction_audit.output_file_count,
            compaction_audit.output_size_bytes,
            compaction_audit.output_size_pyarrow_bytes,
            compaction_audit.peak_memory_used_bytes_per_task,
            compaction_audit.pyarrow_version,
            compaction_audit.telemetry_time_in_seconds,
        ]
        for entry in audit_entries:
            assert entry is not None
    return True
