# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from enum import Enum

TEST_S3_RCF_BUCKET_NAME = "test-compaction-artifacts-bucket"
# REBASE  src = spark compacted table to create an initial version of ray compacted table
BASE_TEST_SOURCE_NAMESPACE = "source_test_namespace"
BASE_TEST_SOURCE_TABLE_NAME = "test_table"
BASE_TEST_SOURCE_TABLE_VERSION = "1"

BASE_TEST_DESTINATION_NAMESPACE = "destination_test_namespace"
BASE_TEST_DESTINATION_TABLE_NAME = "destination_test_table_RAY"
BASE_TEST_DESTINATION_TABLE_VERSION = "1"

MAX_RECORDS_PER_FILE: int = 1


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
