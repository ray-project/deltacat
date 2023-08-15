# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
import ray
from moto import mock_s3
import pytest
import os
import json
import boto3
from typing import Any, Dict
from typing import List
from boto3.resources.base import ServiceResource
import pyarrow as pa

from enum import Enum


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


MAX_RECORDS_PER_FILE: int = 1
TEST_S3_RCF_BUCKET_NAME = "test-compaction-artifacts-bucket"
# REBASE  src = spark compacted table to create an initial version of ray compacted table
TEST_SOURCE_NAMESPACE = "source_test_namespace"
TEST_SOURCE_TABLE_NAME = "test_table"
TEST_SOURCE_TABLE_VERSION = "1"

TEST_DESTINATION_NAMESPACE = "destination_test_namespace"
TEST_DESTINATION_TABLE_NAME = "destination_test_table"
TEST_DESTINATION_TABLE_VERSION = "1"

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)


"""
HELPERS
"""


def helper_read_s3_contents(
    s3_resource: ServiceResource, bucket_name: str, key: str
) -> Dict[str, Any]:
    response = s3_resource.Object(bucket_name, key).get()
    file_content: str = response["Body"].read().decode("utf-8")
    return json.loads(file_content)


"""
MODULE scoped fixtures
"""


# SETUP
@pytest.fixture(autouse=True, scope="module")
def mock_aws_credential():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_ID"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield


@pytest.fixture(scope="module")
def s3_resource(mock_aws_credential):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(scope="module")
def compaction_artifacts_s3_bucket(s3_resource: ServiceResource):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield


# TEARDOWN
@pytest.fixture(autouse=True, scope="module")
def remove_the_database_file_after_compaction_session_tests_complete():
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


"""
FUNCTION scoped fixtures
"""


# SETUP
@pytest.fixture(scope="function")
def ds_mock_kwargs():
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage


# TEARDOWN
@pytest.fixture(scope="function")
def cleanup_database_between_executions():
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


INCREMENTAL_TEST_CASES = {
    "1-incremental-pkstr-sknone-norcf": (
        {"pk_col_1"},  # Primary key columns
        [],  # Sort key columns
        [{"key_name": "region_id", "key_type": "int"}],  # Partition keys
        ["pk_col_1"],  # column_names
        [pa.array([str(i) for i in range(10)])],  # arrow arrays
        None,
        ["1"],
        pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])], names=["pk_col_1"]
        ),
        None,
        None,
        True,
        False,
    ),
    "2-incremental-pkstr-skstr-norcf": (
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            }
        ],
        [],
        ["pk_col_1", "sk_col_1"],
        [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
        None,
        ["1"],
        pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
            names=["pk_col_1", "sk_col_1"],
        ),
        None,
        None,
        True,
        False,
    ),
    "3-incremental-pkstr-multiskstr-norcf": (
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1", "sk_col_2"],
        [
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
            pa.array(["foo"] * 10),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array(["test"] * 10),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        None,
        None,
        True,
        False,
    ),
    "4-incremental-duplicate-pk": (
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1", "sk_col_2"],
        [
            pa.array([str(i) for i in range(5)] + ["6", "6", "6", "6", "6"]),
            pa.array([str(i) for i in range(10)]),
            pa.array(["foo"] * 10),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(5)] + ["6"]),
                pa.array([str(i) for i in range(5)] + ["9"]),
                pa.array(["foo"] * 6),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        None,
        None,
        True,
        False,
    ),
}


@pytest.mark.parametrize(
    [
        "test_name",
        "primary_keys",
        "in_sort_keys",
        "in_partition_keys",
        "column_names",
        "arrow_arrays",
        "rebase_source_partition_locator",
        "partition_values",
        "expected_result",
        "validation_callback_func",
        "validation_callback_func_kwargs",
        "cleanup_prev_table",
        "use_prev_compacted",
    ],
    [
        (
            test_name,
            primary_keys,
            in_sort_keys,
            in_partition_keys,
            column_names,
            arrow_arrays,
            rebase_source_partition_locator,
            partition_values,
            expected_result,
            validation_callback_func,
            validation_callback_func_kwargs,
            cleanup_prev_table,
            use_prev_compacted,
        )
        for test_name, (
            primary_keys,
            in_sort_keys,
            in_partition_keys,
            column_names,
            arrow_arrays,
            rebase_source_partition_locator,
            partition_values,
            expected_result,
            validation_callback_func,
            validation_callback_func_kwargs,
            cleanup_prev_table,
            use_prev_compacted,
        ) in INCREMENTAL_TEST_CASES.items()
    ],
    ids=[test_name for test_name in INCREMENTAL_TEST_CASES],
)
def test_compact_partition_incremental(
    request,
    s3_resource,
    ds_mock_kwargs,
    compaction_artifacts_s3_bucket,
    test_name,
    primary_keys,
    in_sort_keys,
    in_partition_keys,
    column_names: List[str],
    arrow_arrays: List[pa.Array],
    rebase_source_partition_locator,
    partition_values,
    expected_result,
    validation_callback_func,  # use and implement if you want to run additional validations apart from the ones in the test
    validation_callback_func_kwargs,
    cleanup_prev_table,
    use_prev_compacted,
):

    """
    TODO Test Cases:
    1. incremental w/wout round completion file
    2. Backfill w/wout round completion
    3. Rebase w/wout round completion file
    4. Rebase then incremental (use same round completion file)
    """
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import Partition, Stream
    from deltacat.compute.compactor.compaction_session import compact_partition
    from deltacat.storage.model.sort_key import SortKey
    from deltacat.storage import (
        PartitionLocator,
    )
    from deltacat.utils.placement import (
        PlacementGroupManager,
    )
    from deltacat.compute.compactor import (
        RoundCompletionInfo,
    )

    source_namespace = TEST_SOURCE_NAMESPACE
    source_table_name = TEST_SOURCE_TABLE_NAME
    source_table_version = TEST_SOURCE_TABLE_VERSION
    destination_namespace = TEST_DESTINATION_NAMESPACE
    destination_table_name = TEST_DESTINATION_TABLE_NAME
    destination_table_version = TEST_DESTINATION_TABLE_VERSION
    # setup
    sort_keys = None
    if in_sort_keys is not None:
        sort_keys = [SortKey.of(sort_key["key_name"]) for sort_key in in_sort_keys]
    partition_keys = None
    if in_partition_keys is not None:
        partition_keys = [
            PartitionKey.of(
                partition_key["key_name"], PartitionKeyType(partition_key["key_type"])
            )
            for partition_key in in_partition_keys
        ]
    # create the source table
    ds.create_namespace(source_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        source_namespace,
        source_table_name,
        source_table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    source_table_stream: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    test_table: pa.Table = pa.Table.from_arrays(arrow_arrays, names=column_names)
    staged_partition: Partition = ds.stage_partition(
        source_table_stream, partition_values, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(test_table, staged_partition, **ds_mock_kwargs), **ds_mock_kwargs
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    # create the destination table
    ds.create_namespace(destination_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        destination_namespace,
        destination_table_name,
        destination_table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    ray.shutdown()
    ray.init(local_mode=True)
    assert ray.is_initialized()
    source_table_version = ds.get_table_version(
        TEST_SOURCE_NAMESPACE,
        TEST_SOURCE_TABLE_NAME,
        TEST_SOURCE_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    source_partition = ds.get_partition(
        ds.get_stream(
            namespace=TEST_SOURCE_NAMESPACE,
            table_name=TEST_SOURCE_TABLE_NAME,
            table_version=TEST_SOURCE_TABLE_VERSION,
            **ds_mock_kwargs,
        ).locator,
        partition_values,
        **ds_mock_kwargs,
    )
    destination_partition_locator = PartitionLocator.of(
        destination_table_stream.locator,
        partition_values,
        None,
    )
    num_workers, worker_instance_cpu = 1, 1
    total_cpus = num_workers * worker_instance_cpu
    pgm = PlacementGroupManager(1, total_cpus, worker_instance_cpu).pgs[0]
    compact_partition_params: Dict[str, Any] = {
        "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
        "compacted_file_content_type": ContentType.PARQUET,
        "dd_max_parallelism_ratio": 1.0,
        "deltacat_storage": ds,
        "deltacat_storage_kwargs": ds_mock_kwargs,
        "destination_partition_locator": destination_partition_locator,
        "hash_bucket_count": None,
        "last_stream_position_to_compact": source_partition.stream_position,
        "list_deltas_kwargs": {**ds_mock_kwargs, **{"equivalent_table_types": []}},
        "pg_config": pgm,
        "primary_keys": primary_keys,
        "rebase_source_partition_locator": None,
        "records_per_compacted_file": MAX_RECORDS_PER_FILE,
        "source_partition_locator": source_partition.locator,
        "sort_keys": sort_keys if sort_keys else None,
    }
    # execute
    rcf_file_s3_uri = compact_partition(**compact_partition_params)
    _, rcf_object_key = rcf_file_s3_uri.rsplit("/", 1)
    rcf_file_output: Dict[str, Any] = helper_read_s3_contents(
        s3_resource, TEST_S3_RCF_BUCKET_NAME, rcf_object_key
    )
    round_completion_info = RoundCompletionInfo(**rcf_file_output)
    print(f"rcf_file_output: {json.dumps(rcf_file_output, indent=2)}")
    compacted_delta_locator = round_completion_info.compacted_delta_locator
    tables = ds.download_delta(compacted_delta_locator, **ds_mock_kwargs)
    compacted_table = pa.concat_tables(tables)
    assert compacted_table.equals(expected_result)
    if (
        validation_callback_func is not None
        and validation_callback_func_kwargs is not None
    ):
        validation_callback_func(**validation_callback_func_kwargs)
    if not cleanup_prev_table:
        pass
    else:
        request.getfixturevalue("cleanup_database_between_executions")
    ray.shutdown()
    assert not ray.is_initialized()
