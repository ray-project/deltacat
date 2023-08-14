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

MAX_RECORDS_PER_FILE: int = 1
TEST_S3_RCF_BUCKET_NAME = "test-compaction-artifacts-bucket"
# REBASE  src = spark compacted table to create an initial version of ray compacted table
TEST_SOURCE_NAMESPACE = "source_test_namespace"
TEST_SOURCE_TABLE_NAME = "test_table"
TEST_SOURCE_TABLE_VERSION = "1"
TEST_SOURCE_PARTITION_KEYS = [{"keyName": "region_id", "keyType": "int"}]

TEST_DESTINATION_NAMESPACE = "destination_test_namespace"
TEST_DESTINATION_TABLE_NAME = "destination_test_table"
TEST_DESTINATION_TABLE_VERSION = "1"
TEST_DESTINATION_PARTITION_KEYS = [{"keyName": "region_id", "keyType": "int"}]


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


"""
Function scoped fixtures
"""


@pytest.fixture(scope="function")
def ds_mock_kwargs():
    DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
        "db_file_path",
        "deltacat/tests/local_deltacat_storage/db_test.sqlite",
    )
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage
    # cleanup the created db file after
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


@pytest.fixture(scope="function")
def compaction_artifacts_s3_bucket(s3_resource: ServiceResource):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield

    """

    Cases:
    1. incremental + no round completion file (sanity) (destination empty)
    2. incremental + round completion file
    3. Backfill
        w round completion and no round completion
    4. rebase
        w round completion and no round completion
    5. Rebase then incremental (use same round completion file)

    # source_partition.stream_position
    # query rcf
    # different PKs/timestamp/int
    # region_
    # got compacted table

    # primary key as different types
    # partition keys as different types - multip partition keys
    """


INCREMENTAL_TEST_CASES = {
    "1-incremental-pkstr-sknone-norcf": (
        {"pk_col_1"},  # Primary key columns
        None,  # Sort key columns
        "",  # RCF
        ["pk_col_1"],  # column_names
        [pa.array([str(i) for i in range(10)])],  # arrow arrays
        TEST_SOURCE_NAMESPACE,
        TEST_SOURCE_TABLE_NAME,
        TEST_SOURCE_TABLE_VERSION,
        TEST_DESTINATION_NAMESPACE,
        TEST_DESTINATION_TABLE_NAME,
        TEST_DESTINATION_TABLE_VERSION,
    ),
    "2-incremental-pkstr-skstr-norcf": (
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            }
        ],
        "",
        ["pk_col_1", "sk_col_1"],
        [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
        TEST_SOURCE_NAMESPACE,
        TEST_SOURCE_TABLE_NAME,
        TEST_SOURCE_TABLE_VERSION,
        TEST_DESTINATION_NAMESPACE,
        TEST_DESTINATION_TABLE_NAME,
        TEST_DESTINATION_TABLE_VERSION,
    ),
    "3-incremental-pkstr-skstr-norcf": (
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
        ],
        "",
        ["pk_col_1", "sk_col_1", "sk_col_2"],
        [
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
            pa.array(["foo"] * 10),
        ],
        TEST_SOURCE_NAMESPACE,
        TEST_SOURCE_TABLE_NAME,
        TEST_SOURCE_TABLE_VERSION,
        TEST_DESTINATION_NAMESPACE,
        TEST_DESTINATION_TABLE_NAME,
        TEST_DESTINATION_TABLE_VERSION,
    ),
}


@pytest.mark.parametrize(
    [
        "test_name",
        "primary_keys",
        "serialized_sort_keys",
        "rcf",
        "column_names",
        "arrow_arrays",
        "source_namespace",
        "source_table_name",
        "source_table_version",
        "destination_namespace",
        "destination_table_name",
        "destination_table_version",
    ],
    [
        (
            test_name,
            primary_keys,
            serialized_sort_keys,
            rcf,
            column_names,
            arrow_arrays,
            source_namespace,
            source_table_name,
            source_table_version,
            destination_namespace,
            destination_table_name,
            destination_table_version,
        )
        for test_name, (
            primary_keys,
            serialized_sort_keys,
            rcf,
            column_names,
            arrow_arrays,
            source_namespace,
            source_table_name,
            source_table_version,
            destination_namespace,
            destination_table_name,
            destination_table_version,
        ) in INCREMENTAL_TEST_CASES.items()
    ],
    ids=[test_name for test_name in INCREMENTAL_TEST_CASES],
)
def test_compact_partition_incremental(
    s3_resource,
    ds_mock_kwargs,
    compaction_artifacts_s3_bucket,
    test_name,
    primary_keys,
    serialized_sort_keys,
    rcf,
    column_names: List[str],
    arrow_arrays: List[pa.Array],
    source_namespace,
    source_table_name,
    source_table_version,
    destination_namespace,
    destination_table_name,
    destination_table_version,
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import Partition, Stream
    from deltacat.compute.compactor.compaction_session import compact_partition
    from deltacat.storage.model.sort_key import SortKey
    from deltacat.storage import PartitionLocator
    from deltacat.utils.placement import (
        PlacementGroupManager,
    )

    # setup
    sort_keys = None
    if serialized_sort_keys is not None:
        sort_keys = [
            SortKey.of(sort_key["key_name"]) for sort_key in serialized_sort_keys
        ]
    # create the source table
    ds.create_namespace(source_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        source_namespace,
        source_table_name,
        source_table_version,
        primary_key_column_names=list(primary_keys),
        sort_keys=sort_keys,
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
        source_table_stream, [], **ds_mock_kwargs
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
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
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
        [],
        **ds_mock_kwargs,
    )
    destination_partition_locator = PartitionLocator.of(
        destination_table_stream.locator,
        [],
        None,
    )
    num_workers, worker_instance_cpu = 1, 1
    total_cpus = num_workers * worker_instance_cpu
    pgm = PlacementGroupManager(1, total_cpus, worker_instance_cpu).pgs[0]
    compact_partition_params: Dict[str, Any] = {
        "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
        "compacted_file_content_type": ContentType.PARQUET,
        "deltacat_storage": ds,
        "destination_partition_locator": destination_partition_locator,
        "hash_bucket_count": None,
        "last_stream_position_to_compact": source_partition.stream_position,
        "list_deltas_kwargs": {**ds_mock_kwargs, **{"equivalent_table_types": []}},
        "pg_config": pgm,
        "primary_keys": primary_keys,
        "records_per_compacted_file": MAX_RECORDS_PER_FILE,
        "source_partition_locator": source_partition.locator,
        "sort_keys": sort_keys if sort_keys else None,
        **ds_mock_kwargs,
    }
    # execute
    rcf_file_s3_uri = compact_partition(**compact_partition_params)
    _, rcf_object_key = rcf_file_s3_uri.rsplit("/", 1)
    rcf_file_output = helper_read_s3_contents(
        s3_resource, TEST_S3_RCF_BUCKET_NAME, rcf_object_key
    )
    # verify
    assert (
        rcf_file_output["compactedDeltaLocator"]["streamPosition"]
        == source_partition.stream_position
    )
    ray.shutdown()
    assert not ray.is_initialized()
