import ray
import os
from moto import mock_s3
import pytest
import boto3
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.tests.compute.common import (
    setup_general_source_and_destination_tables,
    setup_sort_and_partition_keys,
    offer_iso8601_timestamp_list,
    PartitionKey,
    TEST_S3_RCF_BUCKET_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    BASE_TEST_DESTINATION_TABLE_VERSION,
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
    COMPACTED_VIEW_NAMESPACE,
    RAY_COMPACTED_VIEW_NAMESPACE,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
    MAX_RECORDS_PER_FILE,
)
from deltacat.tests.compute.compact_partition_test_cases import (
    INCREMENTAL_TEST_CASES,
)
from typing import Any, Dict

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)

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


@pytest.fixture(autouse=True, scope="module")
def cleanup_the_database_file_after_all_compaction_session_package_tests_complete():
    # make sure the database file is deleted after all the compactor package tests are completed
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


@pytest.fixture(scope="module")
def setup_s3_resource(mock_aws_credential):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(scope="module")
def setup_compaction_artifacts_s3_bucket(setup_s3_resource: ServiceResource):
    setup_s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield


"""
FUNCTION scoped fixtures
"""


@pytest.fixture(scope="function")
def setup_local_deltacat_storage_conn(request: pytest.FixtureRequest):
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage


def test_compact_partition_rebase_then_incremental(
    request: pytest.FixtureRequest,
    setup_s3_resource: ServiceResource,
    setup_local_deltacat_storage_conn: Dict[str, Any],
    setup_compaction_artifacts_s3_bucket: None,
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import (
        PartitionLocator,
    )
    from deltacat.compute.compactor.model.compact_partition_params import (
        CompactPartitionParams,
    )
    from deltacat.utils.placement import (
        PlacementGroupManager,
    )
    from deltacat.compute.compactor import (
        RoundCompletionInfo,
    )

    ds_mock_kwargs = setup_local_deltacat_storage_conn
    ray.shutdown()
    ray.init(local_mode=True)
    assert ray.is_initialized()
    # phase 1
    source_table_version = BASE_TEST_SOURCE_TABLE_VERSION
    destination_table_version = BASE_TEST_DESTINATION_TABLE_VERSION
    primary_keys_param = {"pk_col_1"}
    sort_keys_param = []
    partition_keys_param = [{"key_name": "region_id", "key_type": "int"}]
    column_names_param = ["pk_col_1"]
    arrow_arrays_param = [pa.array([str(i) for i in range(10)])]
    partition_values_param = ["1"]
    sort_keys, partition_keys = setup_sort_and_partition_keys(
        sort_keys_param, partition_keys_param
    )
    (
        source_table_stream,
        destination_table_stream,
    ) = setup_general_source_and_destination_tables(
        primary_keys_param,
        sort_keys,
        partition_keys,
        column_names_param,
        arrow_arrays_param,
        partition_values_param,
        ds_mock_kwargs,
        source_namespace=BASE_TEST_SOURCE_NAMESPACE,
        source_table_name=BASE_TEST_SOURCE_TABLE_NAME,
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_namespace=COMPACTED_VIEW_NAMESPACE,
        destination_table_name=BASE_TEST_SOURCE_TABLE_NAME + "_compacted",
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
    )
    source_partition = ds.get_partition(
        source_table_stream.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
    destination_partition_locator = PartitionLocator.of(
        destination_table_stream.locator,
        partition_values_param,
        None,
    )
    num_workers, worker_instance_cpu = DEFAULT_NUM_WORKERS, DEFAULT_WORKER_INSTANCE_CPUS
    total_cpus = num_workers * worker_instance_cpu
    pgm = None
    create_placement_group_param = False
    if create_placement_group_param:
        pgm = PlacementGroupManager(
            1, total_cpus, worker_instance_cpu, memory_per_bundle=4000000
        ).pgs[0]
    hash_bucket_count_param = None
    records_per_compacted_file_param = MAX_RECORDS_PER_FILE
    rebase_source_partition_locator_param = None
    compact_partition_params = CompactPartitionParams.of(
        {
            "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
            "compacted_file_content_type": ContentType.PARQUET,
            "dd_max_parallelism_ratio": 1.0,
            "deltacat_storage": ds,
            "deltacat_storage_kwargs": ds_mock_kwargs,
            "destination_partition_locator": destination_partition_locator,
            "hash_bucket_count": hash_bucket_count_param,
            "last_stream_position_to_compact": source_partition.stream_position,
            "list_deltas_kwargs": {**ds_mock_kwargs, **{"equivalent_table_types": []}},
            "pg_config": pgm,
            "primary_keys": primary_keys_param,
            "rebase_source_partition_locator": rebase_source_partition_locator_param,
            "records_per_compacted_file": records_per_compacted_file_param,
            "s3_client_kwargs": {},
            "source_partition_locator": source_partition.locator,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )
