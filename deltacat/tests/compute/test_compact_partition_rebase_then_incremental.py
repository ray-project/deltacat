import ray
import os
from moto import mock_s3
import pytest
import boto3
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.tests.compute.constants import (
    REBASING_NAMESPACE,
    REBASING_NAME_SUFFIX,
    RAY_COMPACTED_NAMESPACE,
    RAY_COMPACTED_NAME_SUFFIX,
    TEST_S3_RCF_BUCKET_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    BASE_TEST_DESTINATION_TABLE_VERSION,
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
    REBASING_NAMESPACE,
    RAY_COMPACTED_NAMESPACE,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
    MAX_RECORDS_PER_FILE,
)
from deltacat.tests.compute.common import (
    setup_general_source_and_destination_tables,
    setup_sort_and_partition_keys,
    offer_iso8601_timestamp_list,
    PartitionKey,
    get_compacted_delta_locator_from_rcf,
)
from deltacat.tests.compute.test_cases_compact_partition import (
    REBASE_THEN_INCREMENTAL_TEST_CASES,
)
from typing import Any, Callable, Dict, List, Set

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


@pytest.fixture(scope="function")
def teardown_local_deltacat_storage_db():
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


@pytest.mark.parametrize(
    [
        "test_name",
        "source_table_version",
        "destination_table_version",
        "primary_keys_param",
        "sort_keys_param",
        "partition_keys_param",
        "column_names_param",
        "arrow_arrays_param",
        "partition_values_param",
        "expected_result",
        "validation_callback_func",
        "validation_callback_func_kwargs",
        "do_teardown_local_deltacat_storage_db",
        "use_prev_compacted",
        "create_placement_group_param",
        "records_per_compacted_file_param",
        "hash_bucket_count_param",
        "compact_partition_func",
    ],
    [
        (
            test_name,
            source_table_version,
            destination_table_version,
            primary_keys_param,
            sort_keys_param,
            partition_keys_param,
            column_names_param,
            arrow_arrays_param,
            partition_values_param,
            expected_result,
            validation_callback_func,
            validation_callback_func_kwargs,
            do_teardown_local_deltacat_storage_db,
            use_prev_compacted,
            create_placement_group_param,
            records_per_compacted_file_param,
            hash_bucket_count_param,
            compact_partition_func,
        )
        for test_name, (
            source_table_version,
            destination_table_version,
            primary_keys_param,
            sort_keys_param,
            partition_keys_param,
            column_names_param,
            arrow_arrays_param,
            partition_values_param,
            expected_result,
            validation_callback_func,
            validation_callback_func_kwargs,
            do_teardown_local_deltacat_storage_db,
            use_prev_compacted,
            create_placement_group_param,
            records_per_compacted_file_param,
            hash_bucket_count_param,
            compact_partition_func,
        ) in REBASE_THEN_INCREMENTAL_TEST_CASES.items()
    ],
    ids=[test_name for test_name in REBASE_THEN_INCREMENTAL_TEST_CASES],
    indirect=[],
)
def test_compact_partition_rebase_then_incremental(
    request: pytest.FixtureRequest,
    setup_s3_resource: ServiceResource,
    setup_local_deltacat_storage_conn: Dict[str, Any],
    setup_compaction_artifacts_s3_bucket: None,
    test_name: str,
    source_table_version: str,
    destination_table_version: str,
    primary_keys_param: Set[str],
    sort_keys_param,
    partition_keys_param,
    column_names_param: List[str],
    arrow_arrays_param: List[pa.Array],
    partition_values_param,
    expected_result,
    validation_callback_func,  # use and implement func and func_kwargs if you want to run additional validations apart from the ones in the test
    validation_callback_func_kwargs,
    do_teardown_local_deltacat_storage_db,
    use_prev_compacted,
    create_placement_group_param,
    records_per_compacted_file_param,
    hash_bucket_count_param,
    compact_partition_func: Callable,
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import (
        Partition,
        PartitionLocator,
        Stream,
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
    """
    REBASE
    """

    source_namespace = BASE_TEST_SOURCE_NAMESPACE
    source_table_name = BASE_TEST_SOURCE_TABLE_NAME
    source_table_version = BASE_TEST_SOURCE_TABLE_VERSION

    rebasing_namespace = REBASING_NAMESPACE
    rebasing_table_name = BASE_TEST_SOURCE_TABLE_NAME + REBASING_NAME_SUFFIX
    rebasing_table_version = BASE_TEST_SOURCE_TABLE_VERSION

    destination_table_namespace = RAY_COMPACTED_NAMESPACE
    destination_table_name = (
        BASE_TEST_DESTINATION_TABLE_NAME + RAY_COMPACTED_NAME_SUFFIX
    )
    destination_table_version = BASE_TEST_DESTINATION_TABLE_VERSION

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
        source_namespace=source_namespace,
        source_table_name=source_table_name,
        source_table_version=source_table_version,
        destination_namespace=destination_table_namespace,
        destination_table_name=destination_table_name,
        destination_table_version=destination_table_version,
    )
    # rebase table
    ds.create_namespace(rebasing_namespace, {}, **ds_mock_kwargs)
    ds.create_table_version(
        rebasing_namespace,
        rebasing_table_name,
        rebasing_table_version,
        primary_key_column_names=list(primary_keys_param),
        sort_keys=sort_keys,
        partition_keys=partition_keys,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    rebasing_table_stream: Stream = ds.get_stream(
        namespace=rebasing_namespace,
        table_name=rebasing_table_name,
        table_version=rebasing_table_version,
        **ds_mock_kwargs,
    )
    test_table: pa.Table = pa.Table.from_arrays(
        [
            pa.array([str(i) for i in range(10)]),
            pa.array([i for i in range(10, 20)]),
            pa.array(["foo"] * 10),
        ],
        names=column_names_param,
    )
    staged_partition: Partition = ds.stage_partition(
        rebasing_table_stream, partition_values_param, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(test_table, staged_partition, **ds_mock_kwargs), **ds_mock_kwargs
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    rebased_stream_after_committed: Stream = ds.get_stream(
        namespace=rebasing_namespace,
        table_name=rebasing_table_name,
        table_version=rebasing_table_version,
        **ds_mock_kwargs,
    )
    rebased_partition: Partition = ds.get_partition(
        rebased_stream_after_committed.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
    source_partition: Partition = ds.get_partition(
        source_table_stream.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
    destination_partition_locator: PartitionLocator = PartitionLocator.of(
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
            "properties": {
                "parent_stream_position": str(10),
            },
            "rebase_source_partition_locator": source_partition.locator,
            "records_per_compacted_file": records_per_compacted_file_param,
            "s3_client_kwargs": {},
            "source_partition_locator": rebased_partition.locator,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )
    # execute
    rcf_file_s3_uri = compact_partition_func(compact_partition_params)
    compacted_delta_locator = get_compacted_delta_locator_from_rcf(
        setup_s3_resource, rcf_file_s3_uri
    )
    tables = ds.download_delta(compacted_delta_locator, **ds_mock_kwargs)
    compacted_table = pa.concat_tables(tables)
    assert compacted_table.equals(
        expected_result
    ), f"{compacted_table} does not match {expected_result}"

    """
    INCREMENTAL
    """

    destination_table_stream: Stream = ds.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    destination_partition_locator: PartitionLocator = PartitionLocator.of(
        destination_table_stream.locator,
        partition_values_param,
        None,
    )
    test_table: pa.Table = pa.Table.from_arrays(
        [
            pa.array([str(i) for i in range(10)]),
            pa.array([i for i in range(20, 30)]),
            pa.array(["foo"] * 10),
        ],
        names=column_names_param,
    )
    staged_partition: Partition = ds.stage_partition(
        source_table_stream, partition_values_param, **ds_mock_kwargs
    )
    ds.commit_delta(
        ds.stage_delta(test_table, staged_partition, **ds_mock_kwargs), **ds_mock_kwargs
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    source_table_stream_after_committed: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    source_partition: Partition = ds.get_partition(
        source_table_stream_after_committed.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
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
            "properties": {
                "parent_stream_position": str(10),
            },
            "rebase_source_partition_locator": None,
            "records_per_compacted_file": records_per_compacted_file_param,
            "s3_client_kwargs": {},
            "source_partition_locator": source_partition.locator,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )
    rcf_file_s3_uri = compact_partition_func(compact_partition_params)
    compacted_delta_locator = get_compacted_delta_locator_from_rcf(
        setup_s3_resource, rcf_file_s3_uri
    )
    tables = ds.download_delta(compacted_delta_locator, **ds_mock_kwargs)
    compacted_table = pa.concat_tables(tables)
    assert compacted_table.equals(
        pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(20, 30)]),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        )
    ), f"{compacted_table} does not match {expected_result}"