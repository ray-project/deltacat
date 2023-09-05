import ray
import os
from moto import mock_s3
import pytest
import boto3
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.tests.compute.test_util_constant import (
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    TEST_S3_RCF_BUCKET_NAME,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
    DEFAULT_MAX_RECORDS_PER_FILE,
)
from deltacat.tests.compute.test_util_common import (
    setup_partition_keys,
    get_compacted_delta_locator_from_rcf,
)
from deltacat.tests.compute.compact_partition_test_cases import (
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
def setup_ray_cluster():
    ray.init(local_mode=True, ignore_reinit_error=True)
    yield


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


@pytest.fixture(autouse=True, scope="module")
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
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


@pytest.mark.parametrize(
    [
        "test_name",
        "primary_keys",
        "sort_keys",
        "partition_keys_param",
        "partition_values_param",
        "column_names_param",
        "input_deltas_arrow_arrays_param",
        "input_deltas_delta_type",
        "expected_terminal_compact_partition_result",
        "create_placement_group_param",
        "records_per_compacted_file_param",
        "hash_bucket_count_param",
        "create_table_strategy",
        "incremental_deltas_arrow_arrays_param",
        "incremental_deltas_delta_type",
        "rebase_expected_compact_partition_result",
        "compact_partition_func",
    ],
    [
        (
            test_name,
            primary_keys,
            sort_keys,
            partition_keys_param,
            partition_values_param,
            column_names_param,
            input_deltas_arrow_arrays_param,
            input_deltas_delta_type,
            expected_terminal_compact_partition_result,
            create_placement_group_param,
            records_per_compacted_file_param,
            hash_bucket_count_param,
            create_table_strategy,
            incremental_deltas_arrow_arrays_param,
            incremental_deltas_delta_type,
            rebase_expected_compact_partition_result,
            compact_partition_func,
        )
        for test_name, (
            primary_keys,
            sort_keys,
            partition_keys_param,
            partition_values_param,
            column_names_param,
            input_deltas_arrow_arrays_param,
            input_deltas_delta_type,
            expected_terminal_compact_partition_result,
            create_placement_group_param,
            records_per_compacted_file_param,
            hash_bucket_count_param,
            create_table_strategy,
            incremental_deltas_arrow_arrays_param,
            incremental_deltas_delta_type,
            rebase_expected_compact_partition_result,
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
    test_name: str,
    primary_keys: Set[str],
    sort_keys: Dict[str, str],
    partition_keys_param: Dict[str, str],
    partition_values_param: str,
    column_names_param: List[str],
    input_deltas_arrow_arrays_param: Dict[str, pa.Array],
    input_deltas_delta_type: str,
    expected_terminal_compact_partition_result: pa.Table,
    create_placement_group_param: bool,
    records_per_compacted_file_param: int,
    hash_bucket_count_param: int,
    create_table_strategy: Callable,
    incremental_deltas_arrow_arrays_param: Dict[str, pa.Array],
    incremental_deltas_delta_type: str,
    rebase_expected_compact_partition_result: pa.Table,
    compact_partition_func: Callable,
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import (
        Delta,
        DeltaLocator,
        Partition,
        PartitionLocator,
    )
    from deltacat.compute.compactor.model.compact_partition_params import (
        CompactPartitionParams,
    )
    from deltacat.utils.placement import (
        PlacementGroupManager,
    )

    ds_mock_kwargs = setup_local_deltacat_storage_conn
    ray.shutdown()
    ray.init(local_mode=True, ignore_reinit_error=True)
    """
    REBASE
    """

    partition_keys = setup_partition_keys(partition_keys_param)
    (
        source_table_stream,
        destination_table_stream,
        rebased_table_stream,
    ) = create_table_strategy(
        primary_keys,
        sort_keys,
        partition_keys,
        column_names_param,
        input_deltas_arrow_arrays_param,
        input_deltas_delta_type,
        partition_values_param,
        ds_mock_kwargs,
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
    rebased_partition: Partition = ds.get_partition(
        rebased_table_stream.locator,
        partition_values_param,
        **ds_mock_kwargs,
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
    records_per_compacted_file_param = DEFAULT_MAX_RECORDS_PER_FILE
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
            "primary_keys": primary_keys,
            "rebase_source_partition_locator": source_partition.locator,
            "records_per_compacted_file": records_per_compacted_file_param,
            "s3_client_kwargs": {},
            "source_partition_locator": rebased_partition.locator,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )
    # execute
    rcf_file_s3_uri = compact_partition_func(compact_partition_params)
    compacted_delta_locator: DeltaLocator = get_compacted_delta_locator_from_rcf(
        setup_s3_resource, rcf_file_s3_uri
    )
    tables = ds.download_delta(compacted_delta_locator, **ds_mock_kwargs)
    compacted_table = pa.concat_tables(tables)
    assert compacted_table.equals(
        rebase_expected_compact_partition_result
    ), f"{compacted_table} does not match {rebase_expected_compact_partition_result}"
    """
    INCREMENTAL
    """
    source_table_stream = ds.get_stream(
        BASE_TEST_SOURCE_NAMESPACE,
        BASE_TEST_SOURCE_TABLE_NAME,
        BASE_TEST_SOURCE_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    source_partition: Partition = ds.get_partition(
        source_table_stream.locator,
        partition_values_param,
        **ds_mock_kwargs,
    )
    incremental_deltas: pa.Table = pa.Table.from_arrays(
        incremental_deltas_arrow_arrays_param,
        names=column_names_param,
    )
    new_delta: Delta = ds.commit_delta(
        ds.stage_delta(incremental_deltas, source_partition, **ds_mock_kwargs),
        **ds_mock_kwargs,
    )
    destination_partition_locator: PartitionLocator = PartitionLocator.of(
        destination_table_stream.locator,
        partition_values_param,
        None,
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
            "last_stream_position_to_compact": new_delta.stream_position,
            "list_deltas_kwargs": {**ds_mock_kwargs, **{"equivalent_table_types": []}},
            "pg_config": pgm,
            "primary_keys": primary_keys,
            "rebase_source_partition_locator": None,
            "records_per_compacted_file": records_per_compacted_file_param,
            "s3_client_kwargs": {},
            "source_partition_locator": source_partition.locator,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )
    rcf_file_s3_uri = compact_partition_func(compact_partition_params)
    compacted_delta_locator: DeltaLocator = get_compacted_delta_locator_from_rcf(
        setup_s3_resource, rcf_file_s3_uri
    )
    tables = ds.download_delta(compacted_delta_locator, **ds_mock_kwargs)
    compacted_table = pa.concat_tables(tables)
    assert compacted_table.equals(
        expected_terminal_compact_partition_result
    ), f"{compacted_table} does not match {expected_terminal_compact_partition_result}"
