import ray
from moto import mock_s3
import pytest
import os
import json
import boto3
from typing import Any, Dict, List, Optional, Set
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.tests.compute.common import (
    setup_sort_and_partition_keys,
    TEST_S3_RCF_BUCKET_NAME,
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
)
from deltacat.tests.compute.testcases import (
    INCREMENTAL_TEST_CASES,
)

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
def teardown_local_deltacat_storage_db():
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


@pytest.fixture(scope="function")
def setup_local_deltacat_storage_conn():
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage


def setup_incremental_source_and_destination_tables(
    source_table_version: str,
    destination_table_version: str,
    primary_keys: Set[str],
    sort_keys: Optional[Dict[str, Any]],
    partition_keys: Optional[Dict[str, Any]],
    column_names: List[str],
    arrow_arrays: List[pa.Array],
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
    source_namespace: str = BASE_TEST_SOURCE_NAMESPACE,
    source_table_name: str = BASE_TEST_SOURCE_TABLE_NAME,
    destination_namespace: str = BASE_TEST_DESTINATION_NAMESPACE,
    destination_table_name: str = BASE_TEST_DESTINATION_TABLE_NAME,
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import Partition, Stream

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
    source_table_stream_after_committed: Stream = ds.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    return source_table_stream_after_committed, destination_table_stream


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
        "rebase_source_partition_locator_param",
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
            rebase_source_partition_locator_param,
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
            rebase_source_partition_locator_param,
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
        ) in INCREMENTAL_TEST_CASES.items()
    ],
    ids=[test_name for test_name in INCREMENTAL_TEST_CASES],
    indirect=[],
)
def test_compact_partition_incremental(
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
    rebase_source_partition_locator_param,
    partition_values_param,
    expected_result,
    validation_callback_func,  # use and implement func and func_kwargs if you want to run additional validations apart from the ones in the test
    validation_callback_func_kwargs,
    do_teardown_local_deltacat_storage_db,
    use_prev_compacted,
    create_placement_group_param,
    records_per_compacted_file_param,
    hash_bucket_count_param,
    compact_partition_func,
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

    # setup
    sort_keys, partition_keys = setup_sort_and_partition_keys(
        sort_keys_param, partition_keys_param
    )
    (
        source_table_stream,
        destination_table_stream,
    ) = setup_incremental_source_and_destination_tables(
        source_table_version,
        destination_table_version,
        primary_keys_param,
        sort_keys,
        partition_keys,
        column_names_param,
        arrow_arrays_param,
        partition_values_param,
        ds_mock_kwargs,
    )
    ray.shutdown()
    ray.init(local_mode=True)
    assert ray.is_initialized()
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
    num_workers, worker_instance_cpu = 1, 1
    total_cpus = num_workers * worker_instance_cpu
    pgm = None
    if create_placement_group_param:
        pgm = PlacementGroupManager(
            1, total_cpus, worker_instance_cpu, memory_per_bundle=4000000
        ).pgs[0]
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
    # execute
    rcf_file_s3_uri = compact_partition_func(compact_partition_params)
    # validate
    _, rcf_object_key = rcf_file_s3_uri.rsplit("/", 1)
    rcf_file_output: Dict[str, Any] = read_s3_contents(
        setup_s3_resource, TEST_S3_RCF_BUCKET_NAME, rcf_object_key
    )
    round_completion_info = RoundCompletionInfo(**rcf_file_output)
    print(f"rcf_file_output: {json.dumps(rcf_file_output, indent=2)}")
    compacted_delta_locator = round_completion_info.compacted_delta_locator
    tables = ds.download_delta(compacted_delta_locator, **ds_mock_kwargs)
    compacted_table = pa.concat_tables(tables)
    assert compacted_table.equals(
        expected_result
    ), f"{compacted_table} does not match {expected_result}"
    if (
        validation_callback_func is not None
        and validation_callback_func_kwargs is not None
    ):
        validation_callback_func(**validation_callback_func_kwargs)
    # https://docs.pytest.org/en/7.1.x/reference/reference.html#pytest.FixtureRequest.getfixturevalue
    if do_teardown_local_deltacat_storage_db:
        request.getfixturevalue("teardown_local_deltacat_storage_db")
    return
