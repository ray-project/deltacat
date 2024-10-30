import ray
import os
from moto import mock_s3
import pytest
import boto3
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from pytest_benchmark.fixture import BenchmarkFixture

from deltacat.tests.compute.test_util_constant import (
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    TEST_S3_RCF_BUCKET_NAME,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
)
from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.tests.compute.test_util_common import (
    get_rcf,
)
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.tests.compute.test_util_common import (
    get_compacted_delta_locator_from_rcf,
)
from deltacat.tests.compute.test_util_create_table_deltas_repo import (
    create_incremental_deltas_on_source_table,
)
from deltacat.tests.compute.test_util_create_table_deltas_repo import (
    create_src_w_deltas_destination_rebase_w_deltas_strategy,
)
from deltacat.tests.compute.compact_partition_rebase_then_incremental_test_cases import (
    REBASE_THEN_INCREMENTAL_TEST_CASES,
)
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from deltacat.types.media import StorageType
from deltacat.storage import (
    DeltaType,
    DeltaLocator,
    Partition,
    PartitionLocator,
)
from deltacat.types.media import ContentType
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.utils.placement import (
    PlacementGroupManager,
)
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)

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
    ray.shutdown()


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
def s3_resource(mock_aws_credential):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(autouse=True, scope="module")
def setup_compaction_artifacts_s3_bucket(s3_resource: ServiceResource):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield


"""
FUNCTION scoped fixtures
"""


@pytest.fixture(scope="function")
def local_deltacat_storage_kwargs(request: pytest.FixtureRequest):
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
        "input_deltas_param",
        "input_deltas_delta_type",
        "expected_terminal_compact_partition_result",
        "expected_terminal_exception",
        "expected_terminal_exception_message",
        "create_placement_group_param",
        "records_per_compacted_file_param",
        "hash_bucket_count_param",
        "read_kwargs_provider_param",
        "drop_duplicates_param",
        "skip_enabled_compact_partition_drivers",
        "assert_compaction_audit",
        "incremental_deltas",
        "rebase_expected_compact_partition_result",
        "compact_partition_func",
        "compactor_version",
    ],
    [
        (
            test_name,
            primary_keys,
            sort_keys,
            partition_keys_param,
            partition_values_param,
            input_deltas,
            input_deltas_delta_type,
            expected_terminal_compact_partition_result,
            expected_terminal_exception,
            expected_terminal_exception_message,
            create_placement_group_param,
            records_per_compacted_file_param,
            hash_bucket_count_param,
            drop_duplicates_param,
            read_kwargs_provider,
            skip_enabled_compact_partition_drivers,
            assert_compaction_audit,
            incremental_deltas,
            rebase_expected_compact_partition_result,
            compact_partition_func,
            compactor_version,
        )
        for test_name, (
            primary_keys,
            sort_keys,
            partition_keys_param,
            partition_values_param,
            input_deltas,
            input_deltas_delta_type,
            expected_terminal_compact_partition_result,
            expected_terminal_exception,
            expected_terminal_exception_message,
            create_placement_group_param,
            records_per_compacted_file_param,
            hash_bucket_count_param,
            drop_duplicates_param,
            read_kwargs_provider,
            skip_enabled_compact_partition_drivers,
            assert_compaction_audit,
            incremental_deltas,
            rebase_expected_compact_partition_result,
            compact_partition_func,
            compactor_version,
        ) in REBASE_THEN_INCREMENTAL_TEST_CASES.items()
    ],
    ids=[test_name for test_name in REBASE_THEN_INCREMENTAL_TEST_CASES],
)
def test_compact_partition_rebase_then_incremental(
    s3_resource: ServiceResource,
    local_deltacat_storage_kwargs: Dict[str, Any],
    test_name: str,
    primary_keys: Set[str],
    sort_keys: List[Optional[Any]],
    partition_keys_param: Optional[List[Any]],
    partition_values_param: List[Optional[str]],
    input_deltas_param: List[pa.Array],
    input_deltas_delta_type: str,
    expected_terminal_compact_partition_result: pa.Table,
    expected_terminal_exception: BaseException,
    expected_terminal_exception_message: Optional[str],
    create_placement_group_param: bool,
    records_per_compacted_file_param: int,
    hash_bucket_count_param: int,
    drop_duplicates_param: bool,
    read_kwargs_provider_param: Any,
    incremental_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    rebase_expected_compact_partition_result: pa.Table,
    skip_enabled_compact_partition_drivers: List[CompactorVersion],
    assert_compaction_audit: Optional[Callable],
    compactor_version: Optional[CompactorVersion],
    compact_partition_func: Callable,
    benchmark: BenchmarkFixture,
):
    import deltacat.tests.local_deltacat_storage as ds

    ds_mock_kwargs = local_deltacat_storage_kwargs
    """
    REBASE
    """
    partition_keys = partition_keys_param
    (
        source_table_stream,
        destination_table_stream,
        rebased_table_stream,
    ) = create_src_w_deltas_destination_rebase_w_deltas_strategy(
        primary_keys,
        sort_keys,
        partition_keys,
        input_deltas_param,
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
            "object_store": RayPlasmaObjectStore(),
            "pg_config": pgm,
            "primary_keys": primary_keys,
            "read_kwargs_provider": read_kwargs_provider_param,
            "rebase_source_partition_locator": source_partition.locator,
            "records_per_compacted_file": records_per_compacted_file_param,
            "s3_client_kwargs": {},
            "source_partition_locator": rebased_partition.locator,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )
    # execute
    rcf_file_s3_uri = benchmark(compact_partition_func, compact_partition_params)
    compacted_delta_locator: DeltaLocator = get_compacted_delta_locator_from_rcf(
        s3_resource, rcf_file_s3_uri
    )
    tables = ds.download_delta(
        compacted_delta_locator, storage_type=StorageType.LOCAL, **ds_mock_kwargs
    )
    actual_rebase_compacted_table = pa.concat_tables(tables)
    # if no primary key is specified then sort by sort_key for consistent assertion
    sorting_cols: List[Any] = (
        [(val, "ascending") for val in primary_keys] if primary_keys else sort_keys
    )
    rebase_expected_compact_partition_result = (
        rebase_expected_compact_partition_result.combine_chunks().sort_by(sorting_cols)
    )
    actual_rebase_compacted_table = (
        actual_rebase_compacted_table.combine_chunks().sort_by(sorting_cols)
    )
    assert actual_rebase_compacted_table.equals(
        rebase_expected_compact_partition_result
    ), f"{actual_rebase_compacted_table} does not match {rebase_expected_compact_partition_result}"
    """
    INCREMENTAL
    """
    (
        source_partition_locator_w_deltas,
        new_delta,
        incremental_delta_length,
        has_delete_deltas,
    ) = create_incremental_deltas_on_source_table(
        BASE_TEST_SOURCE_NAMESPACE,
        BASE_TEST_SOURCE_TABLE_NAME,
        BASE_TEST_SOURCE_TABLE_VERSION,
        source_table_stream,
        partition_values_param,
        incremental_deltas,
        ds_mock_kwargs,
    )
    compact_partition_params = CompactPartitionParams.of(
        {
            "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
            "compacted_file_content_type": ContentType.PARQUET,
            "dd_max_parallelism_ratio": 1.0,
            "deltacat_storage": ds,
            "deltacat_storage_kwargs": ds_mock_kwargs,
            "destination_partition_locator": compacted_delta_locator.partition_locator,
            "drop_duplicates": drop_duplicates_param,
            "hash_bucket_count": hash_bucket_count_param,
            "last_stream_position_to_compact": new_delta.stream_position,
            "list_deltas_kwargs": {**ds_mock_kwargs, **{"equivalent_table_types": []}},
            "object_store": RayPlasmaObjectStore(),
            "pg_config": pgm,
            "primary_keys": primary_keys,
            "read_kwargs_provider": read_kwargs_provider_param,
            "rebase_source_partition_locator": None,
            "rebase_source_partition_high_watermark": None,
            "records_per_compacted_file": records_per_compacted_file_param,
            "s3_client_kwargs": {},
            "source_partition_locator": source_partition_locator_w_deltas,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )
    if expected_terminal_exception:
        with pytest.raises(expected_terminal_exception) as exc_info:
            compact_partition_func(compact_partition_params)
        assert expected_terminal_exception_message in str(exc_info.value)
        return
    rcf_file_s3_uri = compact_partition_func(compact_partition_params)
    round_completion_info = get_rcf(s3_resource, rcf_file_s3_uri)
    compacted_delta_locator_incremental: DeltaLocator = (
        round_completion_info.compacted_delta_locator
    )
    # assert if RCF covers all files
    if compactor_version != CompactorVersion.V1.value:
        previous_end = None
        for start, end in round_completion_info.hb_index_to_entry_range.values():
            assert (previous_end is None and start == 0) or start == previous_end
            previous_end = end
        assert (
            previous_end == round_completion_info.compacted_pyarrow_write_result.files
        )

    audit_bucket, audit_key = round_completion_info.compaction_audit_url.replace(
        "s3://", ""
    ).split("/", 1)
    compaction_audit_obj: dict = read_s3_contents(s3_resource, audit_bucket, audit_key)
    compaction_audit: CompactionSessionAuditInfo = CompactionSessionAuditInfo(
        **compaction_audit_obj
    )

    tables = ds.download_delta(
        compacted_delta_locator_incremental,
        storage_type=StorageType.LOCAL,
        **ds_mock_kwargs,
    )
    actual_compacted_table = pa.concat_tables(tables)
    expected_terminal_compact_partition_result = (
        expected_terminal_compact_partition_result.combine_chunks().sort_by(
            sorting_cols
        )
    )
    actual_compacted_table = actual_compacted_table.combine_chunks().sort_by(
        sorting_cols
    )
    # NOTE: if delete type-deltas are present this relationship no longer holds true
    if not has_delete_deltas:
        assert compaction_audit.input_records == (
            incremental_delta_length if incremental_deltas else 0
        ) + len(actual_rebase_compacted_table), (
            " Total input records must be equal to incremental deltas"
            " + previous compacted table size"
        )

    assert actual_compacted_table.equals(
        expected_terminal_compact_partition_result
    ), f"{actual_compacted_table} does not match {expected_terminal_compact_partition_result}"

    if assert_compaction_audit is not None:
        if not assert_compaction_audit(compactor_version, compaction_audit):
            assert False, "Compaction audit assertion failed"
    return
