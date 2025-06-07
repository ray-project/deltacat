import ray
import os
from moto import mock_s3
import pytest
import boto3
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.io.file_object_store import FileObjectStore
from pytest_benchmark.fixture import BenchmarkFixture
import tempfile

from deltacat.tests.compute.test_util_constant import (
    TEST_S3_RCF_BUCKET_NAME,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
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
from deltacat.tests.compute.test_util_common import (
    get_rcf,
    PartitionKey,
)
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.tests.compute.test_util_common import (
    get_compacted_delta_locator_from_rcf,
)
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.tests.compute.compact_partition_multiple_rounds_test_cases import (
    MULTIPLE_ROUNDS_TEST_CASES,
)
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from deltacat.types.media import StorageType, ContentType
from deltacat.storage import (
    DeltaLocator,
    Partition,
    Stream,
    DeltaType,
    PartitionScheme,
    SortScheme,
)
from deltacat.storage.model.partition import PartitionKey as PartitionSchemeKey
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor import (
    RoundCompletionInfo,
)
from deltacat.utils.placement import (
    PlacementGroupManager,
)
from deltacat.catalog import CatalogProperties
from deltacat.storage import metastore
from deltacat.storage.model.schema import Schema


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
def main_deltacat_storage_kwargs(temp_dir):
    """
    Fixture that creates a CatalogProperties object for each test function
    using the main metastore implementation.
    
    Returns:
        dict: A dictionary with 'catalog' key pointing to CatalogProperties
    """
    catalog = CatalogProperties(root=temp_dir)
    kwargs = {"catalog": catalog}
    yield kwargs


@pytest.fixture(autouse=True, scope="function")
def enable_bucketing_spec_validation(monkeypatch):
    """
    Enable the bucketing spec validation for all tests.
    This will help catch hash bucket drift in testing.
    """
    import deltacat.compute.compactor_v2.steps.merge

    monkeypatch.setattr(
        deltacat.compute.compactor_v2.steps.merge,
        "BUCKETING_SPEC_COMPLIANCE_PROFILE",
        "ASSERT",
    )


@pytest.fixture(autouse=True, scope="function") 
def cleanup_the_database_file_after_all_compaction_session_package_tests_complete():
    """
    Cleanup fixture to prevent state contamination between tests.
    """
    yield
    # Cleanup happens automatically with temp_dir fixture


def create_main_deltacat_storage_kwargs() -> Dict[str, Any]:
    """
    Helper function to create main deltacat storage kwargs
    
    Returns: kwargs to use for main deltacat storage, i.e. {"catalog": CatalogProperties(...)}
    """
    temp_dir = tempfile.mkdtemp()
    catalog = CatalogProperties(root=temp_dir)
    return {"catalog": catalog}


def clean_up_main_deltacat_storage_kwargs(storage_kwargs: Dict[str, Any]):
    """
    Cleans up directory created by create_main_deltacat_storage_kwargs
    """
    import shutil
    catalog = storage_kwargs["catalog"]
    if hasattr(catalog, 'root') and os.path.exists(catalog.root):
        shutil.rmtree(catalog.root)


def _create_table_main(
    namespace: str,
    table_name: str,
    table_version: str,
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: List[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of _create_table
    """
    metastore.create_namespace(namespace, {}, **ds_mock_kwargs)
    # Import the correct PartitionKey and related classes
    from deltacat.storage.model.partition import PartitionKey as StoragePartitionKey
    from deltacat.storage.model.partition import PartitionKeyList
    from deltacat.storage.model.transform import IdentityTransform
    
    partition_scheme = None
    if partition_keys:
        # Convert test utility PartitionKeys to storage model PartitionKeys
        storage_partition_keys = [
            StoragePartitionKey.of(
                key=[key.key_name],  # Use the key_name as a list
                transform=IdentityTransform.of(),
            )
            for key in partition_keys
        ]
        partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(storage_partition_keys),
            scheme_id="default_partition_scheme"  # Add an ID for the partition scheme
        )
    sort_scheme = SortScheme.of(sort_keys) if sort_keys else None
    
    # Create schema from input deltas and add partition key fields
    schema = None
    if input_deltas:
        first_delta_tuple = input_deltas[0]
        first_delta_table = first_delta_tuple[0]  # Extract the pa.Table from the tuple
        
        # Start with the schema from the data
        data_schema = first_delta_table.schema
        schema_fields = list(data_schema)
        
        # Add partition key fields to the schema if they don't already exist
        if partition_keys:
            for key in partition_keys:
                key_name = key.key_name
                key_type = key.key_type
                
                # Check if the partition key is already in the data schema
                if key_name not in data_schema.names:
                    # Add the partition key field to the schema
                    if key_type == "int":
                        pa_type = pa.int32()
                    elif key_type == "string":
                        pa_type = pa.string()
                    elif key_type == "timestamp":
                        pa_type = pa.timestamp('us')
                    else:
                        pa_type = pa.string()  # Default to string
                    
                    schema_fields.append(pa.field(key_name, pa_type))
        
        # Create the combined schema
        combined_pa_schema = pa.schema(schema_fields)
        schema = Schema.of(schema=combined_pa_schema)
    
    metastore.create_table_version(
        namespace,
        table_name,
        table_version,
        schema=schema,
        sort_keys=sort_scheme,
        partition_scheme=partition_scheme,
        supported_content_types=[ContentType.PARQUET],
        **ds_mock_kwargs,
    )
    return namespace, table_name, table_version


def create_src_table_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: List[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of create_src_table
    """
    source_namespace: str = BASE_TEST_SOURCE_NAMESPACE
    source_table_name: str = BASE_TEST_SOURCE_TABLE_NAME
    source_table_version: str = BASE_TEST_SOURCE_TABLE_VERSION
    return _create_table_main(
        source_namespace,
        source_table_name,
        source_table_version,
        sort_keys,
        partition_keys,
        input_deltas,
        ds_mock_kwargs,
    )


def create_destination_table_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: List[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of create_destination_table
    """
    destination_namespace: str = BASE_TEST_DESTINATION_NAMESPACE
    destination_table_name: str = BASE_TEST_DESTINATION_TABLE_NAME
    destination_table_version: str = BASE_TEST_DESTINATION_TABLE_VERSION
    return _create_table_main(
        destination_namespace,
        destination_table_name,
        destination_table_version,
        sort_keys,
        partition_keys,
        input_deltas,
        ds_mock_kwargs,
    )


def create_rebase_table_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: List[pa.Table],
    ds_mock_kwargs: Optional[Dict[str, Any]],
):
    """
    Main storage version of create_rebase_table
    """
    rebasing_namespace = REBASING_NAMESPACE
    rebasing_table_name = REBASING_TABLE_NAME
    rebasing_table_version = REBASING_TABLE_VERSION
    return _create_table_main(
        rebasing_namespace,
        rebasing_table_name,
        rebasing_table_version,
        sort_keys,
        partition_keys,
        input_deltas,
        ds_mock_kwargs,
    )


def multiple_rounds_create_src_w_deltas_destination_rebase_w_deltas_strategy_main(
    sort_keys: Optional[List[Any]],
    partition_keys: Optional[List[PartitionKey]],
    input_deltas: List[pa.Table],
    partition_values: Optional[List[Any]],
    ds_mock_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Stream, Stream, Optional[Stream], bool]:
    """
    Main storage version of multiple_rounds_create_src_w_deltas_destination_rebase_w_deltas_strategy
    """
    from deltacat.storage import Partition, Stream

    source_namespace, source_table_name, source_table_version = create_src_table_main(
        sort_keys, partition_keys, input_deltas, ds_mock_kwargs
    )

    source_table_stream: Stream = metastore.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    # Convert partition values to correct types
    converted_partition_values = partition_values
    if partition_values and partition_keys:
        converted_partition_values = []
        for i, (value, key) in enumerate(zip(partition_values, partition_keys)):
            if key.key_type == "int":
                converted_partition_values.append(int(value))
            elif key.key_type == "string":
                converted_partition_values.append(str(value))
            elif key.key_type == "timestamp":
                converted_partition_values.append(value)  # Keep as is for now
            else:
                converted_partition_values.append(value)
    
    staged_partition: Partition = metastore.stage_partition(
        source_table_stream, converted_partition_values, **ds_mock_kwargs
    )
    is_delete = False
    input_delta_length = 0
    for (
        input_delta,
        input_delta_type,
        input_delta_parameters,
    ) in input_deltas:
        if input_delta_type is DeltaType.DELETE:
            is_delete = True
        staged_delta = metastore.stage_delta(
            input_delta,
            staged_partition,
            input_delta_type,
            entry_params=input_delta_parameters,
            **ds_mock_kwargs,
        )
        metastore.commit_delta(
            staged_delta,
            **ds_mock_kwargs,
        )
        input_delta_length += len(input_delta) if input_delta else 0
    metastore.commit_partition(staged_partition, **ds_mock_kwargs)
    source_table_stream_after_committed: Stream = metastore.get_stream(
        namespace=source_namespace,
        table_name=source_table_name,
        table_version=source_table_version,
        **ds_mock_kwargs,
    )
    # create the destination table
    (
        destination_table_namespace,
        destination_table_name,
        destination_table_version,
    ) = create_destination_table_main(sort_keys, partition_keys, input_deltas, ds_mock_kwargs)
    # create the rebase table
    (
        rebase_table_namespace,
        rebase_table_name,
        rebase_table_version,
    ) = create_rebase_table_main(sort_keys, partition_keys, input_deltas, ds_mock_kwargs)
    rebasing_table_stream: Stream = metastore.get_stream(
        namespace=rebase_table_namespace,
        table_name=rebase_table_name,
        table_version=rebase_table_version,
        **ds_mock_kwargs,
    )
    staged_partition: Partition = metastore.stage_partition(
        rebasing_table_stream, converted_partition_values, **ds_mock_kwargs
    )
    input_delta_length = 0
    for (
        input_delta,
        input_delta_type,
        input_delta_parameters,
    ) in input_deltas:
        if input_delta_type is DeltaType.DELETE:
            is_delete = True
        staged_delta = metastore.stage_delta(
            input_delta,
            staged_partition,
            input_delta_type,
            entry_params=input_delta_parameters,
            **ds_mock_kwargs,
        )
        metastore.commit_delta(
            staged_delta,
            **ds_mock_kwargs,
        )
        input_delta_length += len(input_delta) if input_delta else 0
    metastore.commit_partition(staged_partition, **ds_mock_kwargs)

    # get streams
    destination_table_stream: Stream = metastore.get_stream(
        namespace=destination_table_namespace,
        table_name=destination_table_name,
        table_version=destination_table_version,
        **ds_mock_kwargs,
    )
    rebased_stream_after_committed: Stream = metastore.get_stream(
        namespace=rebase_table_namespace,
        table_name=rebase_table_name,
        table_version=rebase_table_version,
        **ds_mock_kwargs,
    )
    return (
        source_table_stream_after_committed,
        destination_table_stream,
        rebased_stream_after_committed,
        is_delete,
    )


@pytest.mark.parametrize(
    [
        "test_name",
        "primary_keys",
        "sort_keys",
        "partition_keys_param",
        "partition_values_param",
        "input_deltas_param",
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
        "rebase_expected_compact_partition_result",
        "num_rounds_param",
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
            rebase_expected_compact_partition_result,
            num_rounds_param,
            compact_partition_func,
            compactor_version,
        )
        for test_name, (
            primary_keys,
            sort_keys,
            partition_keys_param,
            partition_values_param,
            input_deltas,
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
            rebase_expected_compact_partition_result,
            num_rounds_param,
            compact_partition_func,
            compactor_version,
        ) in MULTIPLE_ROUNDS_TEST_CASES.items()
    ],
    ids=[test_name for test_name in MULTIPLE_ROUNDS_TEST_CASES],
)
def test_compact_partition_rebase_multiple_rounds_same_source_and_destination_main(
    mocker,
    s3_resource: ServiceResource,
    main_deltacat_storage_kwargs: Dict[str, Any],
    test_name: str,
    primary_keys: Set[str],
    sort_keys: List[Optional[Any]],
    partition_keys_param: Optional[List[PartitionKey]],
    partition_values_param: List[Optional[str]],
    input_deltas_param: List[pa.Array],
    expected_terminal_compact_partition_result: pa.Table,
    expected_terminal_exception: BaseException,
    expected_terminal_exception_message: Optional[str],
    create_placement_group_param: bool,
    records_per_compacted_file_param: int,
    hash_bucket_count_param: int,
    drop_duplicates_param: bool,
    read_kwargs_provider_param: Any,
    rebase_expected_compact_partition_result: pa.Table,
    skip_enabled_compact_partition_drivers: List[CompactorVersion],
    assert_compaction_audit: Optional[Callable],
    compactor_version: Optional[CompactorVersion],
    compact_partition_func: Callable,
    num_rounds_param: int,
    benchmark: BenchmarkFixture,
):
    ds_mock_kwargs = main_deltacat_storage_kwargs
    """
    This test tests different multi-round compaction rebase configurations,
    as specified in compact_partition_multiple_rounds_test_cases.py.
    These tests do not test multi-round compaction backfill, which is
    currently unsupported.
    
    This version uses the main metastore implementation instead of local storage.
    """
    (
        source_table_stream,
        _,
        rebased_table_stream,
        _,
    ) = multiple_rounds_create_src_w_deltas_destination_rebase_w_deltas_strategy_main(
        sort_keys,
        partition_keys_param,
        input_deltas_param,
        partition_values_param,
        ds_mock_kwargs,
    )
    # Convert partition values for partition lookup (same as in the helper function)
    converted_partition_values_for_lookup = partition_values_param
    if partition_values_param and partition_keys_param:
        converted_partition_values_for_lookup = []
        for i, (value, key) in enumerate(zip(partition_values_param, partition_keys_param)):
            if key.key_type == "int":
                converted_partition_values_for_lookup.append(int(value))
            elif key.key_type == "string":
                converted_partition_values_for_lookup.append(str(value))
            elif key.key_type == "timestamp":
                converted_partition_values_for_lookup.append(value)  # Keep as is for now
            else:
                converted_partition_values_for_lookup.append(value)
    

    
    source_partition: Partition = metastore.get_partition(
        stream_locator=source_table_stream.locator,
        partition_values=converted_partition_values_for_lookup,
        partition_scheme_id=source_table_stream.partition_scheme.id,
        **ds_mock_kwargs,
    )
    rebased_partition: Partition = metastore.get_partition(
        stream_locator=rebased_table_stream.locator,
        partition_values=converted_partition_values_for_lookup,
        partition_scheme_id=rebased_table_stream.partition_scheme.id,
        **ds_mock_kwargs,
    )

    total_cpus = DEFAULT_NUM_WORKERS * DEFAULT_WORKER_INSTANCE_CPUS
    pgm = None
    if create_placement_group_param:
        pgm = PlacementGroupManager(
            1, total_cpus, DEFAULT_WORKER_INSTANCE_CPUS, memory_per_bundle=4000000
        ).pgs[0]
    with tempfile.TemporaryDirectory() as test_dir:
        compact_partition_params = CompactPartitionParams.of(
            {
                "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
                "compacted_file_content_type": ContentType.PARQUET,
                "dd_max_parallelism_ratio": 1.0,
                "deltacat_storage": metastore,
                "deltacat_storage_kwargs": ds_mock_kwargs,
                "destination_partition_locator": rebased_partition.locator,
                "hash_bucket_count": hash_bucket_count_param,
                "last_stream_position_to_compact": source_partition.stream_position,
                "list_deltas_kwargs": {
                    **ds_mock_kwargs,
                    **{"equivalent_table_types": []},
                },
                "object_store": FileObjectStore(test_dir),
                "pg_config": pgm,
                "primary_keys": primary_keys,
                "read_kwargs_provider": read_kwargs_provider_param,
                "rebase_source_partition_locator": source_partition.locator,
                "rebase_source_partition_high_watermark": rebased_partition.stream_position,
                "records_per_compacted_file": records_per_compacted_file_param,
                "s3_client_kwargs": {},
                "source_partition_locator": rebased_partition.locator,
                "sort_keys": sort_keys if sort_keys else None,
                "num_rounds": num_rounds_param,
                "drop_duplicates": drop_duplicates_param,
                "min_delta_bytes": 560,
            }
        )
        if expected_terminal_exception:
            with pytest.raises(expected_terminal_exception) as exc_info:
                benchmark(compact_partition_func, compact_partition_params)
            assert expected_terminal_exception_message in str(exc_info.value)
            return
        from deltacat.compute.compactor_v2.model.evaluate_compaction_result import (
            ExecutionCompactionResult,
        )

        execute_compaction_result_spy = mocker.spy(
            ExecutionCompactionResult, "__init__"
        )
        object_store_clear_spy = mocker.spy(FileObjectStore, "clear")

        # execute
        rcf_file_s3_uri = benchmark(compact_partition_func, compact_partition_params)

        round_completion_info: RoundCompletionInfo = get_rcf(
            s3_resource, rcf_file_s3_uri
        )
        audit_bucket, audit_key = RoundCompletionInfo.get_audit_bucket_name_and_key(
            round_completion_info.compaction_audit_url
        )

        compaction_audit_obj: Dict[str, Any] = read_s3_contents(
            s3_resource, audit_bucket, audit_key
        )
        compaction_audit: CompactionSessionAuditInfo = CompactionSessionAuditInfo(
            **compaction_audit_obj
        )

        # assert if RCF covers all files
        # multiple rounds feature is only supported in V2 compactor
        previous_end = None
        for start, end in round_completion_info.hb_index_to_entry_range.values():
            assert (previous_end is None and start == 0) or start == previous_end
            previous_end = end
        assert (
            previous_end == round_completion_info.compacted_pyarrow_write_result.files
        )

        # Assert not in-place compacted
        assert (
            execute_compaction_result_spy.call_args.args[-1] is False
        ), "Table version erroneously marked as in-place compacted!"
        compacted_delta_locator: DeltaLocator = get_compacted_delta_locator_from_rcf(
            s3_resource, rcf_file_s3_uri
        )
        tables = metastore.download_delta(
            compacted_delta_locator, storage_type=StorageType.LOCAL, **ds_mock_kwargs
        )
        actual_rebase_compacted_table = pa.concat_tables(tables)
        # if no primary key is specified then sort by sort_key for consistent assertion
        sorting_cols: List[Any] = (
            [(val, "ascending") for val in primary_keys]
            if primary_keys
            else [pa_key for key in sort_keys for pa_key in key.arrow]
            if sort_keys
            else []
        )
        rebase_expected_compact_partition_result = (
            rebase_expected_compact_partition_result.combine_chunks().sort_by(
                sorting_cols
            )
        )
        actual_rebase_compacted_table = (
            actual_rebase_compacted_table.combine_chunks().sort_by(sorting_cols)
        )
        assert actual_rebase_compacted_table.equals(
            rebase_expected_compact_partition_result
        ), f"{actual_rebase_compacted_table} does not match {rebase_expected_compact_partition_result}"

        if assert_compaction_audit:
            if not assert_compaction_audit(compactor_version, compaction_audit):
                assert False, "Compaction audit assertion failed"
        assert object_store_clear_spy.call_count, "Object store was never cleaned up!"
        return 