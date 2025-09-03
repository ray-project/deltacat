import tempfile
import os
from typing import Any, Callable, Dict, List, Optional, Set
import pytest
import pyarrow as pa
import ray

from deltacat.io.file_object_store import FileObjectStore
from pytest_benchmark.fixture import BenchmarkFixture

from deltacat.tests.compute.test_util_constant import (
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
)
from deltacat.tests.compute.test_util_common import (
    get_rci_from_partition,
    read_audit_file,
    PartitionKey,
    get_compacted_delta_locator_from_partition,
)
from deltacat.tests.compute.test_util_common import (
    create_src_w_deltas_destination_rebase_w_deltas_strategy_main,
)

from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.tests.compute.compact_partition_rebase_test_cases import (
    REBASE_TEST_CASES,
)
from deltacat.types.media import StorageType, ContentType
from deltacat.storage import (
    DeltaLocator,
    Partition,
    metastore,
)
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor import (
    RoundCompletionInfo,
)
from deltacat.utils.placement import (
    PlacementGroupManager,
)


"""
MODULE scoped fixtures
"""


@pytest.fixture(autouse=True, scope="module")
def setup_ray_cluster():
    ray.init(local_mode=True, ignore_reinit_error=True)
    yield
    ray.shutdown()


"""
FUNCTION scoped fixtures
"""


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
            rebase_expected_compact_partition_result,
            compact_partition_func,
            compactor_version,
        ) in REBASE_TEST_CASES.items()
    ],
    ids=[test_name for test_name in REBASE_TEST_CASES],
)
def test_compact_partition_rebase_same_source_and_destination_main(
    mocker,
    main_deltacat_storage_kwargs: Dict[str, Any],
    test_name: str,
    primary_keys: Set[str],
    sort_keys: List[Optional[Any]],
    partition_keys_param: Optional[List[PartitionKey]],
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
    rebase_expected_compact_partition_result: pa.Table,
    skip_enabled_compact_partition_drivers: List[CompactorVersion],
    assert_compaction_audit: Optional[Callable],
    compactor_version: Optional[CompactorVersion],
    compact_partition_func: Callable,
    benchmark: BenchmarkFixture,
):
    ds_mock_kwargs = main_deltacat_storage_kwargs
    """
    This test tests the scenario where source partition locator == destination partition locator,
    but rebase source partition locator is different.
    This scenario could occur when hash bucket count changes.

    This version uses the main metastore implementation instead of local storage.
    """
    partition_keys = partition_keys_param
    (
        source_table_stream,
        _,
        rebased_table_stream,
    ) = create_src_w_deltas_destination_rebase_w_deltas_strategy_main(
        sort_keys,
        partition_keys,
        input_deltas_param,
        input_deltas_delta_type,
        partition_values_param,
        ds_mock_kwargs,
    )

    # Convert partition values for partition lookup (same as in the helper function)
    converted_partition_values_for_lookup = partition_values_param
    if partition_values_param and partition_keys:
        converted_partition_values_for_lookup = []
        for i, (value, pk) in enumerate(zip(partition_values_param, partition_keys)):
            if pk.key_type.value == "int":  # Use .value to get string representation
                converted_partition_values_for_lookup.append(int(value))
            else:
                converted_partition_values_for_lookup.append(value)

    source_partition: Partition = metastore.get_partition(
        source_table_stream.locator,
        converted_partition_values_for_lookup,
        **ds_mock_kwargs,
    )
    rebased_partition: Partition = metastore.get_partition(
        rebased_table_stream.locator,
        converted_partition_values_for_lookup,
        **ds_mock_kwargs,
    )
    all_column_names = metastore.get_table_version_column_names(
        rebased_table_stream.locator.table_locator.namespace,
        rebased_table_stream.locator.table_locator.table_name,
        rebased_table_stream.locator.table_version_locator.table_version,
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
    last_stream_position_to_compact = source_partition.stream_position
    with tempfile.TemporaryDirectory() as test_dir:
        compact_partition_params = CompactPartitionParams.of(
            {
                "catalog": ds_mock_kwargs.get("inner"),
                "compacted_file_content_type": ContentType.PARQUET,
                "dd_max_parallelism_ratio": 1.0,
                "deltacat_storage": metastore,
                "deltacat_storage_kwargs": ds_mock_kwargs,
                "destination_partition_locator": rebased_partition.locator,
                "hash_bucket_count": hash_bucket_count_param,
                "last_stream_position_to_compact": last_stream_position_to_compact,
                "list_deltas_kwargs": {
                    **ds_mock_kwargs,
                    **{"equivalent_table_types": []},
                },
                "object_store": FileObjectStore(test_dir),
                "pg_config": pgm,
                "primary_keys": primary_keys,
                "all_column_names": all_column_names,
                "read_kwargs_provider": read_kwargs_provider_param,
                "rebase_source_partition_locator": source_partition.locator,
                "rebase_source_partition_high_watermark": rebased_partition.stream_position,
                "records_per_compacted_file": records_per_compacted_file_param,
                "source_partition_locator": rebased_partition.locator,
                "sort_keys": sort_keys if sort_keys else None,
                "drop_duplicates": drop_duplicates_param,
            }
        )

        from deltacat.compute.compactor_v2.model.evaluate_compaction_result import (
            ExecutionCompactionResult,
        )

        execute_compaction_result_spy = mocker.spy(
            ExecutionCompactionResult, "__init__"
        )
        object_store_put_many_spy = mocker.spy(FileObjectStore, "put_many")

        # execute
        benchmark(compact_partition_func, compact_partition_params)

        # Get RoundCompletionInfo from the compacted partition
        round_completion_info: RoundCompletionInfo = get_rci_from_partition(
            rebased_partition.locator, metastore, catalog=ds_mock_kwargs.get("inner")
        )

        # assert if RCI covers all files
        if compactor_version != CompactorVersion.V1.value:
            previous_end = None
            for start, end in round_completion_info.hb_index_to_entry_range.values():
                assert (previous_end is None and start == 0) or start == previous_end
                previous_end = end
            assert (
                previous_end
                == round_completion_info.compacted_pyarrow_write_result.files
            )

        # Get catalog root for audit file resolution
        catalog = ds_mock_kwargs.get("inner")
        catalog_root = catalog.root

        compaction_audit_obj: Dict[str, Any] = read_audit_file(
            round_completion_info.compaction_audit_url, catalog_root
        )
        compaction_audit: CompactionSessionAuditInfo = CompactionSessionAuditInfo(
            **compaction_audit_obj
        )

        # Assert not in-place compacted
        assert (
            execute_compaction_result_spy.call_args.args[-1] is False
        ), "Table version erroneously marked as in-place compacted!"
        compacted_delta_locator: DeltaLocator = (
            get_compacted_delta_locator_from_partition(
                rebased_partition.locator,
                metastore,
                catalog=ds_mock_kwargs.get("inner"),
            )
        )
        assert (
            compacted_delta_locator.stream_position == last_stream_position_to_compact
        ), "Compacted delta locator must be equal to last stream position"
        tables = metastore.download_delta(
            compacted_delta_locator, storage_type=StorageType.LOCAL, **ds_mock_kwargs
        )
        actual_rebase_compacted_table = pa.concat_tables(tables)
        # if no primary key is specified then sort by sort_key for consistent assertion
        sorting_cols: List[Any] = []
        if primary_keys:
            sorting_cols.extend([(val, "ascending") for val in primary_keys])
        if sort_keys:
            sorting_cols.extend([pa_key for key in sort_keys for pa_key in key.arrow])

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

        if assert_compaction_audit is not None:
            if not assert_compaction_audit(compactor_version, compaction_audit):
                assert False, "Compaction audit assertion failed"
        # We do not expect object store to be cleaned up when there's only one round
        if object_store_put_many_spy.call_count:
            assert os.listdir(test_dir) != []
