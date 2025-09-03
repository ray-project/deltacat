import tempfile
from typing import Any, Dict, List, Optional, Set, Callable
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
    multiple_rounds_create_src_w_deltas_destination_rebase_w_deltas_strategy_main,
)

from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.tests.compute.compact_partition_multiple_rounds_test_cases import (
    MULTIPLE_ROUNDS_TEST_CASES,
)
from deltacat.types.media import StorageType, ContentType
from deltacat.storage import (
    DeltaLocator,
    Partition,
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
from deltacat.storage import metastore


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
        for i, (value, key) in enumerate(
            zip(partition_values_param, partition_keys_param)
        ):
            if key.key_type == "int":
                converted_partition_values_for_lookup.append(int(value))
            elif key.key_type == "string":
                converted_partition_values_for_lookup.append(str(value))
            elif key.key_type == "timestamp":
                converted_partition_values_for_lookup.append(
                    value
                )  # Keep as is for now
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
    all_column_names = metastore.get_table_version_column_names(
        rebased_table_stream.locator.table_locator.namespace,
        rebased_table_stream.locator.table_locator.table_name,
        rebased_table_stream.locator.table_version_locator.table_version,
        catalog=ds_mock_kwargs.get("inner"),
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
                "catalog": ds_mock_kwargs.get("inner"),
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
                "all_column_names": all_column_names,
                "read_kwargs_provider": read_kwargs_provider_param,
                "rebase_source_partition_locator": source_partition.locator,
                "rebase_source_partition_high_watermark": rebased_partition.stream_position,
                "records_per_compacted_file": records_per_compacted_file_param,
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
        benchmark(compact_partition_func, compact_partition_params)

        # Get RoundCompletionInfo from the compacted partition
        round_completion_info: RoundCompletionInfo = get_rci_from_partition(
            rebased_partition.locator, metastore, catalog=ds_mock_kwargs.get("inner")
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

        # assert if RCI covers all files
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
        compacted_delta_locator: DeltaLocator = (
            get_compacted_delta_locator_from_partition(
                rebased_partition.locator,
                metastore,
                catalog=ds_mock_kwargs.get("inner"),
            )
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
