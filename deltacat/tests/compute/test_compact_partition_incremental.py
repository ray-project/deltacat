import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
import uuid
import pytest

import pyarrow as pa
import ray

from pytest_benchmark.fixture import BenchmarkFixture
from deltacat.types.media import StorageType

from deltacat.tests.compute.test_util_common import (
    get_rci_from_partition,
    read_audit_file,
    PartitionKeyType,
)
from deltacat.tests.compute.test_util_common import (
    add_late_deltas_to_partition_main,
    create_src_w_deltas_destination_plus_destination_main,
)
from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.tests.compute.compact_partition_test_cases import (
    INCREMENTAL_TEST_CASES,
)
from deltacat.tests.compute.test_util_constant import (
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
)
from deltacat.compute.compactor import (
    RoundCompletionInfo,
)
from deltacat.storage import (
    CommitState,
    DeltaType,
    Delta,
    DeltaLocator,
    Partition,
    PartitionLocator,
    metastore,
)
from deltacat.types.media import ContentType
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.utils.placement import (
    PlacementGroupManager,
)
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


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


@pytest.fixture(scope="function")
def temp_dir(tmp_path):
    return str(tmp_path)


@pytest.mark.parametrize(
    [
        "test_name",
        "primary_keys",
        "sort_keys",
        "partition_keys_param",
        "partition_values_param",
        "input_deltas",
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
        "is_inplace",
        "add_late_deltas",
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
            input_deltas_param,
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
            is_inplace,
            add_late_deltas,
            compact_partition_func,
            compactor_version,
        )
        for test_name, (
            primary_keys,
            sort_keys,
            partition_keys_param,
            partition_values_param,
            input_deltas_param,
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
            is_inplace,
            add_late_deltas,
            compact_partition_func,
            compactor_version,
        ) in INCREMENTAL_TEST_CASES.items()
    ],
    ids=[test_name for test_name in INCREMENTAL_TEST_CASES],
)
def test_compact_partition_incremental_main(
    main_deltacat_storage_kwargs: Dict[str, Any],
    test_name: str,
    primary_keys: Set[str],
    sort_keys: Dict[str, str],
    partition_keys_param: Optional[Dict[str, str]],
    partition_values_param: str,
    input_deltas: pa.Table,
    input_deltas_delta_type: str,
    expected_terminal_compact_partition_result: pa.Table,
    expected_terminal_exception: BaseException,
    expected_terminal_exception_message: Optional[str],
    create_placement_group_param: bool,
    records_per_compacted_file_param: int,
    hash_bucket_count_param: int,
    drop_duplicates_param: bool,
    read_kwargs_provider_param: Any,
    skip_enabled_compact_partition_drivers,
    assert_compaction_audit: Optional[Callable],
    compactor_version: Optional[CompactorVersion],
    is_inplace: bool,
    add_late_deltas: Optional[List[Tuple[pa.Table, DeltaType]]],
    compact_partition_func: Callable,
    benchmark: BenchmarkFixture,
):
    # Skip in-place compaction tests for main storage as it's not yet implemented
    if is_inplace:
        pytest.skip(
            "In-place compaction not yet implemented in main storage (delta prepending limitation)"
        )

    ds_mock_kwargs: Dict[str, Any] = main_deltacat_storage_kwargs

    # Extract catalog from storage kwargs
    catalog = ds_mock_kwargs.get("inner")

    # setup
    partition_keys = partition_keys_param
    (
        source_table_stream,
        destination_table_stream,
        _,
        source_table_namespace,
        source_table_name,
        source_table_version,
    ) = create_src_w_deltas_destination_plus_destination_main(
        sort_keys,
        partition_keys,
        input_deltas,
        input_deltas_delta_type,
        partition_values_param,
        ds_mock_kwargs,
        is_inplace,
    )

    # Convert partition values to correct types for get_partition call
    converted_partition_values = []
    if partition_values_param and partition_keys:
        # partition_values_param is a single string, but we need to handle it as a list
        partition_values_list = (
            [partition_values_param]
            if isinstance(partition_values_param, str)
            else partition_values_param
        )
        for i, (value, pk) in enumerate(zip(partition_values_list, partition_keys)):
            if pk.key_type == PartitionKeyType.INT:
                converted_partition_values.append(int(value))
            else:
                converted_partition_values.append(value)
    else:
        converted_partition_values = (
            [partition_values_param] if partition_values_param else []
        )

    source_partition: Partition = metastore.get_partition(
        source_table_stream.locator,
        converted_partition_values,
        partition_scheme_id="default_partition_scheme" if partition_keys else None,
        **ds_mock_kwargs,
    )
    # Generate a destination partition ID based on the source partition
    destination_partition_id = str(uuid.uuid4())
    destination_partition_locator: PartitionLocator = PartitionLocator.of(
        destination_table_stream.locator,
        converted_partition_values,
        destination_partition_id,
    )
    num_workers, worker_instance_cpu = DEFAULT_NUM_WORKERS, DEFAULT_WORKER_INSTANCE_CPUS
    total_cpus: int = num_workers * worker_instance_cpu
    pgm: Optional[PlacementGroupManager] = (
        PlacementGroupManager(
            1, total_cpus, worker_instance_cpu, memory_per_bundle=4000000
        ).pgs[0]
        if create_placement_group_param
        else None
    )
    all_column_names = metastore.get_table_version_column_names(
        destination_table_stream.locator.table_locator.namespace,
        destination_table_stream.locator.table_locator.table_name,
        destination_table_stream.locator.table_version_locator.table_version,
        catalog=catalog,
    )
    compact_partition_params = CompactPartitionParams.of(
        {
            "catalog": catalog,
            "compacted_file_content_type": ContentType.PARQUET,
            "dd_max_parallelism_ratio": 1.0,
            "deltacat_storage": metastore,
            "deltacat_storage_kwargs": ds_mock_kwargs,
            "destination_partition_locator": destination_partition_locator,
            "drop_duplicates": drop_duplicates_param,
            "hash_bucket_count": hash_bucket_count_param,
            "last_stream_position_to_compact": source_partition.stream_position,
            "list_deltas_kwargs": {**ds_mock_kwargs, **{"equivalent_table_types": []}},
            "pg_config": pgm,
            "primary_keys": primary_keys,
            "all_column_names": all_column_names,
            "read_kwargs_provider": read_kwargs_provider_param,
            "rebase_source_partition_locator": None,
            "rebase_source_partition_high_watermark": None,
            "records_per_compacted_file": records_per_compacted_file_param,
            "source_partition_locator": source_partition.locator,
            "sort_keys": sort_keys if sort_keys else None,
        }
    )

    # execute
    def _incremental_compaction_setup():
        """
        This callable runs right before invoking the benchmark target function (compaction).
        This is needed as the benchmark module will invoke the target function multiple times
        in a single test run, which can lead to non-idempotent behavior if RCIs are generated.

        Returns: args, kwargs
        """
        return (compact_partition_params,), {}

    if add_late_deltas:
        # NOTE: In the case of in-place compaction it is plausible that new deltas may be added to the source partition during compaction
        # (so that the source_partitition.stream_position > last_stream_position_to_compact).
        # This parameter helps simulate the case to check that no late deltas are dropped even when the compacted partition is created.
        latest_delta, _ = add_late_deltas_to_partition_main(
            add_late_deltas, source_partition, ds_mock_kwargs
        )
    if expected_terminal_exception:
        with pytest.raises(expected_terminal_exception) as exc_info:
            compact_partition_func(compact_partition_params)
        assert expected_terminal_exception_message in str(exc_info.value)
        return
    benchmark.pedantic(compact_partition_func, setup=_incremental_compaction_setup)

    # validate - get RoundCompletionInfo from the compacted partition
    round_completion_info: RoundCompletionInfo = get_rci_from_partition(
        destination_partition_locator, metastore, catalog=catalog
    )
    compacted_delta_locator: DeltaLocator = (
        round_completion_info.compacted_delta_locator
    )

    # Get catalog root for audit file resolution
    catalog_root = catalog.root

    compaction_audit_obj: Dict[str, Any] = read_audit_file(
        round_completion_info.compaction_audit_url, catalog_root
    )

    compaction_audit: CompactionSessionAuditInfo = CompactionSessionAuditInfo(
        **compaction_audit_obj
    )

    # assert if RCI covers all files
    if compactor_version != CompactorVersion.V1.value:
        previous_end = None
        for start, end in round_completion_info.hb_index_to_entry_range.values():
            assert (previous_end is None and start == 0) or start == previous_end
            previous_end = end
        assert (
            previous_end == round_completion_info.compacted_pyarrow_write_result.files
        )

    tables = metastore.download_delta(
        compacted_delta_locator, storage_type=StorageType.LOCAL, **ds_mock_kwargs
    )
    actual_compacted_table = pa.concat_tables(tables)
    sorting_cols: List[Any] = [(val, "ascending") for val in primary_keys]
    # the compacted table may contain multiple files and chunks
    # and order of records may be incorrect due to multiple files.
    expected_terminal_compact_partition_result: pa.Table = (
        expected_terminal_compact_partition_result.combine_chunks().sort_by(
            sorting_cols
        )
    )
    actual_compacted_table = actual_compacted_table.combine_chunks().sort_by(
        sorting_cols
    )

    assert compaction_audit.input_records == len(
        input_deltas
    ), "The input_records must be equal to total records in the input"

    if assert_compaction_audit is not None:
        if not assert_compaction_audit(compactor_version, compaction_audit):
            assert False, "Compaction audit assertion failed"

    assert actual_compacted_table.equals(
        expected_terminal_compact_partition_result
    ), f"{actual_compacted_table} does not match {expected_terminal_compact_partition_result}"

    if is_inplace:
        assert (
            source_partition.locator.partition_values
            == destination_partition_locator.partition_values
            and source_partition.locator.stream_id
            == destination_partition_locator.stream_id
        ), f"The source partition: {source_partition.locator} should match the destination partition: {destination_partition_locator}"
        assert (
            compacted_delta_locator.stream_id == source_partition.locator.stream_id
        ), "The compacted delta should be in the same stream as the source"
        source_partition: Partition = metastore.get_partition(
            source_table_stream.locator,
            converted_partition_values,
            partition_scheme_id="default_partition_scheme" if partition_keys else None,
            **ds_mock_kwargs,
        )
        compacted_partition: Optional[Partition] = metastore.get_partition(
            compacted_delta_locator.stream_locator,
            converted_partition_values,
            partition_scheme_id="default_partition_scheme" if partition_keys else None,
            **ds_mock_kwargs,
        )
        assert (
            compacted_partition.state == source_partition.state == CommitState.COMMITTED
        ), f"The compacted/source table partition should be in {CommitState.COMMITTED} state and not {CommitState.DEPRECATED}"
        if add_late_deltas:
            compacted_partition_deltas: List[Delta] = metastore.list_partition_deltas(
                partition_like=compacted_partition,
                ascending_order=False,
                **ds_mock_kwargs,
            ).all_items()
            assert (
                len(compacted_partition_deltas) == len(add_late_deltas) + 1
            ), f"Expected the number of deltas within the newly promoted partition to equal 1 (the compacted delta) + the # of late deltas: {len(add_late_deltas)}"
            assert (
                compacted_partition_deltas[0].stream_position
                == latest_delta.stream_position
            ), f"Expected the latest delta in the compacted partition: {compacted_partition_deltas[0].stream_position} to have the same stream position as the latest delta: {latest_delta.stream_position}"
    return
