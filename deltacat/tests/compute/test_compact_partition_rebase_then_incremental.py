import tempfile
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
import uuid
import pytest

import pyarrow as pa
import ray
import pandas as pd

from deltacat.io.file_object_store import FileObjectStore
from pytest_benchmark.fixture import BenchmarkFixture

from deltacat.tests.compute.test_util_constant import (
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_SOURCE_TABLE_VERSION,
    DEFAULT_NUM_WORKERS,
    DEFAULT_WORKER_INSTANCE_CPUS,
)
from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.tests.compute.test_util_common import (
    create_src_w_deltas_destination_rebase_w_deltas_strategy_main,
    create_incremental_deltas_on_source_table_main,
    get_rci_from_partition,
    read_audit_file,
    PartitionKey,
    get_compacted_delta_locator_from_partition,
)
from deltacat.tests.compute.compact_partition_rebase_then_incremental_test_cases import (
    REBASE_THEN_INCREMENTAL_TEST_CASES,
)

from deltacat.types.media import StorageType
from deltacat.storage import (
    DeltaType,
    DeltaLocator,
    Partition,
    PartitionLocator,
    metastore,
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
def test_compact_partition_rebase_then_incremental_main(
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
    incremental_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]],
    rebase_expected_compact_partition_result: pa.Table,
    skip_enabled_compact_partition_drivers: List[CompactorVersion],
    assert_compaction_audit: Optional[Callable],
    compactor_version: Optional[CompactorVersion],
    compact_partition_func: Callable,
    benchmark: BenchmarkFixture,
):
    ds_mock_kwargs = main_deltacat_storage_kwargs
    """
    This test performs rebase compaction first, then incremental compaction on the same data.
    This tests the scenario where we first do a rebase (with different source/destination partitions)
    and then follow up with incremental compaction using the result of the rebase.

    This version uses the main metastore implementation instead of local storage.
    """

    """
    REBASE
    """
    partition_keys = partition_keys_param
    (
        source_table_stream,
        destination_table_stream,
        rebased_table_stream,
    ) = create_src_w_deltas_destination_rebase_w_deltas_strategy_main(
        sort_keys,
        partition_keys,
        input_deltas_param,
        input_deltas_delta_type,
        partition_values_param,
        ds_mock_kwargs,
    )

    # Convert partition values for partition lookup (same as in other helper functions)
    converted_partition_values_for_lookup = partition_values_param
    if partition_values_param and partition_keys:
        converted_partition_values_for_lookup = []
        for i, (value, pk) in enumerate(zip(partition_values_param, partition_keys)):
            if pk.key_type.value == "int":  # Use .value to get string representation
                converted_partition_values_for_lookup.append(int(value))
            elif pk.key_type.value == "timestamp":
                # Handle timestamp partition values
                if isinstance(value, str) and "T" in value and value.endswith("Z"):
                    ts = pd.to_datetime(value)
                    # Convert to microseconds since epoch for PyArrow timestamp[us]
                    converted_partition_values_for_lookup.append(
                        int(ts.timestamp() * 1_000_000)
                    )
                else:
                    converted_partition_values_for_lookup.append(value)
            else:
                converted_partition_values_for_lookup.append(value)

    source_partition: Partition = metastore.get_partition(
        source_table_stream.locator,
        converted_partition_values_for_lookup,
        **ds_mock_kwargs,
    )
    # Generate a destination partition ID based on the source partition
    destination_partition_id = str(uuid.uuid4())
    destination_partition_locator: PartitionLocator = PartitionLocator.of(
        destination_table_stream.locator,
        converted_partition_values_for_lookup,
        destination_partition_id,
    )
    all_column_names = metastore.get_table_version_column_names(
        destination_partition_locator.namespace,
        destination_partition_locator.table_name,
        destination_partition_locator.table_version,
        **ds_mock_kwargs,
    )
    rebased_partition: Partition = metastore.get_partition(
        rebased_table_stream.locator,
        converted_partition_values_for_lookup,
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

    with tempfile.TemporaryDirectory() as test_dir:
        # Extract catalog from storage kwargs
        catalog = ds_mock_kwargs.get("inner")

        compact_partition_params = CompactPartitionParams.of(
            {
                "catalog": catalog,
                "compacted_file_content_type": ContentType.PARQUET,
                "dd_max_parallelism_ratio": 1.0,
                "deltacat_storage": metastore,
                "deltacat_storage_kwargs": ds_mock_kwargs,
                "destination_partition_locator": destination_partition_locator,
                "hash_bucket_count": hash_bucket_count_param,
                "last_stream_position_to_compact": source_partition.stream_position,
                "list_deltas_kwargs": {
                    **ds_mock_kwargs,
                    **{"equivalent_table_types": []},
                },
                "object_store": FileObjectStore(test_dir),
                "original_fields": {
                    "pk_col_1",
                    "pk_col_2",
                    "sk_col_1",
                    "sk_col_2",
                    "col_1",
                    "col_2",
                    "region_id",
                },
                "pg_config": pgm,
                "primary_keys": primary_keys,
                "all_column_names": all_column_names,
                "read_kwargs_provider": read_kwargs_provider_param,
                "rebase_source_partition_locator": source_partition.locator,
                "records_per_compacted_file": records_per_compacted_file_param,
                "source_partition_locator": rebased_partition.locator,
                "sort_keys": sort_keys if sort_keys else None,
            }
        )
        # execute
        benchmark(compact_partition_func, compact_partition_params)
        compacted_delta_locator: DeltaLocator = (
            get_compacted_delta_locator_from_partition(
                destination_partition_locator,
                metastore,
                catalog=catalog,
            )
        )
        tables = metastore.download_delta(
            compacted_delta_locator,
            storage_type=StorageType.LOCAL,
            **ds_mock_kwargs,
        )
        actual_rebase_compacted_table = pa.concat_tables(tables)
        all_column_names = metastore.get_table_version_column_names(
            destination_partition_locator.namespace,
            destination_partition_locator.table_name,
            destination_partition_locator.table_version,
            **ds_mock_kwargs,
        )
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

        """
        INCREMENTAL
        """
        (
            source_partition_locator_w_deltas,
            new_delta,
            incremental_delta_length,
            has_delete_deltas,
        ) = create_incremental_deltas_on_source_table_main(
            BASE_TEST_SOURCE_NAMESPACE,
            BASE_TEST_SOURCE_TABLE_NAME,
            BASE_TEST_SOURCE_TABLE_VERSION,
            source_table_stream,
            partition_values_param,
            incremental_deltas,
            ds_mock_kwargs,
        )

        # Handle empty incremental deltas case
        if new_delta is None:
            # For empty incremental deltas, the expected result should be the same as rebase result
            # Skip incremental compaction and just verify the rebase result
            actual_compact_partition_result = actual_rebase_compacted_table
            compaction_audit = None
        else:
            # Perform incremental compaction when there are actual deltas
            last_stream_position = new_delta.stream_position

            compact_partition_params = CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": ds_mock_kwargs,
                    "destination_partition_locator": compacted_delta_locator.partition_locator,
                    "drop_duplicates": drop_duplicates_param,
                    "hash_bucket_count": hash_bucket_count_param,
                    "last_stream_position_to_compact": last_stream_position,
                    "list_deltas_kwargs": {
                        **ds_mock_kwargs,
                        **{"equivalent_table_types": []},
                    },
                    "object_store": FileObjectStore(test_dir),
                    "original_fields": {
                        "pk_col_1",
                        "pk_col_2",
                        "sk_col_1",
                        "sk_col_2",
                        "col_1",
                        "col_2",
                        "region_id",
                    },
                    "pg_config": pgm,
                    "primary_keys": primary_keys,
                    "all_column_names": all_column_names,
                    "read_kwargs_provider": read_kwargs_provider_param,
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": records_per_compacted_file_param,
                    "source_partition_locator": source_partition_locator_w_deltas,
                    "sort_keys": sort_keys if sort_keys else None,
                }
            )
            if expected_terminal_exception:
                with pytest.raises(expected_terminal_exception) as exc_info:
                    compact_partition_func(compact_partition_params)
                assert expected_terminal_exception_message in str(exc_info.value)
                return
            compact_partition_func(compact_partition_params)
            # assert
            compacted_delta_locator: DeltaLocator = (
                get_compacted_delta_locator_from_partition(
                    destination_partition_locator, metastore, catalog=catalog
                )
            )
            tables = metastore.download_delta(
                compacted_delta_locator,
                storage_type=StorageType.LOCAL,
                **ds_mock_kwargs,
            )
            actual_compact_partition_result = pa.concat_tables(tables)

            # Get compaction audit for verification if needed
            round_completion_info = get_rci_from_partition(
                destination_partition_locator, metastore, catalog=catalog
            )
            # Get catalog root for audit file resolution
            catalog_root = catalog.root

            compaction_audit_obj: dict = read_audit_file(
                round_completion_info.compaction_audit_url, catalog_root
            )
            compaction_audit = CompactionSessionAuditInfo(**compaction_audit_obj)

        # Verify the final result
        actual_compact_partition_result = (
            actual_compact_partition_result.combine_chunks().sort_by(sorting_cols)
        )
        expected_terminal_compact_partition_result = (
            expected_terminal_compact_partition_result.combine_chunks().sort_by(
                sorting_cols
            )
        )
        assert actual_compact_partition_result.equals(
            expected_terminal_compact_partition_result
        ), f"{actual_compact_partition_result} does not match {expected_terminal_compact_partition_result}"

        if assert_compaction_audit is not None and compaction_audit is not None:
            if not assert_compaction_audit(compactor_version, compaction_audit):
                pytest.fail("Compaction audit assertion failed")
        return
