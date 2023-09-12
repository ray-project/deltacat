import pyarrow as pa
from typing import Callable, Dict, List, Optional, Set, Tuple
from deltacat.tests.compute.test_util_common import (
    offer_iso8601_timestamp_list,
)
from deltacat.tests.compute.test_util_constant import (
    DEFAULT_MAX_RECORDS_PER_FILE,
    DEFAULT_HASH_BUCKET_COUNT,
)
from dataclasses import dataclass, fields

from deltacat.compute.compactor.compaction_session import (
    compact_partition_from_request as compact_partition_v1,
)
from deltacat.tests.compute.test_util_create_deltas_strategy import (
    create_src_w_deltas_destination_strategy,
    create_src_w_deltas_destination_rebase_w_deltas_strategy,
)

from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor_v2.compaction_session import (
    compact_partition as compact_partition_v2,
)

from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.storage.model.sort_key import SortKey

ZERO_VALUED_SORT_KEY = []
ZERO_VALUED_PARTITION_KEYS_PARAM = None
ZERO_VALUED_PARTITION_VALUES_PARAM = []


ENABLED_COMPACT_PARTITIONS_DRIVERS: List[Tuple[CompactorVersion, Callable]] = [
    (CompactorVersion.V1, compact_partition_v1),
    (CompactorVersion.V2, compact_partition_v2),
]


@dataclass(frozen=True)
class CompactorTestCase:
    """
    A pytest parameterized test case for the `compact_partition` function.

    Args:
        primary_keys: Set[str] - argument for the primary_keys parameter in compact_partition
        sort_keys: List[SortKey] - argument for the sort_keys parameter in compact_partition
        partition_keys_param: List[Dict[str, str]] - argument needed for table version creation required for compact_partition tests
        partition_values_param: List[Optional[str]] - argument needed for partition staging in compact_partition test setup
        column_names_param: List[str] - argument required for delta creation during compact_partition test setup. Actual column names of the table
        input_deltas_arrow_arrays_param: List[pa.Array] - argument required for delta creation during compact_partition test setup. Actual incoming deltas expressed as a pyarrow array
        input_deltas_delta_type: DeltaType - argument required for delta creation during compact_partition test setup. Enum of the type of delta to apply (APPEND, UPSERT, DELETE)
        expected_terminal_compact_partition_result: pa.Table - expected table after compaction runs
        create_placement_group_param: bool - toggles whether to create a pg or not
        records_per_compacted_file_param: int - argument for the records_per_compacted_file parameter in compact_partition
        hash_bucket_count_param: int - argument for the hash_bucket_count parameter in compact_partition
        create_table_strategy: Callable - delta creation strategy
        skip_enabled_compact_partition_drivers: List[CompactorVersion] - skip whatever enabled_compact_partition_drivers are included in this list
    """

    primary_keys: Set[str]
    sort_keys: List[SortKey]
    partition_keys_param: List[Dict[str, str]]
    partition_values_param: List[Optional[str]]
    column_names_param: List[str]
    input_deltas_arrow_arrays_param: List[pa.Array]
    input_deltas_delta_type: DeltaType
    expected_terminal_compact_partition_result: pa.Table
    create_placement_group_param: bool
    records_per_compacted_file_param: int
    hash_bucket_count_param: int
    create_table_strategy: Callable
    skip_enabled_compact_partition_drivers: List[CompactorVersion]

    # makes CompactorTestCase iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@dataclass(frozen=True)
class IncrementalCompactionTestCase(CompactorTestCase):
    pass


@dataclass(frozen=True)
class RebaseThenIncrementalCompactorTestCase(CompactorTestCase):
    """
    A pytest parameterized test case for the `compact_partition` function with rebase and incremental compaction.

    Args:
        * (inherited from CompactorTestCase): see CompactorTestCase docstring for details
        incremental_deltas_arrow_arrays_param: Dict[str, pa.Array]  # argument required for delta creation during compact_partition test setup. Incoming deltas during incremental expressed as a pyarrow array
        incremental_deltas_delta_type: argument required for delta creation during compact_partition test setup.  Enum of the type of incremental delta to apply (APPEND, UPSERT, DELETE)
        rebase_expected_compact_partition_result: expected table after rebase compaction runs
    """

    incremental_deltas_arrow_arrays_param: Dict[str, pa.Array]
    incremental_deltas_delta_type: DeltaType
    rebase_expected_compact_partition_result: pa.Table


def create_tests_cases_for_enabled_compactor_versions(
    test_cases: Dict[str, CompactorTestCase] = None
):
    test_cases = {} if test_cases is None else test_cases
    composite_test_cases = {}
    for compactor_version, compact_partition_func in ENABLED_COMPACT_PARTITIONS_DRIVERS:
        for tc_name, tc_params in test_cases.items():
            # skip creating test case if included in the skip params (e.g. skip_enabled_compact_partition_drivers=[CompactorVersion.V1] will skip creating a compactor version 1 test case)
            if (
                tc_params.skip_enabled_compact_partition_drivers
                and compactor_version
                in tc_params.skip_enabled_compact_partition_drivers
            ):
                continue
            composite_test_cases[f"{tc_name}_{compactor_version}"] = [
                *tc_params,
                compact_partition_func,
            ]

    return composite_test_cases


INCREMENTAL_TEST_CASES: Dict[str, IncrementalCompactionTestCase] = {
    "1-incremental-pkstr-sknone-norcf": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1"],
        input_deltas_arrow_arrays_param=[pa.array([str(i) for i in range(10)])],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "2-incremental-pkstr-skstr-norcf": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
            names=["pk_col_1", "sk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "3-incremental-pkstr-multiskstr-norcf": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
            pa.array(["foo"] * 10),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array(["test"] * 10),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "4-incremental-duplicate-pk": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(5)] + ["6", "6", "6", "6", "6"]),
            pa.array([str(i) for i in range(10)]),
            pa.array(["foo"] * 10),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(5)] + ["6"]),
                pa.array([str(i) for i in range(5)] + ["9"]),
                pa.array(["foo"] * 6),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "5-incremental-decimal-pk-simple": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([i / 10 for i in range(0, 10)]),
            pa.array([str(i) for i in range(10)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "6-incremental-integer-pk-simple": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([i for i in range(0, 10)]),
            pa.array([str(i) for i in range(10)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "7-incremental-timestamp-pk-simple": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array(offer_iso8601_timestamp_list(10, "minutes")),
            pa.array([str(i) for i in range(10)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(offer_iso8601_timestamp_list(10, "minutes")),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "8-incremental-decimal-timestamp-pk-multi": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "pk_col_2", "sk_col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([i / 10 for i in range(0, 20)]),
            pa.array(offer_iso8601_timestamp_list(20, "minutes")),
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 20)]),
                pa.array(offer_iso8601_timestamp_list(20, "minutes")),
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "9-incremental-decimal-pk-multi-dup": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            pa.array(reversed([i for i in range(20)])),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5]),
                pa.array([19, 15, 11, 7, 3]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "10-incremental-decimal-pk-partitionless": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys_param=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values_param=ZERO_VALUED_PARTITION_VALUES_PARAM,
        column_names_param=["pk_col_1", "sk_col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            pa.array([i for i in range(20)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5]),
                pa.array([3, 7, 11, 15, 19]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
}

REBASE_THEN_INCREMENTAL_TEST_CASES = {
    "11-rebase-then-incremental-sanity": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array([i for i in range(0, 10)]),
            pa.array(["foo"] * 10),
            pa.array([i / 10 for i in range(10, 20)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        incremental_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array([i for i in range(20, 30)]),
            pa.array(["foo"] * 10),
            pa.array([i / 10 for i in range(40, 50)]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(20, 30)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(40, 50)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_rebase_w_deltas_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "12-rebase-then-incremental-multiple-pk": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i % 4) for i in range(10)]),
            pa.array([(i % 4) / 10 for i in range(9, -1, -1)]),
            pa.array(offer_iso8601_timestamp_list(10, "minutes")),
            pa.array([i / 10 for i in range(10, 20)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3"]),
                pa.array([0.1, 0, 0.3, 0.2]),
                pa.array(
                    [
                        "2023-05-03T10:00:00Z",
                        "2023-05-03T09:59:00Z",
                        "2023-05-03T09:58:00Z",
                        "2023-05-03T09:57:00Z",
                    ]
                ),
                pa.array([1, 1.1, 1.2, 1.3]),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        ),
        incremental_deltas_arrow_arrays_param=[
            pa.array(["0", "1", "2", "3"]),
            pa.array([0.1, 0, 0.3, 0.2]),
            pa.array(
                [
                    "2023-05-03T10:00:00Z",
                    "2023-05-03T09:59:00Z",
                    "2023-05-03T09:58:00Z",
                    "2023-05-03T09:57:00Z",
                ]
            ),
            pa.array([1, 1.1, 1.2, 1.3]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3"]),
                pa.array([0.1, 0, 0.3, 0.2]),
                pa.array(
                    [
                        "2023-05-03T10:00:00Z",
                        "2023-05-03T09:59:00Z",
                        "2023-05-03T09:58:00Z",
                        "2023-05-03T09:57:00Z",
                    ]
                ),
                pa.array([1, 1.1, 1.2, 1.3]),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_rebase_w_deltas_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "13-rebase-then-incremental-no-sk-no-partition-key": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys_param=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values_param=ZERO_VALUED_PARTITION_VALUES_PARAM,
        column_names_param=["pk_col_1", "col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i % 4) for i in range(12)]),
            pa.array([i / 10 for i in range(10, 22)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3"]),
                pa.array([1.8, 1.9, 2.0, 2.1]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas_arrow_arrays_param=[
            pa.array(["0", "1", "2", "3"]),
            pa.array([18.0, 19.0, 20.0, 21.0]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3"]),
                pa.array([18.0, 19.0, 20.0, 21.0]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_rebase_w_deltas_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "14-rebase-then-incremental-partial-deltas-on-incremental-deltas": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array([i / 10 for i in range(10)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i / 10 for i in range(10)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas_arrow_arrays_param=[
            pa.array(["8", "9"]),
            pa.array([200.0, 100.0]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i / 10 for i in range(8)] + [200.0] + [100.0]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT,
        create_table_strategy=create_src_w_deltas_destination_rebase_w_deltas_strategy,
        skip_enabled_compact_partition_drivers=None,
    ),
    "15-rebase-then-incremental-hash-bucket-GT-records-per-compacted-file": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(12)]),
            pa.array([i for i in range(0, 12)]),
            pa.array(["foo"] * 12),
            pa.array([i / 10 for i in range(10, 22)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(12)]),
                pa.array([i for i in range(0, 12)]),
                pa.array(["foo"] * 12),
                pa.array([i / 10 for i in range(10, 22)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        incremental_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(12)]),
            pa.array([i for i in range(20, 32)]),
            pa.array(["foo"] * 12),
            pa.array([i / 10 for i in range(40, 52)]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(12)]),
                pa.array([i for i in range(20, 32)]),
                pa.array(["foo"] * 12),
                pa.array([i / 10 for i in range(40, 52)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        create_placement_group_param=False,
        records_per_compacted_file_param=10,
        hash_bucket_count_param=DEFAULT_HASH_BUCKET_COUNT + 10,
        create_table_strategy=create_src_w_deltas_destination_rebase_w_deltas_strategy,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
}

INCREMENTAL_TEST_CASES = create_tests_cases_for_enabled_compactor_versions(
    INCREMENTAL_TEST_CASES
)


REBASE_THEN_INCREMENTAL_TEST_CASES = create_tests_cases_for_enabled_compactor_versions(
    REBASE_THEN_INCREMENTAL_TEST_CASES
)
