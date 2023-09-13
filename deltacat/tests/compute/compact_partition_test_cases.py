import pyarrow as pa
import string
from typing import Callable, Dict, List, Optional, Set, Tuple
from deltacat.tests.compute.test_util_common import (
    offer_iso8601_timestamp_list,
    PartitionKey,
    PartitionKeyType,
)
from deltacat.tests.compute.test_util_constant import (
    DEFAULT_MAX_RECORDS_PER_FILE,
    DEFAULT_HASH_BUCKET_COUNT,
)
from dataclasses import dataclass, fields

from deltacat.compute.compactor.compaction_session import (
    compact_partition_from_request as compact_partition_v1,
)

from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor_v2.compaction_session import (
    compact_partition as compact_partition_v2,
)

from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.storage.model.sort_key import SortKey

ZERO_VALUED_SORT_KEY, ZERO_VALUED_PARTITION_VALUES_PARAM = [], []
ZERO_VALUED_PARTITION_KEYS_PARAM = None
ZERO_VALUED_PRIMARY_KEY = {}

EMPTY_UTSV_PATH = "deltacat/tests/utils/data/empty.csv"


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
        skip_enabled_compact_partition_drivers: List[CompactorVersion] - skip whatever enabled_compact_partition_drivers are included in this list
    """

    primary_keys: Set[str]
    sort_keys: List[Optional[SortKey]]
    partition_keys: Optional[List[PartitionKey]]
    partition_values: List[Optional[str]]
    column_names: List[str]
    input_deltas: List[pa.Array]
    input_deltas_delta_type: DeltaType
    expected_terminal_compact_partition_result: pa.Table
    do_create_placement_group: bool
    records_per_compacted_file: int
    hash_bucket_count: int
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

    incremental_deltas: List[pa.Array]
    incremental_deltas_delta_type: DeltaType
    rebase_expected_compact_partition_result: pa.Table


def with_compactor_version_func_test_param(
    test_cases: Dict[str, CompactorTestCase] = None
):
    test_cases = {} if test_cases is None else test_cases
    enriched_test_cases = {}
    for tc_name, tc_params in test_cases.items():
        for (
            compactor_version,
            compact_partition_func,
        ) in ENABLED_COMPACT_PARTITIONS_DRIVERS:
            # skip creating test case if included in the skip params
            # (e.g. skip_enabled_compact_partition_drivers=[CompactorVersion.V1] will skip creating a compactor version 1 test case)
            if (
                tc_params.skip_enabled_compact_partition_drivers
                and compactor_version
                in tc_params.skip_enabled_compact_partition_drivers
            ):
                continue
            enriched_test_cases[f"{tc_name}_{compactor_version}"] = [
                *tc_params,
                compact_partition_func,
            ]

    return enriched_test_cases


INCREMENTAL_TEST_CASES: Dict[str, IncrementalCompactionTestCase] = {
    "1-incremental-pkstr-sknone-norcf": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1"],
        input_deltas=[pa.array([str(i) for i in range(10)])],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "2-incremental-pkstr-skstr-norcf": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1"],
        input_deltas=[
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
            names=["pk_col_1", "sk_col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "3-incremental-pkstr-multiskstr-norcf": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1", "sk_col_2"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "4-incremental-duplicate-pk": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1", "sk_col_2"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "5-incremental-decimal-pk-simple": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "6-incremental-integer-pk-simple": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "7-incremental-timestamp-pk-simple": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "8-incremental-decimal-timestamp-pk-multi": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "pk_col_2", "sk_col_1"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "9-incremental-decimal-pk-multi-dup": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "10-incremental-decimal-pk-partitionless": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values=ZERO_VALUED_PARTITION_VALUES_PARAM,
        column_names=["pk_col_1", "sk_col_1"],
        input_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "11-incremental-empty-csv-delta-case": IncrementalCompactionTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1"],
        input_deltas=[pa.array([str(i) for i in range(10)])],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
}

REBASE_THEN_INCREMENTAL_TEST_CASES = {
    "1-rebase-then-incremental-sanity": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        input_deltas=[
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
        incremental_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "2-rebase-then-incremental-pk-multi": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        input_deltas=[
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
        incremental_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "3-rebase-then-incremental-no-sk-no-partition-key": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values=ZERO_VALUED_PARTITION_VALUES_PARAM,
        column_names=["pk_col_1", "col_1"],
        input_deltas=[
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
        incremental_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "4-rebase-then-incremental-partial-deltas-on-incremental-deltas": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "col_1"],
        input_deltas=[
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
        incremental_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "5-rebase-then-incremental-partial-deltas-on-incremental-deltas-2": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "sk_col_1", "col_1"],
        input_deltas=[
            pa.array([i % 4 for i in range(12)]),
            pa.array([(i / 10 * 10) % 4 for i in range(12)][::-1]),
            pa.array(list(string.ascii_lowercase)[:12]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array([3.0, 2.0, 1.0, 0.0]),
                pa.array(["i", "j", "k", "l"]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        incremental_deltas=[
            pa.array([1, 4]),
            pa.array([4.0, 2.0]),
            pa.array(["a", "b"]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4]),
                pa.array([3.0, 4.0, 1.0, 0.0, 2.0]),
                pa.array(["i", "a", "k", "l", "b"]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "6-rebase-then-incremental-hash-bucket-GT-records-per-compacted-file-v2-only": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("day", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        column_names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        input_deltas=[
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
        incremental_deltas=[
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
        do_create_placement_group=False,
        records_per_compacted_file=10,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT + 10,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "7-rebase-then-incremental-no-pk-compactor-v2-only": RebaseThenIncrementalCompactorTestCase(
        primary_keys=ZERO_VALUED_PRIMARY_KEY,
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["sk_col_1", "col_1"],
        input_deltas=[
            pa.array([1, 2, 3]),
            pa.array([1.0, 2.0, 3.0]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 1, 2, 2, 3, 3]),
                pa.array([1.0, 1.0, 2.0, 2.0, 3.0, 3.0]),
            ],
            names=["sk_col_1", "col_1"],
        ),
        incremental_deltas=[
            pa.array([4, 5, 6]),
            pa.array([10.0, 11.0, 12.0]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 1, 2, 2, 3, 3, 4, 5, 6]),
                pa.array([1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 10.0, 11.0, 12.0]),
            ],
            names=["sk_col_1", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=10,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "8-rebase-then-incremental-delete-type-delta-on-incremental": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        column_names=["pk_col_1", "col_1"],
        input_deltas=[
            pa.array([i for i in range(12)]),
            pa.array([str(i) for i in range(0, 12)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(12)]),
                pa.array([str(i) for i in range(0, 12)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[  # delete last two primary keys
            pa.array([10, 11]),
            pa.array(["", ""]),
        ],
        incremental_deltas_delta_type=DeltaType.DELETE,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(10)]),
                pa.array([str(i) for i in range(0, 10)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=None,
    ),
    "9-rebase-then-incremental-delete-type-delta-on-incremental-multi-pk": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        column_names=["pk_col_1", "pk_col_2", "col_1"],
        input_deltas=[
            pa.array([(i % 4) for i in range(12)]),
            pa.array([float(i % 4) for i in range(12, 0, -1)]),
            pa.array([str(i) for i in range(0, 12)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array([0.0, 3.0, 2.0, 1.0]),
                pa.array(["8", "9", "10", "11"]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        incremental_deltas=[  # delete last two primary keys
            pa.array([2, 3]),
            pa.array([2.0, 1.0]),
            pa.array(["", ""]),
        ],
        incremental_deltas_delta_type=DeltaType.DELETE,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array([0.0, 3.0]),
                pa.array(["8", "9"]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "10-rebase-then-incremental-delete-type-delta-on-incremental-multi-pk-delete-all": RebaseThenIncrementalCompactorTestCase(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        column_names=["pk_col_1", "pk_col_2", "col_1"],
        input_deltas=[
            pa.array([(i % 4) for i in range(12)]),
            pa.array([float(i % 4) for i in range(12, 0, -1)]),
            pa.array([str(i) for i in range(0, 12)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array([0.0, 3.0, 2.0, 1.0]),
                pa.array(["8", "9", "10", "11"]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        incremental_deltas=[  # delete last two primary keys
            pa.array([0, 1, 2, 3]),
            pa.array([0.0, 3.0, 2.0, 1.0]),
            pa.array(["8", "9", "10", "11"]),
        ],
        incremental_deltas_delta_type=DeltaType.DELETE,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([]),
                pa.array([]),
                pa.array([]),
            ],
            schema=pa.schema(
                [
                    ("pk_col_1", pa.int64()),
                    ("pk_col_2", pa.float64()),
                    ("col_1", pa.string()),
                ]
            ),
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
}

INCREMENTAL_TEST_CASES = with_compactor_version_func_test_param(INCREMENTAL_TEST_CASES)


REBASE_THEN_INCREMENTAL_TEST_CASES = with_compactor_version_func_test_param(
    REBASE_THEN_INCREMENTAL_TEST_CASES
)
