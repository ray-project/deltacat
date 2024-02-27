import pyarrow as pa
import string
from typing import Callable, Dict, List, Optional, Set, Tuple, Union
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
from deltacat.utils.common import ReadKwargsProvider

from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor_v2.compaction_session import (
    compact_partition as compact_partition_v2,
)
from deltacat.types.media import ContentType

from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.storage.model.sort_key import SortKey

from deltacat.utils.pyarrow import (
    ReadKwargsProviderPyArrowSchemaOverride,
    content_type_to_reader_kwargs,
    pyarrow_read_csv,
)

ZERO_VALUED_SORT_KEY, ZERO_VALUED_PARTITION_VALUES_PARAM = [], []
ZERO_VALUED_PARTITION_KEYS_PARAM = None
ZERO_VALUED_PRIMARY_KEY = {}

EMPTY_UTSV_PATH = "deltacat/tests/utils/data/empty.csv"

ENABLED_COMPACT_PARTITIONS_DRIVERS: List[Tuple[CompactorVersion, Callable]] = [
    (CompactorVersion.V1, compact_partition_v1),
    (CompactorVersion.V2, compact_partition_v2),
]


@dataclass(frozen=True)
class BaseCompactorTestCase:
    """
    A pytest parameterized test case for the `compact_partition` function.

    Args:
        primary_keys: Set[str] - argument for the primary_keys parameter in compact_partition. Also needed for table/delta creation
        sort_keys: List[SortKey] - argument for the sort_keys parameter in compact_partition. Also needed for table/delta creation
        partition_keys_param: List[PartitionKey] - argument for the partition_keys parameter. Needed for table/delta creation
        partition_values_param: List[Optional[str]] - argument for the partition_valued parameter. Needed for table/delta creation
        input_deltas: List[pa.Array] - argument required for delta creation during compact_partition test setup. Actual incoming deltas expressed as a PyArrow array (https://arrow.apache.org/docs/python/generated/pyarrow.array.html)
        input_deltas_delta_type: DeltaType - enumerated argument required for delta creation during compact_partition test setup. Available values are (DeltaType.APPEND, DeltaType.UPSERT, DeltaType.DELETE). DeltaType.APPEND is not supported by compactor v1 or v2
        expected_terminal_compact_partition_result: pa.Table - expected PyArrow table after compaction (i.e,. the state of the table after applying all row UPDATES/DELETES/INSERTS)
        do_create_placement_group: bool - toggles whether to create a placement group (https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html) or not
        records_per_compacted_file: int - argument for the records_per_compacted_file parameter in compact_partition
        hash_bucket_count_param: int - argument for the hash_bucket_count parameter in compact_partition. Needs to be > 1
        read_kwargs_provider: Optional[ReadKwargsProvider] - argument for read_kwargs_provider parameter in compact_partition. If None then no ReadKwargsProvider is provided to compact_partition_params
        drop_duplicates: bool - argument for drop_duplicates parameter in compact_partition. Only recognized by compactor v2.
        skip_enabled_compact_partition_drivers: List[CompactorVersion] - skip whatever enabled_compact_partition_drivers are included in this list
    """

    primary_keys: Set[str]
    sort_keys: List[Optional[SortKey]]
    partition_keys: Optional[List[PartitionKey]]
    partition_values: List[Optional[str]]
    input_deltas: Union[List[pa.Array], pa.Table]
    input_deltas_delta_type: DeltaType
    expected_terminal_compact_partition_result: pa.Table
    do_create_placement_group: bool
    records_per_compacted_file: int
    hash_bucket_count: int
    read_kwargs_provider: Optional[ReadKwargsProvider]
    drop_duplicates: bool
    skip_enabled_compact_partition_drivers: List[CompactorVersion]

    # makes CompactorTestCase iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@dataclass(frozen=True)
class IncrementalCompactionTestCaseParams(BaseCompactorTestCase):
    pass


@dataclass(frozen=True)
class RebaseThenIncrementalCompactionTestCaseParams(BaseCompactorTestCase):
    """
    A pytest parameterized test case for the `compact_partition` function with rebase and incremental compaction.

    Args:
        * (inherited from CompactorTestCase): see CompactorTestCase docstring for details
        incremental_deltas: pa.Table - argument required for delta creation during the incremental phase of compact_partition test setup. Incoming deltas during incremental expressed as a pyarrow array
        incremental_deltas_delta_type: DeltaType -  argument required for delta creation during the incremental phase of compact_partition test setup. Available values are (DeltaType.APPEND, DeltaType.UPSERT, DeltaType.DELETE). DeltaType.APPEND is not supported by compactor v1 or v2
        rebase_expected_compact_partition_result: pa.Table - expected table after rebase compaction runs. An output that is asserted on in Rebase then Incremental unit tests
    """

    incremental_deltas: Optional[pa.Table]
    incremental_deltas_delta_type: DeltaType
    rebase_expected_compact_partition_result: pa.Table


@dataclass(frozen=True)
class NoRCFOutputCompactionTestCaseParams(BaseCompactorTestCase):
    pass


def with_compactor_version_func_test_param(
    test_cases: Dict[str, BaseCompactorTestCase] = None
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


INCREMENTAL_TEST_CASES: Dict[str, IncrementalCompactionTestCaseParams] = {
    "1-incremental-pkstr-sknone-norcf": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "2-incremental-pkstr-skstr-norcf": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array(["test"] * 10),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
            names=["pk_col_1", "sk_col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "3-incremental-pkstr-multiskstr-norcf": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        # column_names=["pk_col_1", "sk_col_1", "sk_col_2"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array(["test"] * 10),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "4-incremental-duplicate-pk": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(5)] + ["6", "6", "6", "6", "6"]),
                pa.array([str(i) for i in range(10)]),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "5-incremental-decimal-pk-simple": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "6-incremental-integer-pk-simple": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([i for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "7-incremental-timestamp-pk-simple": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array(offer_iso8601_timestamp_list(10, "minutes")),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "8-incremental-decimal-timestamp-pk-multi": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 20)]),
                pa.array(offer_iso8601_timestamp_list(20, "minutes")),
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "9-incremental-decimal-pk-multi-dup": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
                pa.array(reversed([i for i in range(20)])),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "10-incremental-decimal-pk-partitionless": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values=ZERO_VALUED_PARTITION_VALUES_PARAM,
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
                pa.array([i for i in range(20)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "11-incremental-decimal-hash-bucket-single": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values=ZERO_VALUED_PARTITION_VALUES_PARAM,
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
                pa.array([i for i in range(20)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "12-incremental-decimal-single-hash-bucket": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values=ZERO_VALUED_PARTITION_VALUES_PARAM,
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
                pa.array([i for i in range(20)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
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
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
}

REBASE_THEN_INCREMENTAL_TEST_CASES = {
    "1-rebase-then-incremental-sanity": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(20, 30)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(40, 50)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "2-rebase-then-incremental-pk-multi": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i % 4) for i in range(10)]),
                pa.array([(i % 4) / 10 for i in range(9, -1, -1)]),
                pa.array(offer_iso8601_timestamp_list(10, "minutes")),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        ),
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
        incremental_deltas=pa.Table.from_arrays(
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "3-rebase-then-incremental-no-sk-no-partition-key": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values=ZERO_VALUED_PARTITION_VALUES_PARAM,
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i % 4) for i in range(12)]),
                pa.array([i / 10 for i in range(10, 22)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3"]),
                pa.array([1.8, 1.9, 2.0, 2.1]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3"]),
                pa.array([18.0, 19.0, 20.0, 21.0]),
            ],
            names=["pk_col_1", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "4-rebase-then-incremental-partial-deltas-on-incremental-deltas": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i / 10 for i in range(10)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i / 10 for i in range(10)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array(["8", "9"]),
                pa.array([200.0, 100.0]),
            ],
            names=["pk_col_1", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "5-rebase-then-incremental-partial-deltas-on-incremental-deltas-2": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([i % 4 for i in range(12)]),
                pa.array([(i / 10 * 10) % 4 for i in range(12)][::-1]),
                pa.array(list(string.ascii_lowercase)[:12]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array([3.0, 2.0, 1.0, 0.0]),
                pa.array(["i", "j", "k", "l"]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 4]),
                pa.array([4.0, 2.0]),
                pa.array(["a", "b"]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "6-rebase-then-incremental-hash-bucket-GT-records-per-compacted-file-v2-only": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("day", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(12)]),
                pa.array([i for i in range(0, 12)]),
                pa.array(["foo"] * 12),
                pa.array([i / 10 for i in range(10, 22)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(12)]),
                pa.array([i for i in range(20, 32)]),
                pa.array(["foo"] * 12),
                pa.array([i / 10 for i in range(40, 52)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "7-rebase-then-incremental-no-pk-compactor-v2-only": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys=ZERO_VALUED_PRIMARY_KEY,
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, 3]),
                pa.array([1.0, 2.0, 3.0]),
            ],
            names=["sk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 1, 2, 2, 3, 3]),
                pa.array([1.0, 1.0, 2.0, 2.0, 3.0, 3.0]),
            ],
            names=["sk_col_1", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array([4, 5, 6]),
                pa.array([10.0, 11.0, 12.0]),
            ],
            names=["sk_col_1", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "8-rebase-then-incremental-delete-type-delta-on-incremental": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([i for i in range(12)]),
                pa.array([str(i) for i in range(0, 12)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(12)]),
                pa.array([str(i) for i in range(0, 12)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [  # delete last two primary keys
                pa.array([10, 11]),
                pa.array(["", ""]),
            ],
            names=["pk_col_1", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "9-rebase-then-incremental-delete-type-delta-on-incremental-multi-pk": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(12)]),
                pa.array([float(i % 4) for i in range(12, 0, -1)]),
                pa.array([str(i) for i in range(0, 12)]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array([0.0, 3.0, 2.0, 1.0]),
                pa.array(["8", "9", "10", "11"]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [  # delete last two primary keys
                pa.array([2, 3]),
                pa.array([2.0, 1.0]),
                pa.array(["", ""]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "10-rebase-then-incremental-delete-type-delta-on-incremental-multi-pk-delete-all": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(12)]),
                pa.array([float(i % 4) for i in range(12, 0, -1)]),
                pa.array([str(i) for i in range(0, 12)]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array([0.0, 3.0, 2.0, 1.0]),
                pa.array(["8", "9", "10", "11"]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [  # delete last two primary keys
                pa.array([0, 1, 2, 3]),
                pa.array([0.0, 3.0, 2.0, 1.0]),
                pa.array(["8", "9", "10", "11"]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
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
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "11-rebase-then-incremental-empty-csv-delta-case": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=pyarrow_read_csv(
            EMPTY_UTSV_PATH,
            **ReadKwargsProviderPyArrowSchemaOverride(
                schema=pa.schema(
                    [
                        ("pk_col_1", pa.string()),
                        ("col_1", pa.float64()),
                    ]
                )
            )(
                ContentType.UNESCAPED_TSV.value,
                content_type_to_reader_kwargs(ContentType.UNESCAPED_TSV.value),
            ),
        ),
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]),
                pa.array([1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "12-rebase-then-incremental-single-hash-bucket": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(20, 30)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(40, 50)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "13-rebase-then-incremental-drop-duplicates-false-on-incremental-v2-only": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(8)]),
                pa.array([(i % 2) for i in range(8)]),
                pa.array([i / 10 for i in range(10, 18)]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array([0, 1, 0, 1]),
                pa.array([1.4, 1.5, 1.6, 1.7]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 1]),
                pa.array([0, 1, 0, 1, 0]),
                pa.array([i / 10 for i in range(20, 25)]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 0, 1, 1, 1, 2, 2, 3, 3]),
                pa.array([0, 0, 1, 0, 1, 0, 0, 1, 1]),
                pa.array([1.4, 2, 1.5, 2.4, 2.1, 1.6, 2.2, 1.7, 2.3]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
    "14-rebase-then-empty-incremental-delta": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        incremental_deltas=None,
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=3,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "15-rebase-then-incremental-hash-bucket-single": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        incremental_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(20, 30)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(40, 50)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
    "16-rebase-then-empty-incremental-delta-hash-bucket-single": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
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
        incremental_deltas=None,
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
    ),
}

INCREMENTAL_TEST_CASES = with_compactor_version_func_test_param(INCREMENTAL_TEST_CASES)


REBASE_THEN_INCREMENTAL_TEST_CASES = with_compactor_version_func_test_param(
    REBASE_THEN_INCREMENTAL_TEST_CASES
)
