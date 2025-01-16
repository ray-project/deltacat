import pyarrow as pa
from typing import Callable, Dict, List, Optional, Set, Tuple, Union
from deltacat.tests.compute.test_util_common import (
    offer_iso8601_timestamp_list,
    PartitionKey,
    PartitionKeyType,
    assert_compaction_audit,
    assert_compaction_audit_no_hash_bucket,
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
from deltacat.storage import DeleteParameters

from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.storage.model.sort_key import SortKey

from deltacat.exceptions import ValidationError

ZERO_VALUED_SORT_KEY, ZERO_VALUED_PARTITION_VALUES_PARAM = [], []
ZERO_VALUED_PARTITION_KEYS_PARAM = None
ZERO_VALUED_PRIMARY_KEY = {}
ZERO_VALUED_PROPERTIES = {}

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
        expected_terminal_exception: BaseException - expected exception during compaction
        expected_terminal_exception_message: Optional[str] - expected exception message if present.
        do_create_placement_group: bool - toggles whether to create a placement group (https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html) or not
        records_per_compacted_file: int - argument for the records_per_compacted_file parameter in compact_partition
        hash_bucket_count_param: int - argument for the hash_bucket_count parameter in compact_partition
        read_kwargs_provider: Optional[ReadKwargsProvider] - argument for read_kwargs_provider parameter in compact_partition. If None then no ReadKwargsProvider is provided to compact_partition_params
        drop_duplicates: bool - argument for drop_duplicates parameter in compact_partition. Only recognized by compactor v2.
        skip_enabled_compact_partition_drivers: List[CompactorVersion] - skip whatever enabled_compact_partition_drivers are included in this list
        assert_compaction_audit: Optional[Callable] - argument that asserts compaction_audit is updated only if compactor_version is v2.
    """

    primary_keys: Set[str]
    sort_keys: List[Optional[SortKey]]
    partition_keys: Optional[List[PartitionKey]]
    partition_values: List[Optional[str]]
    input_deltas: Union[List[pa.Array], pa.Table]
    input_deltas_delta_type: DeltaType
    expected_terminal_compact_partition_result: pa.Table
    expected_terminal_exception: BaseException
    expected_terminal_exception_message: str
    do_create_placement_group: bool
    records_per_compacted_file: int
    hash_bucket_count: int
    read_kwargs_provider: Optional[ReadKwargsProvider]
    drop_duplicates: bool
    skip_enabled_compact_partition_drivers: List[CompactorVersion]
    assert_compaction_audit: Optional[Callable]

    # makes CompactorTestCase iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@dataclass(frozen=True)
class IncrementalCompactionTestCaseParams(BaseCompactorTestCase):
    """
    Args:
        is_inplace: bool - argument to indicate whether to try compacting an in-place compacted table (the source table is the destination table). Also needed to control whether the destination table is created
        add_late_deltas: List[Tuple[pa.Table, DeltaType]] - argument to indicate whether to add deltas to the source_partition after we've triggered compaction
    """

    is_inplace: bool
    add_late_deltas: Optional[
        List[Tuple[pa.Table, DeltaType, Optional[DeleteParameters]]]
    ]


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
                compactor_version,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit_no_hash_bucket,
    ),
    "13-incremental-pkstr-skexists-isinplacecompacted": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=True,
        add_late_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(20)]),
                        pa.array([i for i in range(20)]),
                    ],
                    names=["pk_col_1", "sk_col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
    "14-incremental-pkstr-skexists-unhappy-hash-bucket-count-not-present": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        expected_terminal_exception=ValidationError,
        expected_terminal_exception_message="One of the assertions in DeltaCAT has failed",
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=None,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
    "15-incremental-empty-input-with-single-hash-bucket": IncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[SortKey.of(key_name="sk_col_1")],
        partition_keys=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values=ZERO_VALUED_PARTITION_VALUES_PARAM,
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([]),
                pa.array([]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([]),
                pa.array([]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        is_inplace=False,
        add_late_deltas=None,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit_no_hash_bucket,
    ),
}

INCREMENTAL_TEST_CASES = with_compactor_version_func_test_param(INCREMENTAL_TEST_CASES)
