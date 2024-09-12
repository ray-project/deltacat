import pyarrow as pa
from typing import Callable, List, Optional, Set, Union
from deltacat.utils.common import ReadKwargsProvider
from deltacat.tests.compute.test_util_common import (
    PartitionKey,
    PartitionKeyType,
)
from deltacat.tests.compute.test_util_constant import (
    DEFAULT_MAX_RECORDS_PER_FILE,
    DEFAULT_HASH_BUCKET_COUNT,
)
from dataclasses import dataclass, fields

from deltacat.exceptions import ValidationError

from deltacat.storage import (
    DeltaType,
    DeleteParameters,
)

from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.storage.model.sort_key import SortKey

from deltacat.tests.compute.compact_partition_test_cases import (
    with_compactor_version_func_test_param,
    ZERO_VALUED_SORT_KEY,
)


@dataclass(frozen=True)
class MultipleRoundsTestCaseParams:
    """
    A pytest parameterized test case for the `compact_partition` function.

    Args:
        primary_keys: Set[str] - argument for the primary_keys parameter in compact_partition. Also needed for table/delta creation
        sort_keys: List[SortKey] - argument for the sort_keys parameter in compact_partition. Also needed for table/delta creation
        partition_keys_param: List[PartitionKey] - argument for the partition_keys parameter. Needed for table/delta creation
        partition_values_param: List[Optional[str]] - argument for the partition_valued parameter. Needed for table/delta creation
        input_deltas: List[pa.Array] - argument required for delta creation during compact_partition test setup. Actual incoming deltas expressed as a PyArrow array (https://arrow.apache.org/docs/python/generated/pyarrow.array.html)
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
        rebase_expected_compact_partition_result: pa.Table - expected table after rebase compaction runs. An output that is asserted on in Rebase unit tests
        num_rounds: int - parameter that specifies the number of rounds of compaction (how many batches of uniform deltas to make). Default is 1 round
    """

    primary_keys: Set[str]
    sort_keys: List[Optional[SortKey]]
    partition_keys: Optional[List[PartitionKey]]
    partition_values: List[Optional[str]]
    input_deltas: Union[List[pa.Array], DeltaType, DeleteParameters]
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
    rebase_expected_compact_partition_result: pa.Table
    num_rounds: int

    # makes MultipleRoundsTestCase iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


MULTIPLE_ROUNDS_TEST_CASES = {
    # 4 input deltas that are identical, 2 rounds requested.
    # Expect to see a table that aggregates 40 records across the 2 rounds
    # (dropDuplicates = False)
    "1-multiple-rounds-sanity": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 10)] * 4),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 10)] * 4),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 10)] * 4),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 10)] * 4),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=2,
    ),
    # 4 input deltas that are unique, 2 rounds requested.
    # Expect to see a table that aggregates 40 unique records across the 2 rounds
    # (dropDuplicates = False)
    "2-multiple-rounds-unique-values": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10, 20)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["bar"] * 10),
                        pa.array([i / 10 for i in range(10, 20)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(20, 30)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(20, 30)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(30, 40)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(30, 40)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 10 + ["bar"] * 10 + ["foo"] * 20),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 10 + ["bar"] * 10 + ["foo"] * 20),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=2,
    ),
    # Testing assert that checks if the num_rounds passed in
    # is less than the len(uniform_deltas).
    "3-num-rounds-greater-than-deltas-count": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10, 20)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(10, 20)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(20, 30)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(20, 30)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(30, 40)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(30, 40)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=ValidationError,
        expected_terminal_exception_message="One of the assertions in DeltaCAT has failed",
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=15,
    ),
    # 4 input deltas that are identical, 2 rounds requested.
    # Expect to see a table that aggregates 40 records across the 2 rounds
    # (dropDuplicates = False), hb_count = 1
    "4-multiple-rounds-hb-count-equals-1": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10, 20)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(10, 20)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(20, 30)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(20, 30)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(30, 40)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(30, 40)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=2,
    ),
    # Testing assert that ensure we are running multiple rounds only when
    # drop_duplicates is False (rebase). Running backfill on multiple rounds
    # is currently not supported.
    "5-multiple-rounds-only-supports-rebase": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10, 20)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(10, 20)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(20, 30)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(20, 30)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(30, 40)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(30, 40)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 40)]),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 40)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=ValidationError,
        expected_terminal_exception_message="One of the assertions in DeltaCAT has failed",
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=2,
    ),
    # 4 input deltas that are identical, 2 rounds requested.
    # Expect to see a table that aggregates 40 records across the 2 rounds
    # (dropDuplicates = False), tests placement group parameter functionality
    # (do_create_placement_group = True)
    "6-multiple-rounds-test-pgm": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(0, 10)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(0, 10)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 10)] * 4),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 10)] * 4),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(0, 10)] * 4),
                pa.array([i for i in range(0, 10)] * 4),
                pa.array(["foo"] * 40),
                pa.array([i / 10 for i in range(0, 10)] * 4),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=2,
    ),
    # 4 input deltas (3 upsert, 1 delete delta), 2 rounds requested
    # Expect to see a table that aggregates 10 records total
    # (12 upserts - 2 deletes = 10 records)
    # (dropDuplicates = False)
    "7-multiple-rounds-delete-deltas": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([10, 11, 12, 13]),
                        pa.array(["a", "b", "c", "d"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([14, 15, 16, 17]),
                        pa.array(["e", "f", "g", "h"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([18, 19, 20, 21]),
                        pa.array(["i", "j", "k", "l"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [pa.array([10, 11]), pa.array(["a", "b"])],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(12, 22)]),
                pa.array(["c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(12, 22)]),
                pa.array(["c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=2,
    ),
    # 6 input deltas (4 upsert, 2 delete deltas), 3 rounds requested
    # Testing multiple delete deltas in between upserts with odd
    # number of rounds requested
    # (dropDuplicates = False)
    "8-multiple-rounds-multiple-delete-deltas": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([1, 2, 3, 4]),
                        pa.array(
                            ["iron man", "captain america", "black widow", "hulk"]
                        ),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([5, 6, 7, 8]),
                        pa.array(["hawkeye", "thor", "star lord", "gamora"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [pa.array([1, 3]), pa.array(["iron man", "black widow"])],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array([8]), pa.array(["gamora"])],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([9, 10, 11, 12]),
                        pa.array(["war machine", "scarlet witch", "vision", "falcon"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([13, 14, 15, 16]),
                        pa.array(["ant man", "wasp", "rocket raccoon", "groot"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([2, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16]),
                pa.array(
                    [
                        "captain america",
                        "hulk",
                        "hawkeye",
                        "thor",
                        "star lord",
                        "war machine",
                        "scarlet witch",
                        "vision",
                        "falcon",
                        "ant man",
                        "wasp",
                        "rocket raccoon",
                        "groot",
                    ]
                ),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([2, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16]),
                pa.array(
                    [
                        "captain america",
                        "hulk",
                        "hawkeye",
                        "thor",
                        "star lord",
                        "war machine",
                        "scarlet witch",
                        "vision",
                        "falcon",
                        "ant man",
                        "wasp",
                        "rocket raccoon",
                        "groot",
                    ]
                ),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=3,
    ),
    # 4 input deltas (3 upsert, 1 delete delta), 2 rounds requested
    # Expect to see a table that aggregates 10 records total
    # (12 upserts - 2 deletes (null PK) = 10 records)
    # (dropDuplicates = False)
    "9-multiple-rounds-delete-deltas-with-null-pk": MultipleRoundsTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([None, 11, 12, 13]),
                        pa.array(["a", "b", "c", "d"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([14, 15, 16, 17]),
                        pa.array(["e", "f", "g", "h"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([18, 19, 20, 21]),
                        pa.array(["i", "j", "k", "l"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [pa.array([None, 11]), pa.array(["a", "b"])],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
        ],
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(12, 22)]),
                pa.array(["c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(12, 22)]),
                pa.array(["c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
        num_rounds=2,
    ),
}

MULTIPLE_ROUNDS_TEST_CASES = with_compactor_version_func_test_param(
    MULTIPLE_ROUNDS_TEST_CASES
)
