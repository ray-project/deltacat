import pyarrow as pa
from deltacat.tests.compute.test_util_common import (
    PartitionKey,
    PartitionKeyType,
)
from deltacat.tests.compute.test_util_constant import (
    DEFAULT_MAX_RECORDS_PER_FILE,
    DEFAULT_HASH_BUCKET_COUNT,
)
from dataclasses import dataclass


from deltacat.storage import (
    DeltaType,
)

from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.storage.model.sort_key import SortKey

from deltacat.tests.compute.compact_partition_test_cases import (
    BaseCompactorTestCase,
    with_compactor_version_func_test_param,
)


@dataclass(frozen=True)
class RebaseCompactionTestCaseParams(BaseCompactorTestCase):
    """
    A pytest parameterized test case for the `compact_partition` function with rebase compaction.

    Args:
        * (inherited from CompactorTestCase): see CompactorTestCase docstring for details
        rebase_expected_compact_partition_result: pa.Table - expected table after rebase compaction runs. An output that is asserted on in Rebase unit tests
    """

    rebase_expected_compact_partition_result: pa.Table


REBASE_TEST_CASES = {
    "1-rebase-sanity": RebaseCompactionTestCaseParams(
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
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(20, 30)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(40, 50)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
    "2-rebase-with-null-pk": RebaseCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1]),
                pa.array([1, 2, 3, 4, 5, 6]),
                pa.array(["foo"] * 6),
                pa.array([5, 6, 7, 8, 9, 10]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([None, 1, 2]),
                pa.array([5, 6, 4]),
                pa.array(["foo"] * 3),
                pa.array([9, 10, 8]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([None, 1, 2]),
                pa.array([5, 6, 4]),
                pa.array(["foo"] * 3),
                pa.array([7, 10, 8]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
    "3-rebase-with-null-two-pk": RebaseCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5]),
                pa.array([1, None, 3, None, None, 1, 5]),
                pa.array(["foo"] * 7),
                pa.array([5, 6, 7, 8, 9, 10, 11]),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 5, None]),
                pa.array([1, None, 3, 5, None]),
                pa.array(["foo"] * 5),
                pa.array([10, 8, 7, 11, 9]),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 5, None]),
                pa.array([1, None, 3, 5, None]),
                pa.array(["foo"] * 5),
                pa.array([10, 8, 7, 11, 9]),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
    "4-rebase-with-null-multiple-pk-different-types": RebaseCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2", "pk_col_3"},
        sort_keys=[],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 5, None, None]),
                pa.array([1, None, 3, 5, None, None]),
                pa.array(["a", "b", "c", "g", "e", None]),
                pa.array([10, 8, 7, 11, 12, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 5, None, None]),
                pa.array([1, None, 3, 5, None, None]),
                pa.array(["a", "b", "c", "g", "e", None]),
                pa.array([10, 8, 7, 11, 12, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
    "5-rebase-with-null-multiple-pk-one-hash-bucket": RebaseCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2", "pk_col_3"},
        sort_keys=[],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 5, None, None]),
                pa.array([1, None, 3, 5, None, None]),
                pa.array(["a", "b", "c", "g", "e", None]),
                pa.array([10, 8, 7, 11, 12, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 5, None, None]),
                pa.array([1, None, 3, 5, None, None]),
                pa.array(["a", "b", "c", "g", "e", None]),
                pa.array([10, 8, 7, 11, 12, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
    "6-rebase-with-null-multiple-pk-drop-duplicates-false": RebaseCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2", "pk_col_3"},
        sort_keys=[],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
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
    ),
    "7-rebase-drop-duplicates-false": RebaseCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, 2, 3, 3, 1]),
                pa.array([1, 2, 3, 4, 5, 6]),
                pa.array(["a", "b", "c", "b", "e", "a"]),
                pa.array([5, 6, 7, 8, 9, 10]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1", "col_2"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, 2, 3, 3, 1]),
                pa.array([1, 2, 3, 4, 5, 6]),
                pa.array(["a", "b", "c", "b", "e", "a"]),
                pa.array([5, 6, 7, 8, 9, 10]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1", "col_2"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, 2, 3, 3, 1]),
                pa.array([1, 2, 3, 4, 5, 6]),
                pa.array(["a", "b", "c", "b", "e", "a"]),
                pa.array([5, 6, 7, 8, 9, 10]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1", "col_2"],
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
    ),
    "8-rebase-with-with-null-pk-duplicates-false-hash-bucket-1": RebaseCompactionTestCaseParams(
        primary_keys={"pk_col_1", "pk_col_2", "pk_col_3"},
        sort_keys=[],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 2, None, 2, None, 1, 5, None, None, None]),
                pa.array([1, None, 3, None, None, 1, 5, None, None, None]),
                pa.array(["a", "b", "c", "b", "e", "a", "g", "e", None, None]),
                pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14]),
            ],
            names=["pk_col_1", "pk_col_2", "pk_col_3", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=1,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=False,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=None,
    ),
}

REBASE_TEST_CASES = with_compactor_version_func_test_param(REBASE_TEST_CASES)
