import pyarrow as pa
from deltacat.tests.compute.test_util_common import (
    PartitionKey,
    PartitionKeyType,
    assert_compaction_audit,
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
class MultipleRoundsCompactionTestCaseParams(BaseCompactorTestCase):
    """
    A pytest parameterized test case for the `compact_partition` function with multiple rounds testing.

    Args:
        * (inherited from CompactorTestCase): see CompactorTestCase docstring for details
        incremental_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]] - argument required for delta creation during the incremental phase of compact_partition test setup. Incoming deltas during incremental expressed as a pyarrow array
        rebase_expected_compact_partition_result: pa.Table - expected table after rebase compaction runs. An output that is asserted on in Rebase then Incremental unit tests
    """

    rebase_expected_compact_partition_result: pa.Table


MULTIPLE_ROUNDS_TEST_CASES = {
    "1-multiple-rounds-sanity": MultipleRoundsCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=[
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
            pa.Table.from_arrays(
                [
                    pa.array([str(i) for i in range(10)]),
                    pa.array([i for i in range(0, 10)]),
                    pa.array(["foo"] * 10),
                    pa.array([i / 10 for i in range(10)]),
                ],
                names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
            ),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)] * 28),
                pa.array([i for i in range(0, 10)] * 28),
                pa.array(["foo"] * 280),
                pa.array([i / 10 for i in range(10)] * 28),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10)]),
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
        assert_compaction_audit=assert_compaction_audit,
    ),
}

MULTIPLE_ROUNDS_TEST_CASES = with_compactor_version_func_test_param(
    {
        **MULTIPLE_ROUNDS_TEST_CASES,
    },
)
