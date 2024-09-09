import datetime as dt
import pyarrow as pa
from deltacat.constants import DW_LAST_UPDATED_COLUMN_NAME
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
        expected_terminal_compact_partition_result=pa.Table.from_arrays([]),
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
    "2-rebase-sort": RebaseCompactionTestCaseParams(
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
                pa.array(dt.datetime(year, 1, 1) for year in range(2000, 2010)),
            ],
            names=[
                "pk_col_1",
                "sk_col_1",
                "sk_col_2",
                "col_1",
                DW_LAST_UPDATED_COLUMN_NAME,
            ],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        # dw_last_update is in ascending order in the input table.
        # Expect descending sort on dw_last_updated for each hash bucket.
        # Since there is only one hash bucket, the order of input rows should be reversed.
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in reversed(range(10))]),
                pa.array([i for i in reversed(range(0, 10))]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in reversed(range(10, 20))]),
                pa.array(
                    dt.datetime(year, 1, 1) for year in reversed(range(2000, 2010))
                ),
            ],
            names=[
                "pk_col_1",
                "sk_col_1",
                "sk_col_2",
                "col_1",
                DW_LAST_UPDATED_COLUMN_NAME,
            ],
        ),
        expected_terminal_compact_partition_result=pa.Table.from_arrays([]),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
    ),
}

REBASE_TEST_CASES = with_compactor_version_func_test_param(REBASE_TEST_CASES)
