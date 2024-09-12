import pyarrow as pa
from typing import List, Optional, Tuple
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
import string
from dataclasses import dataclass


from deltacat.storage import (
    DeltaType,
)
from deltacat.types.media import ContentType

from deltacat.compute.compactor.model.compactor_version import CompactorVersion

from deltacat.storage.model.sort_key import SortKey

from deltacat.utils.pyarrow import (
    ReadKwargsProviderPyArrowSchemaOverride,
    content_type_to_reader_kwargs,
    pyarrow_read_csv,
)
from deltacat.tests.compute.compact_partition_test_cases import (
    BaseCompactorTestCase,
    with_compactor_version_func_test_param,
    ZERO_VALUED_SORT_KEY,
    ZERO_VALUED_PARTITION_VALUES_PARAM,
    ZERO_VALUED_PARTITION_KEYS_PARAM,
    ZERO_VALUED_PRIMARY_KEY,
    EMPTY_UTSV_PATH,
)
from deltacat.storage import DeleteParameters
from deltacat.exceptions import ValidationError


@dataclass(frozen=True)
class RebaseThenIncrementalCompactionTestCaseParams(BaseCompactorTestCase):
    """
    A pytest parameterized test case for the `compact_partition` function with rebase and incremental compaction.

    Args:
        * (inherited from CompactorTestCase): see CompactorTestCase docstring for details
        incremental_deltas: List[Tuple[pa.Table, DeltaType, Optional[Dict[str, str]]]] - argument required for delta creation during the incremental phase of compact_partition test setup. Incoming deltas during incremental expressed as a pyarrow array
        rebase_expected_compact_partition_result: pa.Table - expected table after rebase compaction runs. An output that is asserted on in Rebase then Incremental unit tests
    """

    incremental_deltas: List[Tuple[pa.Table, DeltaType, Optional[DeleteParameters]]]
    rebase_expected_compact_partition_result: pa.Table


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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
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
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
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
                DeltaType.UPSERT,
                {},
            )
        ],
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["0", "1", "2", "3"]),
                        pa.array([18.0, 19.0, 20.0, 21.0]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3"]),
                pa.array([18.0, 19.0, 20.0, 21.0]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["8", "9"]),
                        pa.array([200.0, 100.0]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i / 10 for i in range(8)] + [200.0] + [100.0]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([1, 4]),
                        pa.array([4.0, 2.0]),
                        pa.array(["a", "b"]),
                    ],
                    names=["pk_col_1", "sk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4]),
                pa.array([3.0, 4.0, 1.0, 0.0, 2.0]),
                pa.array(["i", "a", "k", "l", "b"]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(12)]),
                        pa.array([i for i in range(20, 32)]),
                        pa.array(["foo"] * 12),
                        pa.array([i / 10 for i in range(40, 52)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(12)]),
                pa.array([i for i in range(20, 32)]),
                pa.array(["foo"] * 12),
                pa.array([i / 10 for i in range(40, 52)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=10,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT + 10,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([4, 5, 6]),
                        pa.array([10.0, 11.0, 12.0]),
                    ],
                    names=["sk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 1, 2, 2, 3, 3, 4, 5, 6]),
                pa.array([1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 10.0, 11.0, 12.0]),
            ],
            names=["sk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=10,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "8-rebase-then-incremental-empty-csv-delta-case": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pyarrow_read_csv(
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
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]),
                pa.array([1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=None,
    ),
    "9-rebase-then-incremental-single-hash-bucket": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
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
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=None,
    ),
    "10-rebase-then-incremental-drop-duplicates-false-on-incremental-v2-only": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0, 1, 2, 3, 1]),
                        pa.array([0, 1, 0, 1, 0]),
                        pa.array([i / 10 for i in range(20, 25)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 0, 1, 1, 1, 2, 2, 3, 3]),
                pa.array([0, 0, 1, 0, 1, 0, 0, 1, 1]),
                pa.array([1.4, 2, 1.5, 2.4, 2.1, 1.6, 2.2, 1.7, 2.3]),
            ],
            names=["pk_col_1", "sk_col_1", "col_1"],
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
    "11-rebase-then-empty-incremental-delta": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[(None, DeltaType.UPSERT, None)],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=3,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit_no_hash_bucket,
    ),
    "12-rebase-then-incremental-hash-bucket-single": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
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
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=None,
    ),
    "13-rebase-then-empty-incremental-delta-hash-bucket-single": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[(None, DeltaType.UPSERT, None)],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=1,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=None,
    ),
    "14-rebase-then-incremental-with-null-pk": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=[
            SortKey.of(key_name="sk_col_1"),
            SortKey.of(key_name="sk_col_2"),
        ],
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(9)] + [None]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(9)] + [None]),
                pa.array([i for i in range(0, 10)]),
                pa.array(["foo"] * 10),
                pa.array([i / 10 for i in range(10, 20)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(9)] + [None]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(9)] + [None]),
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
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit,
    ),
}

REBASE_THEN_INCREMENTAL_DELETE_DELTA_TYPE_TEST_CASES = {
    "14-rebase-then-incremental-delete-type-delta-on-incremental": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
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
                    [pa.array([10, 11]), pa.array(["a", "b"])],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array(["c"])],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array(["c"])],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array(["c"])],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array([10, 11]), pa.array(["a", "b"])],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array(["c"])],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(10)] + [13]),
                pa.array([str(i) for i in range(0, 10)] + ["d"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "15-rebase-then-incremental-delete-type-delta-on-incremental-multi-pk": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [  # delete last two primary keys
                        pa.array(["10", "11"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array([0.0, 3.0]),
                pa.array(["8", "9"]),
            ],
            names=["pk_col_1", "pk_col_2", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit_no_hash_bucket,
    ),
    "16-rebase-then-incremental-delete-type-delta-on-incremental-multi-pk-delete-all": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["8", "9", "10", "11"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            )
        ],
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
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit_no_hash_bucket,
    ),
    "17-rebase-then-incremental-delete-type-delta-delete-entire-base-table": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(1000)] + [4, 5]),
                pa.array([str(i) for i in range(0, 1000)] + ["fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4, 5]),
                pa.array(["996", "997", "998", "999", "fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["996", "997", "998", "999", "fiz", "buz"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            )
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([]),
                pa.array([]),
            ],
            schema=pa.schema(
                [
                    ("pk_col_1", pa.int64()),
                    ("col_1", pa.string()),
                ]
            ),
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit_no_hash_bucket,
    ),
    "18-rebase-then-incremental-delete-type-delta-keep-base-table-drop-all-incremental": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(1000)] + [4, 5]),
                pa.array([str(i) for i in range(0, 1000)] + ["fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4, 5]),
                pa.array(["996", "997", "998", "999", "fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0, 1, 2]),
                        pa.array(["0", "1", "2"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([2, 3]),
                        pa.array(["abc", "def"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([4]),
                        pa.array(["ghi"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["0", "1", "2", "abc", "def", "ghi"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([5]),
                pa.array(["buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "19-rebase-then-incremental-delete-type-delta-drop-only-from-base-table-keep-all-incremental": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(1000)] + [4, 5]),
                pa.array([str(i) for i in range(0, 1000)] + ["fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4, 5]),
                pa.array(["996", "997", "998", "999", "fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0, 1, 2, 3, 4, 5]),
                        pa.array(["0", "1", "2", "3", "4", "5"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                        pa.array(["foo"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["996", "997", "998", "999", "fiz", "buz"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4, 5]),
                pa.array(["foo", "1", "2", "3", "4", "5"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "20-rebase-then-incremental-delete-type-delta-drop-all-base-table-drop-all-incremental": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(1000)] + [4, 5]),
                pa.array([str(i) for i in range(0, 1000)] + ["fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4, 5]),
                pa.array(["996", "997", "998", "999", "fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([6]),
                        pa.array(["foo"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["996", "997", "998", "999", "fiz", "buz", "foo"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([]),
                pa.array([]),
            ],
            schema=pa.schema(
                [
                    ("pk_col_1", pa.int64()),
                    ("col_1", pa.string()),
                ]
            ),
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "21-rebase-then-incremental-delete-type-delta-UDDUUDD": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([(i % 4) for i in range(1000)] + [4, 5]),
                pa.array([str(i) for i in range(0, 1000)] + ["fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4, 5]),
                pa.array(["996", "997", "998", "999", "fiz", "buz"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0, 1, 2, 3, 4, 5]),
                        pa.array(["0", "1", "2", "3", "4", "5"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["DOESNOTEXIST"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([1]),
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([2, 3, 6, 7]),
                        pa.array(["boo", "bar", "fiz", "aaa"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["fiz", "bar", "boo"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 4, 5, 7]),
                pa.array(["0", "4", "5", "aaa"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "22-rebase-then-incremental-delete-type-delta-UD-affects-compacted-and-incremental": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([]),
                pa.array([]),
            ],
            schema=pa.schema(
                [
                    ("pk_col_1", pa.int64()),
                    ("col_1", pa.string()),
                ]
            ),
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "23-rebase-then-incremental-delete-type-delta-UDU-upsert-again": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0]),
                pa.array(["1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "24-rebase-then-incremental-delete-type-no-delete-column-has-delete-deltas-expected-exception": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                None,
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([]),
                pa.array([]),
            ],
            schema=pa.schema(
                [
                    ("pk_col_1", pa.int64()),
                    ("col_1", pa.string()),
                ]
            ),
        ),
        expected_terminal_exception=ValidationError,
        expected_terminal_exception_message="One of the assertions in DeltaCAT has failed",
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "25-rebase-then-incremental-delete-type-delta-has-delete-column-no-delete-records": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2]),
                pa.array(["0", "1", "2"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2]),
                pa.array(["0", "1", "2"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([]),
                        pa.array([]),
                    ],
                    schema=pa.schema(
                        [
                            ("pk_col_1", pa.int64()),
                            ("col_1", pa.string()),
                        ]
                    ),
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2]),
                pa.array(["1", "1", "2"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "26-rebase-then-incremental-delete-type-delta-UDU-duplicate-delete-records": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array(["0", "1", "2", "3"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array(["0", "1", "2", "3"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                        pa.array(["1"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["1", "1", "1", "2", "2", "2"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([3]),
                pa.array(["3"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "27-rebase-then-incremental-delete-type-delta-DDU-deletes-then-upserts": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.TIMESTAMP)],
        partition_values=["2022-01-01T00:00:00.000Z"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4]),
                pa.array(["0", "1", "2", "3", "4"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3, 4]),
                pa.array(["0", "1", "2", "3", "4"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array(["1", "1", "2"]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0]),
                    ],
                    names=["pk_col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([0, 3]),
                        pa.array(["a", "b"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([0, 3, 4]),
                pa.array(["a", "b", "4"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=True,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "28-rebase-then-incremental-delete-type-delta-hash-bucket-single": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([str(i) for i in range(10)]),
                        pa.array([i for i in range(20, 30)]),
                        pa.array(["foo"] * 10),
                        pa.array([i / 10 for i in range(40, 50)]),
                    ],
                    names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([i / 10 for i in range(0, 3)]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([i / 10 for i in range(43, 45)]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([i for i in range(20, 25)]),
                    ],
                    names=["sk_col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["sk_col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(5, 10)]),
                pa.array([i for i in range(25, 30)]),
                pa.array(["foo"] * 5),
                pa.array([i / 10 for i in range(45, 50)]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
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
    "29-rebase-then-incremental-delete-type-delta-no-pk-compactor": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([3.0]),
                    ],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([4, 5, 6]),
                        pa.array([10.0, 11.0, 12.0]),
                    ],
                    names=["sk_col_1", "col_1"],
                ),
                DeltaType.UPSERT,
                None,
            ),
            (
                pa.Table.from_arrays(
                    [
                        pa.array([6]),
                    ],
                    names=["sk_col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["sk_col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([1, 1, 2, 2, 4, 5]),
                pa.array([1.0, 1.0, 2.0, 2.0, 10.0, 11.0]),
            ],
            names=["sk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=10,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
    "30-rebase-then-incremental-delete-type-delta-on-incremental-compactor-v1-v2": RebaseThenIncrementalCompactionTestCaseParams(
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
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([10, 11]),
                        pa.array(["10", "11"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(10)]),
                pa.array([str(i) for i in range(0, 10)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=None,
        assert_compaction_audit=assert_compaction_audit_no_hash_bucket,
    ),
    "31-rebase-then-incremental-delete-delta-on-incremental-null-pk-delete-null": RebaseThenIncrementalCompactionTestCaseParams(
        primary_keys={"pk_col_1"},
        sort_keys=ZERO_VALUED_SORT_KEY,
        partition_keys=[PartitionKey.of("region_id", PartitionKeyType.INT)],
        partition_values=["1"],
        input_deltas=pa.Table.from_arrays(
            [
                pa.array([i for i in range(11)] + [None]),
                pa.array([str(i) for i in range(0, 12)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(11)] + [None]),
                pa.array([str(i) for i in range(0, 12)]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        incremental_deltas=[
            (
                pa.Table.from_arrays(
                    [
                        pa.array([10, 11, None, 13]),
                        pa.array(["a", "b", "c", "d"]),
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
            (
                pa.Table.from_arrays(
                    [pa.array([None])],  # Support deleting null PK records
                    names=["pk_col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array(["c"])],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array(["c"])],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array([10, 11]), pa.array(["a", "b"])],
                    names=["pk_col_1", "col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["pk_col_1", "col_1"]),
            ),
            (
                pa.Table.from_arrays(
                    [pa.array(["c"])],
                    names=["col_1"],
                ),
                DeltaType.DELETE,
                DeleteParameters.of(["col_1"]),
            ),
        ],
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(10)] + [13]),
                pa.array([str(i) for i in range(0, 10)] + ["d"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_terminal_exception=None,
        expected_terminal_exception_message=None,
        do_create_placement_group=False,
        records_per_compacted_file=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count=DEFAULT_HASH_BUCKET_COUNT,
        read_kwargs_provider=None,
        drop_duplicates=True,
        skip_enabled_compact_partition_drivers=[CompactorVersion.V1],
        assert_compaction_audit=assert_compaction_audit,
    ),
}

REBASE_THEN_INCREMENTAL_TEST_CASES = with_compactor_version_func_test_param(
    {
        **REBASE_THEN_INCREMENTAL_TEST_CASES,
        **REBASE_THEN_INCREMENTAL_DELETE_DELTA_TYPE_TEST_CASES,
    },
)
