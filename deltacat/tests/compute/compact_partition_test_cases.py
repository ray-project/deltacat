import pyarrow as pa
from typing import Callable, Dict, List
from deltacat.tests.compute.test_util_common import (
    offer_iso8601_timestamp_list,
)
from deltacat.tests.compute.test_util_constant import (
    DEFAULT_MAX_RECORDS_PER_FILE,
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

ZERO_VALUED_PARTITION_VALUES_PARAM = []
ZERO_VALUED_PARTITION_KEYS_PARAM = None

ENABLED_COMPACT_PARTITIONS_DRIVERS: List[Callable] = [compact_partition_v1]


def create_tests_cases_for_enabled_compactor_versions(
    test_cases: Dict[str, List] = None
):
    if test_cases is None:
        test_cases = {}
    final_cases = {}
    for version, compact_partition_func in enumerate(
        ENABLED_COMPACT_PARTITIONS_DRIVERS
    ):
        for case_name, case_value in test_cases.items():
            final_cases[f"{case_name}_v{version + 1}"] = [
                *case_value,
                compact_partition_func,
            ]

    return final_cases


@dataclass(frozen=True)
class CompactorTestCase:
    primary_keys_param: Dict[str, str]
    sort_keys_param: Dict[str, str]
    partition_keys_param: List[Dict[str, str]]
    partition_values_param: str
    column_names_param: List[str]
    input_deltas_arrow_arrays_param: List[pa.Array]
    input_deltas_delta_type: str
    expected_terminal_compact_partition_result: pa.Table
    create_placement_group_param: bool
    records_per_compacted_file_param: int
    hash_bucket_count_param: int
    validation_callback_func: Callable
    validation_callback_func_kwargs: Dict[str, str]
    create_table_strategy: Callable

    # makes CompactorTestCase iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@dataclass(frozen=True)
class IncrementalCompactionTestCase(CompactorTestCase):
    pass


@dataclass(frozen=True)
class RebaseThenIncrementalCompactorTestCase(CompactorTestCase):
    incremental_deltas_arrow_arrays_param: Dict[str, pa.Array]
    incremental_deltas_delta_type: DeltaType
    rebase_expected_compact_partition_result: pa.Table


INCREMENTAL_INDEPENDENT_TEST_CASES: Dict[str, IncrementalCompactionTestCase] = {
    "1-incremental-pkstr-sknone-norcf": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1"],
        input_deltas_arrow_arrays_param=[pa.array([str(i) for i in range(10)])],
        input_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "2-incremental-pkstr-skstr-norcf": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[],
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "3-incremental-pkstr-multiskstr-norcf": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "4-incremental-duplicate-pk": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "5-incremental-decimal-pk-simple": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "6-incremental-integer-pk-simple": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "7-incremental-timestamp-pk-simple": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "8-incremental-decimal-timestamp-pk-multi": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1", "pk_col_2"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "9-incremental-decimal-pk-multi-dup": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
    "10-incremental-decimal-pk-partitionless": IncrementalCompactionTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
        partition_keys_param=ZERO_VALUED_PARTITION_KEYS_PARAM,
        partition_values_param=ZERO_VALUED_PARTITION_KEYS_PARAM,
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_strategy,
    ),
}

REBASE_THEN_INCREMENTAL_TEST_CASES = {
    "10-rebase-then-incremental-sanity": RebaseThenIncrementalCompactorTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
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
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_rebase_w_deltas_strategy,
    ),
    "11-rebase-then-incremental-duplicate-pk": RebaseThenIncrementalCompactorTestCase(
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        partition_values_param=["1"],
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        input_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(5)] + ["6", "6", "6", "6", "6"]),
            pa.array([i for i in range(0, 10)]),
            pa.array(["foo"] * 10),
            pa.array([i / 10 for i in range(10, 20)]),
        ],
        input_deltas_delta_type=DeltaType.UPSERT,
        rebase_expected_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3", "4", "6"]),
                pa.array([0, 1, 2, 3, 4, 9]),
                pa.array(["foo", "foo", "foo", "foo", "foo", "foo"]),
                pa.array([1, 1.1, 1.2, 1.3, 1.4, 1.9]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        incremental_deltas_arrow_arrays_param=[
            pa.array([str(i) for i in range(5)] + ["6", "6", "7", "7", "8"]),
            pa.array([i for i in range(20, 30)]),
            pa.array(["foo"] * 10),
            pa.array([i / 10 for i in range(40, 50)]),
        ],
        incremental_deltas_delta_type=DeltaType.UPSERT,
        expected_terminal_compact_partition_result=pa.Table.from_arrays(
            [
                pa.array(["0", "1", "2", "3", "4", "6", "7", "8"]),
                pa.array([20, 21, 22, 23, 24, 26, 28, 29]),
                pa.array(["foo", "foo", "foo", "foo", "foo", "foo", "foo", "foo"]),
                pa.array([4, 4.1, 4.2, 4.3, 4.4, 4.6, 4.8, 4.9]),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2", "col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        create_placement_group_param=True,
        records_per_compacted_file_param=DEFAULT_MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
        create_table_strategy=create_src_w_deltas_destination_rebase_w_deltas_strategy,
    ),
}

INCREMENTAL_TEST_CASES = create_tests_cases_for_enabled_compactor_versions(
    INCREMENTAL_INDEPENDENT_TEST_CASES
)


REBASE_THEN_INCREMENTAL_TEST_CASES = create_tests_cases_for_enabled_compactor_versions(
    REBASE_THEN_INCREMENTAL_TEST_CASES
)
