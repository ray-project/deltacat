import pyarrow as pa
from typing import Dict, List
from deltacat.tests.compute.common import (
    offer_iso8601_timestamp_list,
)
from deltacat.tests.compute.constants import (
    BASE_TEST_SOURCE_TABLE_VERSION,
    BASE_TEST_DESTINATION_TABLE_VERSION,
    HASH_BUCKET_COUNT,
    MAX_RECORDS_PER_FILE,
)
from dataclasses import dataclass, fields

from deltacat.compute.compactor.compaction_session import (
    compact_partition_from_request as compact_partition_v1,
)


@dataclass(frozen=True)
class CompactorTestCase:
    source_table_version: str
    destination_table_version: str
    primary_keys_param: Dict[str, str]
    sort_keys_param: Dict[str, str]
    partition_keys_param: Dict[str, str]
    column_names_param: Dict[str, str]
    arrow_arrays_param: Dict[str, pa.Array]
    partition_values_param: str
    expected_result: pa.Table
    validation_callback_func: callable
    validation_callback_func_kwargs: Dict[str, str]
    teardown_local_deltacat_storage_db: bool
    use_prev_compacted: bool
    create_placement_group_param: bool
    records_per_compacted_file_param: int
    hash_bucket_count_param: int

    # makes CompactorTestCase iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@dataclass(frozen=True)
class RebaseThenIncrementalCompactorTestCase(CompactorTestCase):
    pass


def create_tests_cases_for_all_compactor_versions(test_cases: Dict[str, List]):
    final_cases = {}
    for version, compact_partition_func in enumerate([compact_partition_v1]):
        for case_name, case_value in test_cases.items():
            final_cases[f"{case_name}_v{version}"] = [
                *case_value,
                compact_partition_func,
            ]

    return final_cases


"""
TODO Test Cases:
1. incremental w/wout round completion file
2. Backfill w/wout round completion
3. Rebase w/wout round completion file
4. Rebase then incremental (use same round completion file)
"""


INCREMENTAL_INDEPENDENT_TEST_CASES: Dict[str, CompactorTestCase] = {
    "1-incremental-pkstr-sknone-norcf": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        column_names_param=["pk_col_1"],
        arrow_arrays_param=[pa.array([str(i) for i in range(10)])],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "2-incremental-pkstr-skstr-norcf": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        column_names_param=["pk_col_1", "sk_col_1"],
        arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
            names=["pk_col_1", "sk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "3-incremental-pkstr-multiskstr-norcf": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
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
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2"],
        arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
            pa.array(["foo"] * 10),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array(["test"] * 10),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "4-incremental-duplicate-pk": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
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
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2"],
        arrow_arrays_param=[
            pa.array([str(i) for i in range(5)] + ["6", "6", "6", "6", "6"]),
            pa.array([str(i) for i in range(10)]),
            pa.array(["foo"] * 10),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(5)] + ["6"]),
                pa.array([str(i) for i in range(5)] + ["9"]),
                pa.array(["foo"] * 6),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "5-incremental-decimal-pk-simple": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        column_names_param=["pk_col_1", "sk_col_1"],
        arrow_arrays_param=[
            pa.array([i / 10 for i in range(0, 10)]),
            pa.array([str(i) for i in range(10)]),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "6-incremental-integer-pk-simple": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        column_names_param=["pk_col_1", "sk_col_1"],
        arrow_arrays_param=[
            pa.array([i for i in range(0, 10)]),
            pa.array([str(i) for i in range(10)]),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array([i for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "7-incremental-timestamp-pk-simple": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        column_names_param=["pk_col_1", "sk_col_1"],
        arrow_arrays_param=[
            pa.array(offer_iso8601_timestamp_list(10, "minutes")),
            pa.array([str(i) for i in range(10)]),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array(offer_iso8601_timestamp_list(10, "minutes")),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "8-incremental-decimal-timestamp-pk-multi": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
        primary_keys_param={"pk_col_1", "pk_col_2"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        column_names_param=["pk_col_1", "pk_col_2", "sk_col_1"],
        arrow_arrays_param=[
            pa.array([i / 10 for i in range(0, 20)]),
            pa.array(offer_iso8601_timestamp_list(20, "minutes")),
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 20)]),
                pa.array(offer_iso8601_timestamp_list(20, "minutes")),
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
    "9-incremental-decimal-pk-multi-dup": CompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
        primary_keys_param={"pk_col_1"},
        sort_keys_param=[
            {
                "key_name": "sk_col_1",
            },
        ],
        partition_keys_param=[{"key_name": "region_id", "key_type": "int"}],
        column_names_param=["pk_col_1", "sk_col_1"],
        arrow_arrays_param=[
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            pa.array(reversed([i for i in range(20)])),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5]),
                pa.array([19, 15, 11, 7, 3]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    ),
}

# TODO: Add test cases where next tc is dependent on the previous compacted table existing
# INCREMENTAL_DEPENDENT_TEST_CASES = {
#     "11-incremental-multi-dup-retain-table": (
#         BASE_TEST_SOURCE_TABLE_VERSION,
#         BASE_TEST_DESTINATION_TABLE_VERSION,
#         ["pk_col_1", "pk_col_2"],
#         [
#             {
#                 "key_name": "sk_col_1",
#             },
#         ],
#         [],
#         ["pk_col_1", "pk_col_2", "sk_col_1"],
#         [
#             pa.array([i / 10 for i in range(0, 20)]),
#             pa.array(offer_iso8601_timestamp_list(20, "minutes")),
#             pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
#         ],
#         None,
#         ["1"],
#         pa.Table.from_arrays(
#             [
#                 pa.array([i / 10 for i in range(0, 20)]),
#                 pa.array(offer_iso8601_timestamp_list(20, "minutes")),
#                 pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
#             ],
#             names=["pk_col_1", "pk_col_2", "sk_col_1"],
#         ),
#         None,
#         None,
#         False,
#         False,
#         True,
#         MAX_RECORDS_PER_FILE,
#         HASH_BUCKET_COUNT,
#     ),
# }

INCREMENTAL_TEST_CASES = create_tests_cases_for_all_compactor_versions(
    {
        # **INCREMENTAL_INDEPENDENT_TEST_CASES,
    }
)

REBASE_THEN_INCREMENTAL_TEST_CASES = {
    "1-rebase-then-incremental": RebaseThenIncrementalCompactorTestCase(
        source_table_version=BASE_TEST_SOURCE_TABLE_VERSION,
        destination_table_version=BASE_TEST_DESTINATION_TABLE_VERSION,
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
        column_names_param=["pk_col_1", "sk_col_1", "sk_col_2"],
        arrow_arrays_param=[
            pa.array([str(i) for i in range(10)]),
            pa.array([i for i in range(10)]),
            pa.array(["foo"] * 10),
        ],
        partition_values_param=["1"],
        expected_result=pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array([i for i in range(10, 20)]),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        validation_callback_func=None,
        validation_callback_func_kwargs=None,
        teardown_local_deltacat_storage_db=True,
        use_prev_compacted=False,
        create_placement_group_param=True,
        records_per_compacted_file_param=MAX_RECORDS_PER_FILE,
        hash_bucket_count_param=None,
    )
}


REBASE_THEN_INCREMENTAL_TEST_CASES = create_tests_cases_for_all_compactor_versions(
    {
        **REBASE_THEN_INCREMENTAL_TEST_CASES,
    }
)
