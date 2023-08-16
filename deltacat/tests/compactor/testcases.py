import pyarrow as pa
from deltacat.tests.compactor.common import (
    MAX_RECORDS_PER_FILE,
    offer_iso8601_timestamp_list,
)
from deltacat.tests.compactor.common import (
    BASE_TEST_SOURCE_TABLE_VERSION,
    BASE_TEST_DESTINATION_TABLE_VERSION,
)


"""
TODO Test Cases:
1. incremental w/wout round completion file
2. Backfill w/wout round completion
3. Rebase w/wout round completion file
4. Rebase then incremental (use same round completion file)
"""


INCREMENTAL_INDEPENDENT_TEST_CASES = {
    "1-incremental-pkstr-sknone-norcf": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        {"pk_col_1"},  # Primary key columns
        [],  # Sort key columns
        [{"key_name": "region_id", "key_type": "int"}],  # Partition keys
        ["pk_col_1"],  # column_names
        [pa.array([str(i) for i in range(10)])],  # arrow arrays
        None,  # rebase_source_partition_locator_param
        ["1"],  # partition_values_param
        pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])],
            names=["pk_col_1"],  # expected_result
        ),
        None,  # validation_callback_func
        None,  # validation_callback_func_kwargs
        True,  # teardown_local_deltacat_storage_db
        False,  # use_prev_compacted
        True,  # create_placement_group_param
        MAX_RECORDS_PER_FILE,  # records_per_compacted_file_param
        None,  # hash_bucket_count_param
    ),
    "2-incremental-pkstr-skstr-norcf": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            }
        ],
        [],
        ["pk_col_1", "sk_col_1"],
        [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
        None,
        ["1"],
        pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)]), pa.array(["test"] * 10)],
            names=["pk_col_1", "sk_col_1"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "3-incremental-pkstr-multiskstr-norcf": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1", "sk_col_2"],
        [
            pa.array([str(i) for i in range(10)]),
            pa.array(["test"] * 10),
            pa.array(["foo"] * 10),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(10)]),
                pa.array(["test"] * 10),
                pa.array(["foo"] * 10),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "4-incremental-duplicate-pk": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
            {
                "key_name": "sk_col_2",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1", "sk_col_2"],
        [
            pa.array([str(i) for i in range(5)] + ["6", "6", "6", "6", "6"]),
            pa.array([str(i) for i in range(10)]),
            pa.array(["foo"] * 10),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([str(i) for i in range(5)] + ["6"]),
                pa.array([str(i) for i in range(5)] + ["9"]),
                pa.array(["foo"] * 6),
            ],
            names=["pk_col_1", "sk_col_1", "sk_col_2"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "5-incremental-decimal-pk-simple": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1"],
        [
            pa.array([i / 10 for i in range(0, 10)]),
            pa.array([str(i) for i in range(10)]),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "7-incremental-integer-pk-simple": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1"],
        [
            pa.array([i for i in range(0, 10)]),
            pa.array([str(i) for i in range(10)]),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([i for i in range(0, 10)]),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "8-incremental-timestamp-pk-simple": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1"],
        [
            pa.array(offer_iso8601_timestamp_list(10, "minutes")),
            pa.array([str(i) for i in range(10)]),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array(offer_iso8601_timestamp_list(10, "minutes")),
                pa.array([str(i) for i in range(10)]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "9-incremental-decimal-timestamp-pk-multi": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1", "pk_col_2"],
        [
            {
                "key_name": "sk_col_1",
            },
        ],
        [],
        ["pk_col_1", "pk_col_2", "sk_col_1"],
        [
            pa.array([i / 10 for i in range(0, 20)]),
            pa.array(offer_iso8601_timestamp_list(20, "minutes")),
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 20)]),
                pa.array(offer_iso8601_timestamp_list(20, "minutes")),
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "10-incremental-decimal-pk-multi-dup": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1"],
        [
            {
                "key_name": "sk_col_1",
            },
        ],
        [],
        ["pk_col_1", "sk_col_1"],
        [
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            pa.array(reversed([i for i in range(20)])),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([0.1, 0.2, 0.3, 0.4, 0.5]),
                pa.array([19, 15, 11, 7, 3]),
            ],
            names=["pk_col_1", "sk_col_1"],
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
}

"""
for test_name, (
            source_table_version,
            destination_table_version,
            primary_keys_param,
            sort_keys_param,
            partition_keys_param,
            column_names_param,
            arrow_arrays_param,
            rebase_source_partition_locator_param,
            partition_values_param,
            expected_result,
            validation_callback_func,
            validation_callback_func_kwargs,
            do_teardown_local_deltacat_storage_db,
            use_prev_compacted,
            create_placement_group_param,
            records_per_compacted_file_param,
            hash_bucket_count_param,
        ) in INCREMENTAL_TEST_CASES.items()
"""

# TODO: Add test cases where next tc is dependent on the previous compacted table existing
INCREMENTAL_DEPENDENT_TEST_CASES = {
    "11-incremental-multi-dup-retain-table": (
        BASE_TEST_SOURCE_TABLE_VERSION,
        BASE_TEST_DESTINATION_TABLE_VERSION,
        ["pk_col_1", "pk_col_2"],
        [
            {
                "key_name": "sk_col_1",
            },
        ],
        [],
        ["pk_col_1", "pk_col_2", "sk_col_1"],
        [
            pa.array([i / 10 for i in range(0, 20)]),
            pa.array(offer_iso8601_timestamp_list(20, "minutes")),
            pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
        ],
        None,
        ["1"],
        pa.Table.from_arrays(
            [
                pa.array([i / 10 for i in range(0, 20)]),
                pa.array(offer_iso8601_timestamp_list(20, "minutes")),
                pa.array([0.1] * 4 + [0.2] * 4 + [0.3] * 4 + [0.4] * 4 + [0.5] * 4),
            ],
            names=["pk_col_1", "pk_col_2", "sk_col_1"],
        ),
        None,
        None,
        False,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
}

INCREMENTAL_TEST_CASES = {
    **INCREMENTAL_INDEPENDENT_TEST_CASES,
    **INCREMENTAL_DEPENDENT_TEST_CASES,
}
