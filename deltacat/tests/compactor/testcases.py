# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
import pyarrow as pa
from deltacat.tests.compactor.common import (
    MAX_RECORDS_PER_FILE,
)


INCREMENTAL_TEST_CASES = {
    "1-incremental-pkstr-sknone-norcf": (
        {"pk_col_1"},  # Primary key columns
        [],  # Sort key columns
        [{"key_name": "region_id", "key_type": "int"}],  # Partition keys
        ["pk_col_1"],  # column_names
        [pa.array([str(i) for i in range(10)])],  # arrow arrays
        None,
        ["1"],
        pa.Table.from_arrays(
            [pa.array([str(i) for i in range(10)])], names=["pk_col_1"]
        ),
        None,
        None,
        True,
        False,
        True,
        MAX_RECORDS_PER_FILE,
        None,
    ),
    "2-incremental-pkstr-skstr-norcf": (
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
}
