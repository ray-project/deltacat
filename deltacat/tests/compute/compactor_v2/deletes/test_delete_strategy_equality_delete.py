import pytest

from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from dataclasses import dataclass, fields
import ray
from typing import Any, Dict, List
import pyarrow as pa


@dataclass(frozen=True)
class ApplyAllDeletesTestCaseParams:
    """
    A pytest parameterized test case for the `apply_all_deletes` function.
    """

    table: pa.Table
    delete_file_envelopes_params: List[Dict[str, Any]]
    expected_table: pa.Table
    expected_total_dropped_rows: int
    expected_exception: BaseException

    # makes TestCaseParams iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


TEST_CASES_APPLY_MANY_DELETES = {
    "1-test-delete-successful": ApplyAllDeletesTestCaseParams(
        table=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["col_1"],
                ),
                "delete_columns": ["col_1"],
            }
        ],
        expected_table=pa.Table.from_arrays(
            [
                pa.array(
                    [
                        0,
                    ]
                ),
                pa.array(["0"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_total_dropped_rows=1,
        expected_exception=None,
    ),
    "2-test-no-table": ApplyAllDeletesTestCaseParams(
        table=None,
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["col_1"],
                ),
                "delete_columns": ["col_1"],
            }
        ],
        expected_table=None,
        expected_total_dropped_rows=0,
        expected_exception=None,
    ),
    "3-test-apply-deletes-no-delete-table": ApplyAllDeletesTestCaseParams(
        table=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
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
                "delete_columns": ["col_1"],
            }
        ],
        expected_table=None,
        expected_total_dropped_rows=0,
        expected_exception=None,
    ),
    "4-test-apply-deletes-column-does-not-exist-table": ApplyAllDeletesTestCaseParams(
        table=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["invalid"],
                ),
                "delete_columns": ["invalid"],
            }
        ],
        expected_table=None,
        expected_total_dropped_rows=0,
        expected_exception=None,
    ),
}


class TestEqualityDeleteStrategy:
    def test_apply_deletes(self):
        # TODO:
        pass

    @pytest.mark.parametrize(
        [
            "test_name",
            "table",
            "delete_file_envelopes_params",
            "expected_table",
            "expected_total_dropped_rows",
            "expected_exception",
        ],
        [
            (
                test_name,
                table,
                delete_file_envelopes_params,
                expected_table,
                expected_total_dropped_rows,
                expected_exception,
            )
            for test_name, (
                table,
                delete_file_envelopes_params,
                expected_table,
                expected_total_dropped_rows,
                expected_exception,
            ) in TEST_CASES_APPLY_MANY_DELETES.items()
        ],
        ids=[test_name for test_name in TEST_CASES_APPLY_MANY_DELETES],
    )
    def test_apply_many_deletes(
        self,
        test_name: str,
        table: pa.Table,
        delete_file_envelopes_params: List[Dict[str, Any]],
        expected_table: pa.Table,
        expected_total_dropped_rows: int,
        expected_exception: BaseException,
    ):
        from deltacat.compute.compactor_v2.deletes.delete_strategy_equality_delete import (
            EqualityDeleteStrategy,
        )
        from deltacat.compute.compactor_v2.deletes.delete_strategy import (
            DeleteStrategy,
        )

        delete_strategy: DeleteStrategy = EqualityDeleteStrategy()
        ray.shutdown()
        ray.init(local_mode=True, ignore_reinit_error=True)
        delete_file_envelopes = [
            DeleteFileEnvelope.of(**params) for params in delete_file_envelopes_params
        ]
        if expected_exception:
            with pytest.raises(expected_exception):
                delete_strategy.apply_many_deletes(
                    table,
                    delete_file_envelopes,
                )
            return
        actual_table, actual_dropped_rows = delete_strategy.apply_many_deletes(
            table,
            delete_file_envelopes,
        )
        if expected_table:
            assert expected_table.combine_chunks().equals(
                actual_table
            ), f"{expected_table} does not match {actual_table}"
        assert expected_total_dropped_rows == actual_dropped_rows
