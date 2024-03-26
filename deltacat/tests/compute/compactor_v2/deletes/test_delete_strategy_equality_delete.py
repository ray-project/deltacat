import pytest

from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from dataclasses import dataclass, fields
import ray
from typing import List
import pyarrow as pa


@dataclass(frozen=True)
class ApplyAllDeletesTestCaseParams:
    """
    A pytest parameterized test case for the `apply_all_deletes` function.
    """

    table: pa.Table
    delete_file_envelopes: List[DeleteFileEnvelope]
    expected_table: pa.Table
    expected_total_dropped_rows: int
    expected_exception: BaseException

    # makes TestCaseParams iterable which is required to build the list of pytest.param values to pass to pytest.mark.parametrize
    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


TEST_CASES_APPLY_MANY_DELETES = {
    "1-test-sanity": ApplyAllDeletesTestCaseParams(
        table=pa.Table.from_arrays(
            [
                pa.array([0, 1]),
                pa.array(["0", "1"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        delete_file_envelopes=[
            DeleteFileEnvelope.of(
                1,
                DeltaType.DELETE,
                pa.Table.from_arrays(
                    [
                        pa.array(["1"]),
                    ],
                    names=["col_1"],
                ),
                ["col_1"],
            )
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
}


class TestEqualityDeleteStrategy:
    def test_apply_deletes(self):
        # TODO:
        pass

    @pytest.mark.parametrize(
        [
            "test_name",
            "table",
            "delete_file_envelopes",
            "expected_table",
            "expected_total_dropped_rows",
            "expected_exception",
        ],
        [
            (
                test_name,
                table,
                delete_file_envelopes,
                expected_table,
                expected_total_dropped_rows,
                expected_exception,
            )
            for test_name, (
                table,
                delete_file_envelopes,
                expected_table,
                expected_total_dropped_rows,
                expected_exception,
            ) in TEST_CASES_APPLY_MANY_DELETES.items()
        ],
        ids=[test_name for test_name in TEST_CASES_APPLY_MANY_DELETES],
    )
    def test_apply_all_deletes(
        self,
        test_name: str,
        table: pa.Table,
        delete_file_envelopes: List[DeleteFileEnvelope],
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
        if expected_exception:
            with pytest.raises(expected_exception):
                delete_strategy.apply_all_deletes(
                    table,
                    delete_file_envelopes,
                )
            return
        actual_table, actual_dropped_rows = delete_strategy.apply_all_deletes(
            table,
            delete_file_envelopes,
        )
        assert expected_table.combine_chunks().equals(
            actual_table
        ), f"{expected_table} does not match {actual_table}"
        assert expected_total_dropped_rows == actual_dropped_rows
