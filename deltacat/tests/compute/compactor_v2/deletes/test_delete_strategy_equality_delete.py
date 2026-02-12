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


@pytest.fixture(scope="module", autouse=True)
def setup_ray_cluster():
    ray.init(local_mode=True, ignore_reinit_error=True)
    yield
    ray.shutdown()


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
    "5-test-apply-deletes-column-does-not-exist-table": ApplyAllDeletesTestCaseParams(
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
    "7-test-composite-key-no-cross-join": ApplyAllDeletesTestCaseParams(
        # Regression test: delete rows with mismatched composite key values
        # must NOT cross-join to produce phantom matches.
        # Main table: pk_1=a for all rows, pk_2=1..4
        # Delete row 1: (a, 1) -> should match and delete row (a, 1)
        # Delete row 2: (b, 3) -> pk_1=b does not exist in main table,
        #   so this row should NOT cause any deletion.
        # Previously, the column-wise is_in + AND logic would incorrectly
        # delete row (a, 3) because pk_1=a is in {a,b} AND pk_2=3 is in {1,3}.
        table=pa.Table.from_arrays(
            [
                pa.array(["a", "a", "a", "a"]),
                pa.array([1, 2, 3, 4]),
                pa.array(["d1", "d2", "d3", "d4"]),
            ],
            names=["pk_1", "pk_2", "data"],
        ),
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array(["a", "b"]),
                        pa.array([1, 3]),
                    ],
                    names=["pk_1", "pk_2"],
                ),
                "delete_columns": ["pk_1", "pk_2"],
            }
        ],
        expected_table=pa.Table.from_arrays(
            [
                pa.array(["a", "a", "a"]),
                pa.array([2, 3, 4]),
                pa.array(["d2", "d3", "d4"]),
            ],
            names=["pk_1", "pk_2", "data"],
        ),
        expected_total_dropped_rows=1,
        expected_exception=None,
    ),
    "8-test-delete-null-pk-value": ApplyAllDeletesTestCaseParams(
        # Null values in key columns must be treated as equal (null == null)
        # for deletion matching, even though PyArrow joins default to SQL
        # semantics where null != null.
        table=pa.Table.from_arrays(
            [
                pa.array([1, 2, None], type=pa.int64()),
                pa.array(["a", "b", "c"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array([None], type=pa.int64()),
                    ],
                    names=["pk_col_1"],
                ),
                "delete_columns": ["pk_col_1"],
            }
        ],
        expected_table=pa.Table.from_arrays(
            [
                pa.array([1, 2]),
                pa.array(["a", "b"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_total_dropped_rows=1,
        expected_exception=None,
    ),
    "9-test-delete-null-typed-column": ApplyAllDeletesTestCaseParams(
        # When a key column has pa.null() data type (all values are null),
        # PyArrow join raises ArrowInvalid. The null-safe path must handle this
        # by casting the null-typed column before joining.
        table=pa.Table.from_arrays(
            [
                pa.array([None, None], type=pa.null()),
                pa.array(["a", "b"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array([None], type=pa.null()),
                    ],
                    names=["pk_col_1"],
                ),
                "delete_columns": ["pk_col_1"],
            }
        ],
        expected_table=pa.Table.from_arrays(
            [
                pa.array([], type=pa.null()),
                pa.array([], type=pa.string()),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_total_dropped_rows=2,
        expected_exception=None,
    ),
    "6-test-delete-multi-column": ApplyAllDeletesTestCaseParams(
        table=pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array(["0", "1", "2", "3"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        delete_file_envelopes_params=[
            {
                "stream_position": 1,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array(["0", "1"]),
                    ],
                    names=["col_1"],
                ),
                "delete_columns": ["col_1"],
            },
            {
                "stream_position": 2,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array([2]),
                        pa.array(["2"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                "delete_columns": ["pk_col_1", "col_1"],
            },
            {
                "stream_position": 3,
                "delta_type": DeltaType.DELETE,
                "table": pa.Table.from_arrays(
                    [
                        pa.array([3]),
                        pa.array(["DOESNOTMATCH"]),
                    ],
                    names=["pk_col_1", "col_1"],
                ),
                "delete_columns": ["pk_col_1", "col_1"],
            },
        ],
        expected_table=pa.Table.from_arrays(
            [
                pa.array(
                    [
                        3,
                    ]
                ),
                pa.array(["3"]),
            ],
            names=["pk_col_1", "col_1"],
        ),
        expected_total_dropped_rows=3,
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

    def test_apply_many_deletes_missing_table_ref_plasma_object_store(
        self,
    ):
        from deltacat.compute.compactor_v2.deletes.delete_strategy_equality_delete import (
            EqualityDeleteStrategy,
        )
        from deltacat.compute.compactor_v2.deletes.delete_strategy import (
            DeleteStrategy,
        )
        from deltacat.compute.compactor.model.table_object_store import (
            LocalTableRayObjectStoreReferenceStorageStrategy,
        )
        from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore

        delete_strategy: DeleteStrategy = EqualityDeleteStrategy()
        table = pa.Table.from_arrays(
            [
                pa.array([0, 1, 2, 3]),
                pa.array(["0", "1", "2", "3"]),
            ],
            names=["pk_col_1", "col_1"],
        )
        valid_delete_table = RayPlasmaObjectStore().put(
            pa.Table.from_arrays(([pa.array([0])]), names=["pk_col_1"])
        )
        delete_file_envelopes = [
            DeleteFileEnvelope(
                **{
                    "delta_type": DeltaType.DELETE,
                    "table": valid_delete_table,
                    "delete_columns": ["pk_col_1"],
                    "table_reference": valid_delete_table,
                    "table_storage_strategy": LocalTableRayObjectStoreReferenceStorageStrategy(),
                }
            ),
            DeleteFileEnvelope(
                **{
                    "delta_type": DeltaType.DELETE,
                    "table": None,
                    "delete_columns": ["col_1"],
                    "table_reference": None,
                    "table_storage_strategy": None,
                }
            ),
            DeleteFileEnvelope(
                **{
                    "delta_type": DeltaType.DELETE,
                    "table": valid_delete_table,
                    "delete_columns": ["pk_col_1"],
                    "table_reference": valid_delete_table,
                    "table_storage_strategy": LocalTableRayObjectStoreReferenceStorageStrategy(),
                }
            ),
        ]
        expected_table = pa.Table.from_arrays(
            [
                pa.array([1, 2, 3]),
                pa.array(["1", "2", "3"]),
            ],
            names=["pk_col_1", "col_1"],
        )
        actual_table, actual_dropped_rows = delete_strategy.apply_many_deletes(
            table, delete_file_envelopes
        )
        # no rows should be dropped since one of the delete file envelopes has no delete table
        assert expected_table.combine_chunks().equals(
            actual_table
        ), f"{expected_table} does not match {actual_table}"
        assert actual_dropped_rows == 1
