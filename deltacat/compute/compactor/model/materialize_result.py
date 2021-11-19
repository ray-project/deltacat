# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import Delta
from deltacat.compute.compactor import PyArrowWriteResult


class MaterializeResult(dict):
    @staticmethod
    def of(delta: Delta,
           task_index: int,
           pyarrow_write_result: PyArrowWriteResult) -> MaterializeResult:
        return MaterializeResult({
            "delta": delta,
            "taskIndex": task_index,
            "paWriteResult": pyarrow_write_result,
        })

    @property
    def delta(self) -> Delta:
        return self["delta"]

    @property
    def task_index(self) -> int:
        return self["taskIndex"]

    @property
    def pyarrow_write_result(self) -> PyArrowWriteResult:
        return self["paWriteResult"]
