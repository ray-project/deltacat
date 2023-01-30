# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import Delta
from deltacat.compute.compactor.model.pyarrow_write_result import \
    PyArrowWriteResult

from typing import Any, Dict


class MaterializeResult(dict):
    @staticmethod
    def of(delta: Delta,
           task_index: int,
           pyarrow_write_result: PyArrowWriteResult) -> MaterializeResult:
        materialize_result = MaterializeResult()
        materialize_result["delta"] = delta
        materialize_result["taskIndex"] = task_index
        materialize_result["paWriteResult"] = pyarrow_write_result
        return materialize_result

    @property
    def delta(self) -> Delta:
        val: Dict[str, Any] = self.get("delta")
        if val is not None and not isinstance(val, Delta):
            self["delta"] = val = Delta(val)
        return val

    @property
    def task_index(self) -> int:
        return self["taskIndex"]

    @property
    def pyarrow_write_result(self) -> PyArrowWriteResult:
        val: Dict[str, Any] = self.get("paWriteResult")
        if val is not None and not isinstance(val, PyArrowWriteResult):
            self["paWriteResult"] = val = PyArrowWriteResult(val)
        return val
