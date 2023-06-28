# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional

from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.storage import Delta


class MaterializeResult(dict):
    @staticmethod
    def of(
        delta: Delta,
        task_index: int,
        pyarrow_write_result: PyArrowWriteResult,
        count_of_src_dfl_not_touched: Optional[int] = 0,
        count_of_src_dfl: Optional[int] = 0,
    ) -> MaterializeResult:
        materialize_result = MaterializeResult()
        materialize_result["delta"] = delta
        materialize_result["taskIndex"] = task_index
        materialize_result["paWriteResult"] = pyarrow_write_result
        materialize_result["countOfSrcFileNotTouched"] = count_of_src_dfl_not_touched
        materialize_result["countOfSrcFile"] = count_of_src_dfl
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

    @property
    def count_of_src_dfl_not_touched(self) -> int:
        return self["countOfSrcFileNotTouched"]

    @property
    def count_of_src_dfl(self) -> int:
        return self["countOfSrcFile"]
