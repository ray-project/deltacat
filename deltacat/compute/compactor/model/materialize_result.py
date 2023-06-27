# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional, Optional
import numpy as np

from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.storage import Delta


class MaterializeResult(dict):
    @staticmethod
    def of(
        delta: Delta,

        task_index: int,

        pyarrow_write_result: PyArrowWriteResult,
        count_of_src_dfl_not_touched: Optional[int] = 0,
        count_of_src_dfl: Optional[int] = 0, ,
        peak_memory_usage_bytes: np.double = None,
        telemetry_time_in_seconds: np.double = None,
    ) -> MaterializeResult:
        materialize_result = MaterializeResult()
        materialize_result["delta"] = delta
        materialize_result["taskIndex"] = task_index
        materialize_result["paWriteResult"] = pyarrow_write_result
        materialize_result["countOfSrcFileNotTouched"] = count_of_src_dfl_not_touched
        materialize_result["countOfSrcFile"] = count_of_src_dfl
        materialize_result["peakMemoryUsageBytes"] = peak_memory_usage_bytes
        materialize_result["telemetryTimeInSeconds"] = telemetry_time_in_seconds
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
    def peak_memory_usage_bytes(self) -> Optional[np.double]:
        return self["peakMemoryUsageBytes"]

    @property
    def telemetry_time_in_seconds(self) -> Optional[np.double]:
        return self["telemetryTimeInSeconds"]

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
