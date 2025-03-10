# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional
import numpy as np

from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.storage import Delta


class MaterializeResult(dict):
    @staticmethod
    def of(
        delta: Delta,
        task_index: int,
        pyarrow_write_result: PyArrowWriteResult,
        referenced_pyarrow_write_result: Optional[PyArrowWriteResult] = None,
        peak_memory_usage_bytes: Optional[np.double] = None,
        telemetry_time_in_seconds: Optional[np.double] = None,
        task_completed_at: Optional[np.double] = None,
    ) -> MaterializeResult:
        materialize_result = MaterializeResult()
        materialize_result["delta"] = delta
        materialize_result["taskIndex"] = task_index
        materialize_result["paWriteResult"] = pyarrow_write_result
        materialize_result["referencedPaWriteResult"] = referenced_pyarrow_write_result
        materialize_result["peakMemoryUsageBytes"] = peak_memory_usage_bytes
        materialize_result["telemetryTimeInSeconds"] = telemetry_time_in_seconds
        materialize_result["taskCompletedAt"] = task_completed_at
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
    def referenced_pyarrow_write_result(self) -> PyArrowWriteResult:
        val: Dict[str, Any] = self.get("referencedPaWriteResult")
        if val is not None and not isinstance(val, PyArrowWriteResult):
            self["referencedPaWriteResult"] = val = PyArrowWriteResult(val)

        return val

    @property
    def task_completed_at(self) -> Optional[np.double]:
        return self["taskCompletedAt"]
