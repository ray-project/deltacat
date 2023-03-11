# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional

from deltacat.compute.compactor.model.primary_key_index import (
    PrimaryKeyIndexVersionLocator,
)
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.storage import DeltaLocator, PartitionLocator


class RoundCompletionInfo(dict):
    @staticmethod
    def of(
        high_watermark: int,
        compacted_delta_locator: DeltaLocator,
        compacted_pyarrow_write_result: PyArrowWriteResult,
        pk_index_pyarrow_write_result: PyArrowWriteResult,
        sort_keys_bit_width: int,
        primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
        rebase_source_partition_locator: Optional[PartitionLocator],
    ) -> RoundCompletionInfo:

        rci = RoundCompletionInfo()
        rci["highWatermark"] = high_watermark
        rci["compactedDeltaLocator"] = compacted_delta_locator
        rci["compactedPyarrowWriteResult"] = compacted_pyarrow_write_result
        rci["pkIndexPyarrowWriteResult"] = pk_index_pyarrow_write_result
        rci["sortKeysBitWidth"] = sort_keys_bit_width
        rci["primaryKeyIndexVersionLocator"] = primary_key_index_version_locator
        rci["rebaseSourcePartitionLocator"] = rebase_source_partition_locator
        return rci

    @property
    def high_watermark(self) -> int:
        return self["highWatermark"]

    @property
    def compacted_delta_locator(self) -> DeltaLocator:
        val: Dict[str, Any] = self.get("compactedDeltaLocator")
        if val is not None and not isinstance(val, DeltaLocator):
            self["compactedDeltaLocator"] = val = DeltaLocator(val)
        return val

    @property
    def compacted_pyarrow_write_result(self) -> PyArrowWriteResult:
        val: Dict[str, Any] = self.get("compactedPyarrowWriteResult")
        if val is not None and not isinstance(val, PyArrowWriteResult):
            self["compactedPyarrowWriteResult"] = val = PyArrowWriteResult(val)
        return val

    @property
    def pk_index_pyarrow_write_result(self) -> PyArrowWriteResult:
        val: Dict[str, Any] = self.get("pkIndexPyarrowWriteResult")
        if val is not None and not isinstance(val, PyArrowWriteResult):
            self["pkIndexPyarrowWriteResult"] = val = PyArrowWriteResult(val)
        return val

    @property
    def sort_keys_bit_width(self) -> int:
        return self["sortKeysBitWidth"]

    @property
    def primary_key_index_version_locator(self) -> PrimaryKeyIndexVersionLocator:
        val: Dict[str, Any] = self.get("primaryKeyIndexVersionLocator")
        if val is not None and not isinstance(val, PrimaryKeyIndexVersionLocator):
            self["primaryKeyIndexVersionLocator"] = val = PrimaryKeyIndexVersionLocator(
                val
            )
        return val

    @property
    def rebase_source_partition_locator(self) -> Optional[PartitionLocator]:
        return self.get("rebaseSourcePartitionLocator")
