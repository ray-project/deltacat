# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import DeltaLocator, PartitionLocator
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from typing import Any, Dict, Optional, Union


class RoundCompletionInfo(dict):
    """
    In case of compacting deltas from multi-sources, the high_watermark dict records the high watermark for each source
    e.g., high_watermark = {"src1":100,"src2":200}
    src1 or src2 is the canonical_string of partition_locator
    """

    @staticmethod
    def of(
        high_watermark: Union[Dict[PartitionLocator, int], int],
        compacted_delta_locator: DeltaLocator,
        compacted_pyarrow_write_result: PyArrowWriteResult,
        sort_keys_bit_width: int,
        rebase_source_partition_locator: Optional[PartitionLocator],
    ) -> RoundCompletionInfo:

        rci = RoundCompletionInfo()
        rci["highWatermark"] = high_watermark
        rci["compactedDeltaLocator"] = compacted_delta_locator
        rci["compactedPyarrowWriteResult"] = compacted_pyarrow_write_result
        rci["sortKeysBitWidth"] = sort_keys_bit_width
        rci["rebaseSourcePartitionLocator"] = rebase_source_partition_locator
        return rci

    @property
    def high_watermark(self) -> Union[Dict[PartitionLocator, int], int]:
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
    def sort_keys_bit_width(self) -> int:
        return self["sortKeysBitWidth"]

    @property
    def rebase_source_partition_locator(self) -> Optional[PartitionLocator]:
        return self.get("rebaseSourcePartitionLocator")
