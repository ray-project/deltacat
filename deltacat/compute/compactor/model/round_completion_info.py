# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import DeltaLocator, PartitionLocator
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from typing import Any, Dict, Optional
from deltacat.compute.compactor.model.primary_key_index import (
    PrimaryKeyIndexVersionLocator,
)


class HighWatermark(dict):
    """
    Inherit from dict to make it easy for serialization/deserialization.
    Keep both partition locator and high watermark as a tuple to be persisted in the rcf
    """

    def set(self, partition_locator: PartitionLocator, delta_stream_position: int):
        self[partition_locator.canonical_string()] = (
            partition_locator,
            delta_stream_position,
        )

    def get(self, partition_locator: PartitionLocator) -> int:
        if partition_locator.canonical_string() in self:
            return self[partition_locator.canonical_string()][1]
        return 0


class RoundCompletionInfo(dict):
    """
    In case of compacting deltas from multi-sources, the high_watermark dict records the high watermark for each source
    e.g., high_watermark = {"src1":100,"src2":200}
    src1 or src2 is the canonical_string of partition_locator
    """

    @staticmethod
    def of(
        high_watermark: HighWatermark,
        compacted_delta_locator: DeltaLocator,
        compacted_pyarrow_write_result: PyArrowWriteResult,
        pk_index_pyarrow_write_result: PyArrowWriteResult,
        sort_keys_bit_width: int,
        rebase_source_partition_locator: Optional[PartitionLocator],
        primary_key_index_version_locator: Optional[PrimaryKeyIndexVersionLocator],
    ) -> RoundCompletionInfo:

        rci = RoundCompletionInfo()
        rci["highWatermark"] = high_watermark
        rci["compactedDeltaLocator"] = compacted_delta_locator
        rci["compactedPyarrowWriteResult"] = compacted_pyarrow_write_result
        rci["pkIndexPyarrowWriteResult"] = pk_index_pyarrow_write_result
        rci["sortKeysBitWidth"] = sort_keys_bit_width
        rci["rebaseSourcePartitionLocator"] = rebase_source_partition_locator
        rci["primaryKeyIndexVersionLocator"] = primary_key_index_version_locator
        return rci

    @property
    def high_watermark(self) -> HighWatermark:
        val: Dict[str, Any] = self.get("highWatermark")
        if (
            val is not None
            and isinstance(val, dict)
            and not isinstance(val, HighWatermark)
        ):
            self["highWatermark"] = val = HighWatermark(val)
        return val

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

    @property
    def primary_key_index_version_locator(self) -> PrimaryKeyIndexVersionLocator:
        val: Dict[str, Any] = self.get("primaryKeyIndexVersionLocator")
        if val is not None and not isinstance(val, PrimaryKeyIndexVersionLocator):
            self["primaryKeyIndexVersionLocator"] = val = PrimaryKeyIndexVersionLocator(
                val
            )
        return val

    @property
    def pk_index_pyarrow_write_result(self) -> PyArrowWriteResult:
        val: Dict[str, Any] = self.get("pkIndexPyarrowWriteResult")
        if val is not None and not isinstance(val, PyArrowWriteResult):
            self["pkIndexPyarrowWriteResult"] = val = PyArrowWriteResult(val)
        return val
