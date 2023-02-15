# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import DeltaLocator, PartitionLocator
from deltacat.compute.compactor.model.pyarrow_write_result import \
    PyArrowWriteResult
from deltacat.compute.compactor.model.primary_key_index import \
    PrimaryKeyIndexVersionLocator

from typing import Any, Dict, Optional


class RoundCompletionInfo(dict):
    @staticmethod
    def of(high_watermark: int,
           compacted_delta_locator: DeltaLocator,
           sort_keys_bit_width: int,
           rebase_source_partition_locator: Optional[PartitionLocator],
           compacted_last_stream_position: int,) \
            -> RoundCompletionInfo:

        rci = RoundCompletionInfo()
        rci["highWatermark"] = high_watermark
        rci["compactedDeltaLocator"] = compacted_delta_locator
        rci["sortKeysBitWidth"] = sort_keys_bit_width
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
    def sort_keys_bit_width(self) -> int:
        return self["sortKeysBitWidth"]

    @property
    def rebase_source_partition_locator(self) -> Optional[PartitionLocator]:
        return self.get("rebaseSourcePartitionLocator")

