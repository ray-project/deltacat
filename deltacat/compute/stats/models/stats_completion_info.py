# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.storage import DeltaLocator

from typing import Any, Dict


class StatsCompletionInfo(dict):
    @staticmethod
    def of(high_watermark: int,
           delta_locator: DeltaLocator,
           stats_result: StatsResult) \
            -> StatsCompletionInfo:

        sci = StatsCompletionInfo()
        sci["highWatermark"] = high_watermark
        sci["deltaLocator"] = delta_locator
        sci["statsResult"] = stats_result
        return sci

    @property
    def high_watermark(self) -> int:
        return self["highWatermark"]

    @property
    def delta_locator(self) -> DeltaLocator:
        val: Dict[str, Any] = self.get("deltaLocator")
        if val is not None and not isinstance(val, DeltaLocator):
            self["deltaLocator"] = val = DeltaLocator(val)
        return val

    @property
    def stats_result(self) -> StatsResult:
        val: Dict[str, Any] = self.get("statsResult")
        if val is not None and not isinstance(val, StatsResult):
            self["statsResult"] = val = StatsResult(val)
        return val

