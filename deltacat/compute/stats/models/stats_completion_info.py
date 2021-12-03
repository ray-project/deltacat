# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import pyarrow as pa

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.storage import DeltaLocator

from typing import Any, Dict


class StatsCompletionInfo(dict):
    @staticmethod
    def of(delta_locator: DeltaLocator,
           delta_stats: StatsResult,
           manifest_entries_stats: Dict[int, StatsResult]) -> StatsCompletionInfo:

        sci = StatsCompletionInfo()
        sci["deltaLocator"] = delta_locator
        sci["deltaStats"] = delta_stats
        sci["manifestEntriesStats"] = manifest_entries_stats
        sci["pyarrowVersion"] = pa.__version__
        return sci

    @property
    def delta_locator(self) -> DeltaLocator:
        val: Dict[str, Any] = self.get("deltaLocator")
        if val is not None and not isinstance(val, DeltaLocator):
            self["deltaLocator"] = val = DeltaLocator(val)
        return val

    @property
    def delta_stats(self) -> StatsResult:
        val: Dict[str, Any] = self.get("deltaStats")
        if val is not None and not isinstance(val, StatsResult):
            self["deltaStats"] = val = StatsResult(val)
        return val

    @property
    def manifest_entries_stats(self) -> Dict[int, StatsResult]:
        return self.get("manifestEntriesStats")

    @property
    def pyarrow_version(self) -> str:
        return self.get("pyarrowVersion")

