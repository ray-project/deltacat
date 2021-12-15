# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import pyarrow as pa

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.storage import DeltaLocator

from typing import Any, Dict, List


class StatsCompletionInfo(dict):
    """
    Holds computed statistics for one or more manifest entries (tables) and their corresponding delta locator.

    To be stored/retrieved from a file system (ex: S3).
    """
    @staticmethod
    def of(manifest_entries_stats: List[StatsResult],
           delta_locator: DeltaLocator) -> StatsCompletionInfo:

        sci = StatsCompletionInfo()
        sci["deltaLocator"] = delta_locator
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
    def manifest_entries_stats(self) -> List[StatsResult]:
        val = self["manifestEntriesStats"]
        return [StatsResult(_) for _ in val] if val else []

    @property
    def pyarrow_version(self) -> str:
        return self.get("pyarrowVersion")
