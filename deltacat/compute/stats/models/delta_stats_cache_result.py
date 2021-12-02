from typing import List

from deltacat.compute.stats.models.stats_completion_info import StatsCompletionInfo
from deltacat.storage import DeltaLocator


class DeltaStatsCacheResult(dict):
    """
    A result wrapper for a list of cached delta stats in a file system.

    `stats` represents a list of stats completion info for each delta resulting in a cache hit.
    `cache_miss_delta_locators` represents a list of delta locators for each delta resulting in a cache miss.
    """
    @staticmethod
    def of(stats: List[StatsCompletionInfo], cache_miss_delta_locators: List[DeltaLocator]):
        cds = DeltaStatsCacheResult()
        cds["stats"] = stats
        cds["cacheMissDeltaLocators"] = cache_miss_delta_locators
        return cds

    @property
    def stats(self) -> List[StatsCompletionInfo]:
        val = self["stats"]
        return [StatsCompletionInfo(_) for _ in val] if val else []

    @property
    def cache_miss_delta_locators(self) -> List[DeltaLocator]:
        val = self["cacheMissDeltaLocators"]
        return [DeltaLocator(_) for _ in val] if val else []
