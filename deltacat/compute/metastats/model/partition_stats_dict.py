# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from collections import defaultdict
from typing import List, Dict, Optional, Set, Any, NamedTuple

from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.compute.stats.models.manifest_entry_stats import ManifestEntryStats
from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.storage import DeltaLocator


class PartitionStats(dict):

    @staticmethod
    def of(delta_stats: Dict[DeltaStats], partition_canonical_string: str) -> PartitionStats:
        ps = PartitionStats()
        ps["delta_stats"] = delta_stats
        ps["partition_canonical_string"] = partition_canonical_string
        return ps

    @staticmethod
    def build_from_dict(partition_stats: str) -> PartitionStats:
        delta_stats_dict = {}
        for stream_position, delta_stats in partition_stats["delta_stats"].items():
            delta_stats_dict[stream_position] = DeltaStats.build_from_dict(delta_stats)
        return PartitionStats.of(delta_stats_dict, partition_stats["partition_canonical_string"])

    @property
    def delta_stats(self) -> Dict[DeltaStats]:
        return self["delta_stats"]

    @property
    def partition_canonical_string(self) -> str:
        return self["partition_canonical_string"]

