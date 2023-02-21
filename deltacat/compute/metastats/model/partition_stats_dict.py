# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Dict

from deltacat.compute.stats.models.delta_stats import DeltaStats


class PartitionStats(dict):
    @staticmethod
    def of(
        delta_stats: Dict[DeltaStats], partition_canonical_string: str
    ) -> PartitionStats:
        ps = PartitionStats()
        ps["delta_stats"] = delta_stats
        ps["partition_canonical_string"] = partition_canonical_string
        return ps

    @staticmethod
    def build_from_dict(partition_stats: str) -> PartitionStats:
        delta_stats_dict = {}
        for stream_position, delta_stats in partition_stats["delta_stats"].items():
            delta_stats_dict[stream_position] = DeltaStats.build_from_dict(delta_stats)
        return PartitionStats.of(
            delta_stats_dict, partition_stats["partition_canonical_string"]
        )

    @property
    def delta_stats(self) -> Dict[DeltaStats]:
        return self["delta_stats"]

    @property
    def partition_canonical_string(self) -> str:
        return self["partition_canonical_string"]
