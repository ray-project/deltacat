# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional

from deltacat.compute.stats.models.delta_stats import DeltaStats, DeltaStatsCacheMiss


class DeltaStatsCacheResult(dict):
    """
    A helper class containing the results from a cache query.

    Stats are fetched and cached at the column level, and each column may represent one
    or more manifest entries.

    `hits` represents a DeltaStats object that contains dataset-wide statistics across
    many of its tables (or manifest entries) and is comprised of one or more column-wide
    DeltaColumnStats.

    `misses` represents a DeltaStatsCacheMiss object that contains a list of
    column names that were not found in the file system (ex: S3) and a `delta_locator`
    as a reference to the delta metadata tied to the missing dataset columns.
    """
    @staticmethod
    def of(hits: Optional[DeltaStats], misses: Optional[DeltaStatsCacheMiss]) -> DeltaStatsCacheResult:
        cds = DeltaStatsCacheResult()
        cds["hits"] = hits
        cds["misses"] = misses
        return cds

    @property
    def hits(self) -> Optional[DeltaStats]:
        return self["hits"]

    @property
    def misses(self) -> Optional[DeltaStatsCacheMiss]:
        return self["misses"]
