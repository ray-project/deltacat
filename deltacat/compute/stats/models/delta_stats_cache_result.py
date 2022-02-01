# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional

from deltacat.compute.stats.models.delta_stats import DeltaStats, DeltaStatsCacheMiss


class DeltaStatsCacheResult(dict):
    """A helper class containing the results from a cache query.

    Stats are fetched and cached at the column level, and each column may represent one
    or more manifest entries.
    """
    @staticmethod
    def of(hits: Optional[DeltaStats], misses: Optional[DeltaStatsCacheMiss]) -> DeltaStatsCacheResult:
        cds = DeltaStatsCacheResult()
        cds["hits"] = hits
        cds["misses"] = misses
        return cds

    @property
    def hits(self) -> Optional[DeltaStats]:
        """Retrieve stats that were found in the cache

        `hits` represents a DeltaStats object that contains dataset-wide statistics across
        many of its tables (or manifest entries) and is composed of one or more column-wide
        DeltaColumnStats.

        Returns:
            A delta wide stats container
        """
        return self["hits"]

    @property
    def misses(self) -> Optional[DeltaStatsCacheMiss]:
        """Retrieve stats that were missing from the cache

        `misses` represents a DeltaStatsCacheMiss object that contains a list of
        column names that were not found in the file system (ex: S3) and a `delta_locator`
        as a reference to the delta metadata tied to the missing dataset columns.

        Returns:
            A tuple with metadata regarding the cache miss
        """
        return self["misses"]
