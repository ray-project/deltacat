from typing import Optional

from deltacat.compute.stats.models.dataset_stats import DatasetStats, DatasetStatsCacheMiss


class DeltaStatsCacheResult(dict):
    """
    A helper class containing the results from a cache query.

    Stats are fetched and cached at the column level, and each column may represent one
    or more manifest entries.

    `hits` represents a DatasetStats object that contains dataset-wide statistics across
    many of its tables (or manifest entries) and is comprised of one or more column-wide
    DatasetColumnStats.

    `misses` represents a DatasetStatsCacheMiss object that contains a list of
    column names that were not found in the file system (ex: S3) and a `delta_locator`
    as a reference to the delta metadata tied to the missing dataset columns.
    """
    @staticmethod
    def of(hits: Optional[DatasetStats], misses: Optional[DatasetStatsCacheMiss]):
        cds = DeltaStatsCacheResult()
        cds["hits"] = hits
        cds["misses"] = misses
        return cds

    @property
    def hits(self) -> Optional[DatasetStats]:
        return self["hits"]

    @property
    def misses(self) -> Optional[DatasetStatsCacheMiss]:
        return self["misses"]
