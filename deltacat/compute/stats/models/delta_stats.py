# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from collections import defaultdict
from typing import List, Dict, Optional, Set, Any, NamedTuple

from deltacat.compute.stats.models.delta_column_stats import DeltaColumnStats
from deltacat.compute.stats.models.manifest_entry_stats import ManifestEntryStats
from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.storage import DeltaLocator


class DeltaStats(dict):
    """
    Stats container for all columns of a delta.

    Provides distinct stats for each delta manifest entry, aggregate stats across all manifest entries,
    and a DeltaColumnStats reference for each column.

    Each DeltaColumnStats has a column name and a ManifestEntryStats object,
    which contains column-level stats for each delta manifest entry.

    Example of visual representation:
        Manifest Entry 1
        =======
        foo bar baz
        A   B   C
        D   E   F

        Manifest Entry 2
        =======
        foo bar baz
        G   H   I
        J   K   L

        DeltaStats([
            DeltaColumnStats("foo",
                ManifestEntryStats([
                    StatsResult([A, D]),     #  Manifest Entry 1
                    StatsResult([G, J]),     #  Manifest Entry 2
                ]))
            DeltaColumnStats("bar",
                ManifestEntryStats([
                    StatsResult([B, E]),     #  Manifest Entry 1
                    StatsResult([H, K]),     #  Manifest Entry 2
                ]))
            DeltaColumnStats("baz",
                ManifestEntryStats([
                    StatsResult([C, F]),     #  Manifest Entry 1
                    StatsResult([I, L]),     #  Manifest Entry 2
                ]))
        ], Stats(AllDeltaColumnStats))
    """

    @staticmethod
    def of(column_stats: List[DeltaColumnStats]) -> DeltaStats:
        ds = DeltaStats()
        ds["column_stats"] = column_stats
        ds["stats"] = DeltaStats.get_delta_stats(column_stats)
        return ds

    @staticmethod
    def build_from_dict(delta_stats: dict) -> DeltaStats:
        delta_column_stats_list = []
        for dcs in delta_stats["column_stats"]:
            delta_column_stats_list.append(DeltaColumnStats.build_from_dict(dcs))
        return DeltaStats.of(delta_column_stats_list)

    @property
    def column_stats(self) -> List[DeltaColumnStats]:
        """
        Returns a list of stats associated to each column in this delta.
        """
        return self["column_stats"]

    @property
    def stats(self) -> Optional[StatsResult]:
        """Returns a StatsResult object that represents this delta, aggregated by the column stats of this delta.
        """
        val: Dict[str, Any] = self.get("stats")
        if val is not None and not isinstance(val, StatsResult):
            self["stats"] = val = StatsResult(val)
        elif val is None and self.column_stats:
            self["stats"] = val = DeltaStats.get_delta_stats(self.column_stats)

        return val

    @property
    def columns(self) -> List[str]:
        """Returns a list of column names associated to this delta.

        Returns:
            A list of column names
        """
        return DeltaStats.get_column_names(self.column_stats)

    def manifest_entry_stats(self, manifest_entry_idx: int) -> StatsResult:
        """Calculate the stats of a manifest entry by combining its columnar stats.

        Args:
            manifest_entry_idx: The manifest entry table to calculate stats for

        Returns:
            Stats for the manifest entry.
        """
        return StatsResult.merge(DeltaStats.get_manifest_entry_column_stats(self.column_stats, manifest_entry_idx),
                                 record_row_count_once=True)

    def manifest_entry_column_stats(self, manifest_entry_idx: int) -> List[StatsResult]:
        """Fetch a list of stats for each column in a manifest entry.

        Args:
            manifest_entry_idx: The manifest entry table to calculate stats for

        Returns:
            A list of columnar stats for the manifest entry
        """
        return DeltaStats.get_manifest_entry_column_stats(self.column_stats, manifest_entry_idx)

    @staticmethod
    def get_manifest_entry_column_stats(columns: List[DeltaColumnStats], manifest_entry_idx: int) -> List[StatsResult]:
        """Helper method to provide a list of columnar stats for a specific manifest entry.

        Returns:
            A list of columnar stats for the manifest entry
        """
        dataset_columnar_stats_list: List[ManifestEntryStats] = [column.manifest_stats for column in columns
                                                                 if column.manifest_stats is not None]
        try:
            return [stats.stats[manifest_entry_idx] for stats in dataset_columnar_stats_list]
        except IndexError:
            sci: ManifestEntryStats = dataset_columnar_stats_list[0]
            raise ValueError(f"Table index {manifest_entry_idx} is not present in this dataset of {sci.delta_locator} "
                             f"with manifest table count of {len(sci.stats)}")

    @staticmethod
    def get_column_names(columns: List[DeltaColumnStats]) -> List[str]:
        """Helper method to get the names of each column from a list of delta column stats

        Args:
            columns: A list of delta column stats

        Returns:
            A list of column names
        """
        return [column_stats.column for column_stats in columns] if columns else []

    @staticmethod
    def get_delta_stats(columns: List[DeltaColumnStats],
                        stat_types: Optional[Set[StatsType]] = None) -> Optional[StatsResult]:
        """Calculate the sum of provided column stats and return it

        Args:
            columns: A list of delta column stats

        Returns:
            Stats for the calculated sum
        """
        assert columns and len(columns) > 0, \
            f"Expected columns `{columns}` of type `{type(columns)}` " \
            f"to be a non-empty list of DeltaColumnStats"

        assert all([col.manifest_stats for col in columns]), \
            f"Expected stats completion info to be present in each item of {columns} "

        manifest_entry_count = len(columns[0].manifest_stats.stats)
        column_stats_map: Dict[str, List[Optional[StatsResult]]] = \
            defaultdict(lambda: [None] * manifest_entry_count)

        for column_stats in columns:
            for file_idx, entry_stats in enumerate(column_stats.manifest_stats.stats):
                column_stats_map[column_stats.column][file_idx] = entry_stats

        return DeltaStats._merge_stats_from_columns_to_dataset(DeltaStats.get_column_names(columns),
                                                               column_stats_map,
                                                               manifest_entry_count,
                                                               stat_types)

    @staticmethod
    def _merge_stats_from_columns_to_dataset(column_names: List[str],
                                             column_stats: Dict[str, List[Optional[StatsResult]]],
                                             manifest_entries_size: int,
                                             stat_types: Optional[Set[StatsType]] = None) -> StatsResult:
        manifest_entry_stats_summary_list: List[StatsResult] = []
        for manifest_entry_idx in range(manifest_entries_size):
            curr_manifest_entry_column_stats_list: List[StatsResult] = []
            for column_name in column_names:
                current_table_column_stats: StatsResult = column_stats[column_name][manifest_entry_idx]
                curr_manifest_entry_column_stats_list.append(current_table_column_stats)

            curr_manifest_entry_stats_summary = StatsResult.merge(curr_manifest_entry_column_stats_list,
                                                                  stat_types,
                                                                  record_row_count_once=True)
            manifest_entry_stats_summary_list.append(curr_manifest_entry_stats_summary)
        return StatsResult.merge(manifest_entry_stats_summary_list, stat_types)


class DeltaStatsCacheMiss(NamedTuple):
    """A helper class for cache miss results from DeltaStatsCacheResult.

    `column_names` represents missing dataset column names from the file system (ex: S3).
    delta_locator` is tied to the missing dataset columns and provided for future calculations.
    """
    column_names: List[str]
    delta_locator: DeltaLocator
