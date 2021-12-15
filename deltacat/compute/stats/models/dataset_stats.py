# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from collections import defaultdict
from typing import List, Dict, Optional, Set, Any, NamedTuple

from deltacat.compute.stats.models.dataset_column_stats import DatasetColumnStats
from deltacat.compute.stats.models.stats_completion_info import StatsCompletionInfo
from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.storage import DeltaLocator


class DatasetStats(dict):
    """
    Stats container for all columns across a dataset (a list of tables).

    Provides a stats summary for the entire dataset and contains
    a list of references to DatasetColumnStats, one for each column in the dataset.

    Each DatasetColumnStats has a column name and a StatsCompletionInfo object,
    which contains stats results of each table for that particular column.

    Example of visual representation:
        Table 1
        =======
        foo bar baz
        A   B   C
        D   E   F

        Table 2
        =======
        foo bar baz
        G   H   I
        J   K   L

        DatasetStats([
            DatasetColumnStats("foo",
                StatsCompletionInfo([
                    Stats([A, D]),     #  Table 1
                    Stats([G, J]),     #  Table 2
                ]))
            DatasetColumnStats("bar",
                StatsCompletionInfo([
                    Stats([B, E]),     #  Table 1
                    Stats([H, K]),     #  Table 2
                ]))
            DatasetColumnStats("baz",
                StatsCompletionInfo([
                    Stats([C, F]),     #  Table 1
                    Stats([I, L]),     #  Table 2
                ]))
        ], Stats(AllDatasetColumnStats))
    """

    @staticmethod
    def of(columns: List[DatasetColumnStats]):
        ds = DatasetStats()
        ds["columns"] = columns
        ds["stats"] = DatasetStats.generate_stats_from_columns(columns)
        return ds

    @property
    def columns(self) -> List[DatasetColumnStats]:
        return self["columns"]

    @property
    def stats(self) -> Optional[StatsResult]:
        val: Dict[str, Any] = self.get("stats")
        if val is not None and not isinstance(val, StatsResult):
            self["stats"] = val = StatsResult(val)
        elif val is None and self.columns:
            self["stats"] = val = DatasetStats.generate_stats_from_columns(self.columns)

        return val

    @property
    def column_names(self) -> List[str]:
        return DatasetStats.get_column_names(self.columns)

    def table_stats(self, table_idx: int) -> StatsResult:
        return StatsResult.merge(DatasetStats.get_table_stats_list(self.columns, table_idx),
                                 record_row_count_once=True)

    def table_stats_list(self, table_idx: int) -> List[StatsResult]:
        return DatasetStats.get_table_stats_list(self.columns, table_idx)

    @staticmethod
    def get_table_stats_list(columns: List[DatasetColumnStats], table_idx: int) -> List[StatsResult]:
        """
        Helper method to provide a list of columnar stats for a specific table/manifest entry

        TODO (ricmiyam): A table specific stats container would be nice to have (i.e. TableStats, TableColumnStats)
        """
        dataset_columnar_stats_list: List[StatsCompletionInfo] = [column.manifest_stats for column in columns
                                                                  if column.manifest_stats is not None]
        try:
            return [stats.manifest_entries_stats[table_idx] for stats in dataset_columnar_stats_list]
        except IndexError:
            sci: StatsCompletionInfo = dataset_columnar_stats_list[0]
            raise ValueError(f"Table index {table_idx} is not present in this dataset of {sci.delta_locator} "
                             f"with manifest table count of {len(sci.manifest_entries_stats)}")

    @staticmethod
    def get_column_names(columns: List[DatasetColumnStats]):
        return [column_stats.column for column_stats in columns] if columns else []

    @staticmethod
    def generate_stats_from_columns(columns: List[DatasetColumnStats],
                                    stat_types: Optional[Set[StatsType]] = None) -> Optional[StatsResult]:
        assert columns and len(columns) > 0, \
            f"Expected columns {columns} of type ({type(columns)}) " \
            f"to be a non-empty list of DatasetColumnStats"

        assert all([col.manifest_stats for col in columns]), \
            f"Expected stats completion info to be present in each item of {columns} "

        manifest_entry_count = len(columns[0].manifest_stats.manifest_entries_stats)
        column_stats_map: Dict[str, List[Optional[StatsResult]]] = \
            defaultdict(lambda: [None] * manifest_entry_count)

        for column_stats in columns:
            for file_idx, entry_stats in enumerate(column_stats.manifest_stats.manifest_entries_stats):
                column_stats_map[column_stats.column][file_idx] = entry_stats

        return DatasetStats._merge_stats_from_columns_to_dataset(DatasetStats.get_column_names(columns),
                                                                 column_stats_map,
                                                                 manifest_entry_count,
                                                                 stat_types)

    @staticmethod
    def _merge_stats_from_columns_to_dataset(column_names: List[str],
                                             column_stats: Dict[str, List[Optional[StatsResult]]],
                                             manifest_entries_size: int,
                                             stat_types: Optional[Set[StatsType]] = None) -> StatsResult:
        table_stats_list: List[StatsResult] = []
        for table_idx in range(manifest_entries_size):
            curr_table_stats_list: List[StatsResult] = []
            for column_name in column_names:
                current_table_column_stats: StatsResult = column_stats[column_name][table_idx]
                curr_table_stats_list.append(current_table_column_stats)

            curr_table_stats_summary = StatsResult.merge(curr_table_stats_list, stat_types, record_row_count_once=True)
            table_stats_list.append(curr_table_stats_summary)
        return StatsResult.merge(table_stats_list, stat_types)


class DatasetStatsCacheMiss(NamedTuple):
    """
    A helper class for cache miss results from DeltaStatsCacheResult.

    `columns` represents missing dataset column names from the file system (ex: S3).
    `delta_locator` is tied to the missing dataset columns and provided for future calculations.
    """
    column_names: List[str]
    delta_locator: DeltaLocator
