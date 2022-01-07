# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, List, Set, Dict, Any

from collections import defaultdict
from deltacat.compute.stats.types import StatsType, ALL_STATS_TYPES


class StatsResult(dict):
    @staticmethod
    def of(row_count: Optional[int] = 0,
           pyarrow_table_bytes: Optional[int] = 0) -> StatsResult:
        sr = StatsResult()
        sr[StatsType.ROW_COUNT.value] = row_count
        sr[StatsType.PYARROW_TABLE_BYTES.value] = pyarrow_table_bytes
        return sr

    @property
    def row_count(self) -> int:
        return self[StatsType.ROW_COUNT.value]

    @property
    def pyarrow_table_bytes(self) -> int:
        return self[StatsType.PYARROW_TABLE_BYTES.value]

    @staticmethod
    def from_stats_types(stats_types: Dict[StatsType, Any]) -> StatsResult:
        return StatsResult({k: v for k, v in stats_types.items()
                            if k in [StatsType.ROW_COUNT, StatsType.PYARROW_TABLE_BYTES]})

    @staticmethod
    def merge(stats_list: List[StatsResult],
              stat_types: Optional[Set[StatsType]] = None,
              record_row_count_once: bool = False) -> StatsResult:
        """
        Helper method to merge any list of StatsResult objects into a single
        StatsResult object by adding up their numerical stats.

        If `stat_types` is provided, the calculation will only include the requested stats.

        If `record_row_count_once` is optionally set to `True`, then row counts are only added
        from the first stats entry. One use case for this is merging table-centric stats
        by columns, since the row count is expected to be the same across
        different columns.

        TODO (ricmiyam): Handle non-numerical stats when they are added
        """
        assert isinstance(stats_list, list) and len(stats_list) > 0, \
            f"Expected stats list: {stats_list} of type {type(stats_list)} to be a " \
            f"non-empty list of StatsResult objects."

        # Fallback to all stat types if not provided
        stats_to_collect: Set = stat_types or ALL_STATS_TYPES

        merged_stats: Dict[StatsType, int] = defaultdict(int)
        for stats_result in stats_list:
            for stat_type in stats_to_collect:
                merged_stats[stat_type.value] += stats_result[stat_type.value]

        if record_row_count_once and StatsType.ROW_COUNT in stats_to_collect:
            merged_stats[StatsType.ROW_COUNT.value] = stats_list[0].row_count

        return StatsResult.from_stats_types(merged_stats)
