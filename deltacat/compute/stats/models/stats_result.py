# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from deltacat.compute.stats.types import ALL_STATS_TYPES, StatsType


class StatsResult(dict):
    """A generic container that holds stats for a single manifest entry file."""

    @staticmethod
    def of(
        row_count: Optional[int] = 0, pyarrow_table_bytes: Optional[int] = 0
    ) -> StatsResult:
        """Static factory for building a stats result object

        Args:
            row_count: The total number of rows of a manifest entry
            pyarrow_table_bytes: The total number of bytes when loaded into memory as a PyArrow Table

        Returns:
            A stats result object
        """
        sr = StatsResult()
        sr[StatsType.ROW_COUNT.value] = row_count
        sr[StatsType.PYARROW_TABLE_BYTES.value] = pyarrow_table_bytes
        return sr

    @property
    def row_count(self) -> int:
        """Represents the row count of a manifest entry file.

        Returns:
            The total number of rows of a manifest entry
        """
        return self[StatsType.ROW_COUNT.value]

    @property
    def pyarrow_table_bytes(self) -> int:
        """Represents the size of a manifest entry file (in bytes) as it was loaded into a PyArrow table.

        Returns:
            The total number of bytes when loaded into memory as a PyArrow Table
        """
        return self[StatsType.PYARROW_TABLE_BYTES.value]

    @staticmethod
    def from_stats_types(stats_types: Dict[StatsType, Any]) -> StatsResult:
        """A helper method to filter a dictionary by supported stats and returns a stats result object.

        Args:
            stats_types: Stats that should be included for constructing a stats result

        Returns:
            A stats result object
        """
        return StatsResult(
            {
                k: v
                for k, v in stats_types.items()
                if k in [StatsType.ROW_COUNT, StatsType.PYARROW_TABLE_BYTES]
            }
        )

    @staticmethod
    def merge(
        stats_list: List[StatsResult],
        stat_types: Optional[Set[StatsType]] = None,
        record_row_count_once: bool = False,
    ) -> StatsResult:
        """Helper method to merge any list of StatsResult objects into one.

        StatsResult objects are merged by adding up their numerical stats.
        TODO (ricmiyam): Handle non-numerical stats when they are added

        Args:
            stat_types: If provided, the calculation will only include the requested stats.
            record_row_count_once: If optionally set to `True`, then row counts are only added
                from the first stats entry. One use case for this is merging table-centric stats
                by columns, since the row count is expected to be the same across different columns.

        Returns:
            A stats result object
        """
        assert isinstance(stats_list, list) and len(stats_list) > 0, (
            f"Expected stats list: {stats_list} of type {type(stats_list)} to be a "
            f"non-empty list of StatsResult objects."
        )

        # Fallback to all stat types if not provided
        stats_to_collect: Set = stat_types or ALL_STATS_TYPES

        merged_stats: Dict[StatsType, int] = defaultdict(int)
        for stats_result in stats_list:
            for stat_type in stats_to_collect:
                if stats_result:
                    merged_stats[stat_type.value] += stats_result[stat_type.value]

        if record_row_count_once and StatsType.ROW_COUNT in stats_to_collect:
            merged_stats[StatsType.ROW_COUNT.value] = stats_list[0].row_count

        return StatsResult.from_stats_types(merged_stats)
