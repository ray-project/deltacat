# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Dict, Any

from deltacat.compute.stats.models.manifest_entry_stats import ManifestEntryStats
from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType


class DeltaColumnStats(dict):
    """
    Stats container for an individual column across a dataset (a list of tables).

    Example:
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

        DeltaColumnStats("foo",
            ManifestEntryStats([
                SomeStats([A, D]),     #  Table 1
                SomeStats([G, J]),     #  Table 2
            ]))
        DeltaColumnStats("bar",
            ManifestEntryStats([
                SomeStats([B, E]),     #  Table 1
                SomeStats([H, K]),     #  Table 2
            ]))
        DeltaColumnStats("baz",
            ManifestEntryStats([
                SomeStats([C, F]),     #  Table 1
                SomeStats([I, L]),     #  Table 2
            ]))
    """
    @staticmethod
    def of(column: str, manifest_stats: ManifestEntryStats) -> DeltaColumnStats:
        dcs = DeltaColumnStats()
        dcs["column"] = column
        dcs["manifestStats"] = manifest_stats

        if manifest_stats:
            # Omit row count for columnar-centric stats
            dcs["stats"] = dcs.calculate_columnar_stats()

        return dcs

    @property
    def column(self) -> str:
        return self.get("column")

    @property
    def manifest_stats(self) -> Optional[ManifestEntryStats]:
        val: Dict[str, Any] = self.get("manifestStats")
        if val is not None and not isinstance(val, ManifestEntryStats):
            self["manifestStats"] = val = ManifestEntryStats(val)
        return val

    @property
    def stats(self) -> Optional[StatsResult]:
        """
        Aggregate of all stats for this dataset-wide column
        """
        val: Dict[str, Any] = self.get("stats")
        if val is not None and not isinstance(val, StatsResult):
            self["stats"] = val = StatsResult(val)
        elif val is None and self.manifest_stats:
            self["stats"] = val = self.calculate_columnar_stats()

        return val

    def calculate_columnar_stats(self):
        return StatsResult.merge(self.manifest_stats.stats, {StatsType.PYARROW_TABLE_BYTES})
