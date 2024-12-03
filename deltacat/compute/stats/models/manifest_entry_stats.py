# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, List

import pyarrow as pa

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.storage import DeltaLocator


class ManifestEntryStats(dict):
    """Holds computed statistics for one or more manifest entries (tables) and their corresponding delta locator.

    To be stored/retrieved from a file system (ex: S3).
    """

    @staticmethod
    def of(
        manifest_entries_stats: List[StatsResult], delta_locator: DeltaLocator
    ) -> ManifestEntryStats:
        """
        Creates a stats container that represents a particular manifest.

        `manifest_entries_stats` are a list of distinct stats for each manifest entry file
        tied to this manifest. `delta_locator` is provided as a reference to the delta where the
        manifest entries reside.
        """

        mes = ManifestEntryStats()
        mes["deltaLocator"] = delta_locator
        mes["stats"] = manifest_entries_stats
        mes["pyarrowVersion"] = pa.__version__
        return mes

    @staticmethod
    def build_from_dict(manifest_entries_stats: dict) -> ManifestEntryStats:
        stats_res_list = []
        for stats_res in manifest_entries_stats["stats"]:
            stats_res_list.append(
                StatsResult.of(stats_res["rowCount"], stats_res["pyarrowTableBytes"])
            )
        return ManifestEntryStats.of(
            stats_res_list, manifest_entries_stats["deltaLocator"]
        )

    @property
    def delta_locator(self) -> DeltaLocator:
        """Reference to the delta that holds the manifest entries

        Returns:
            A delta locator object
        """
        val: Dict[str, Any] = self.get("deltaLocator")
        if val is not None and not isinstance(val, DeltaLocator):
            self["deltaLocator"] = val = DeltaLocator(val)
        return val

    @property
    def stats(self) -> List[StatsResult]:
        """
        Returns a list of distinct stats for each manifest entry file.
        """
        val = self["stats"]
        return [StatsResult(_) for _ in val] if val else []

    @property
    def pyarrow_version(self) -> str:
        """
        Read-only property which returns the PyArrow version number as it was written into a file system.
        """
        return self.get("pyarrowVersion")
