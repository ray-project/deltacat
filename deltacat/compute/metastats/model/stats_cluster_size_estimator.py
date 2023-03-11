# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.compute.stats.models.delta_stats import DeltaStats


class StatsClusterSizeEstimator(dict):
    @staticmethod
    def of(
        memory_per_cpu: int,
        file_count_per_cpu: int,
        total_memory_needed: int,
        total_file_count: int,
    ) -> DeltaStats:
        estimator = StatsClusterSizeEstimator()
        estimator["memory_per_cpu"] = memory_per_cpu
        estimator["file_count_per_cpu"] = file_count_per_cpu
        estimator["total_memory_needed"] = total_memory_needed
        estimator["total_file_count"] = total_file_count
        return estimator

    @property
    def memory_per_cpu(self) -> int:
        """
        Returns a list of stats associated to each column in this delta.
        """
        return self["memory_per_cpu"]

    @property
    def file_count_per_cpu(self) -> int:
        """
        Returns a list of stats associated to each column in this delta.
        """
        return self["file_count_per_cpu"]

    @property
    def total_memory_needed(self) -> int:
        """
        Returns a list of stats associated to each column in this delta.
        """
        return self["total_memory_needed"]

    @property
    def total_file_count(self) -> int:
        """
        Returns a list of stats associated to each column in this delta.
        """
        return self["total_file_count"]

    @staticmethod
    def estimate_cpus_needed(estimator: StatsClusterSizeEstimator):

        # TODO(zyiqin): Current implementation is only for a rough guess using the PYARROW_INFLATION_MULTIPLIER,
        #  note the inflation rate is for content_length to pyarrow_table_bytes for all columns.
        #  The full implementation logic should be like:
        #  1. liner regression with 99 confidence level: pull metastats data for all deltas for this partition if len(datapoints) > 30.
        #  2. if not enough previous stats collected for same partition: Fall back to datapoints for all paritions for same table.
        #  3. If not enough stats collected for this table: use average content length to each content_type and content_encoding inflation rates
        #  4. If not enough stats for this content_type and content_encoding combination: use the basic PYARROW_INFLATION_MULTIPLIER instead.
        # So, only option 4 is implemented here since this pre-requirement for first 3 options are not met for first round of metastats&stats collection.

        min_cpus_based_on_memory = (
            estimator.total_memory_needed // estimator.memory_per_cpu
        ) + 1
        min_cpus_based_on_file_count = (
            estimator.total_file_count // estimator.file_count_per_cpu
        ) + 1
        return max(min_cpus_based_on_memory, min_cpus_based_on_file_count)
