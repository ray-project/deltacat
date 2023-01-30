import logging

import pyarrow
import ray
from collections import defaultdict

from deltacat.compute.stats.utils.manifest_stats_file import read_manifest_stats_by_columns, write_manifest_stats_file
from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.models.manifest_entry_stats import ManifestEntryStats
from deltacat.compute.stats.models.delta_column_stats import DeltaColumnStats
from deltacat.compute.stats.models.delta_stats import DeltaStats, DeltaStatsCacheMiss

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.utils.intervals import DeltaRange
from deltacat.storage import PartitionLocator, Delta, DeltaLocator
from deltacat import logs, LocalTable, TableType
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.compute.compactor import DeltaAnnotated

from typing import Dict, List, Optional, Any

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def read_cached_delta_stats(delta: Delta,
                            columns_to_fetch: List[str],
                            stat_results_s3_bucket: str):
    """Read delta stats that are cached in S3

    This Ray distributed task reads delta stats from a file system (i.e. S3) based on specified columns.
    Stats are extracted from columns that are found (cache hit), while columns that are missing
    from the file system will record their column names and the delta locator as a cache miss.

    Args:
        delta: The delta object to look up
        columns_to_fetch: Columns to look up for this delta
        stat_results_s3_bucket: The S3 bucket name
    """

    delta_locator = DeltaLocator.of(delta.partition_locator, delta.stream_position)
    column_stats_completion_info: List[DeltaColumnStats] = \
        read_manifest_stats_by_columns(stat_results_s3_bucket, columns_to_fetch, delta_locator)

    found_columns_stats: List[DeltaColumnStats] = []
    missed_columns: List[str] = []
    for column_stats in column_stats_completion_info:
        if column_stats.manifest_stats:
            found_columns_stats.append(column_stats)
        else:
            missed_columns.append(column_stats.column)

    found_stats: Optional[DeltaStats] = DeltaStats.of(found_columns_stats) if found_columns_stats else None
    missed_stats: Optional[DeltaStatsCacheMiss] = DeltaStatsCacheMiss(missed_columns, delta.locator) \
        if missed_columns else None

    return DeltaStatsCacheResult.of(found_stats, missed_stats)


@ray.remote
def cache_delta_column_stats(stat_results_s3_bucket: str,
                             dataset_column: DeltaColumnStats) -> None:
    """Ray distributed task to cache the delta column stats into a file system (i.e. S3).

    Args:
        stat_results_s3_bucket: The S3 bucket name
        dataset_column: Column-oriented stats for a given delta
    """
    write_manifest_stats_file(stat_results_s3_bucket, dataset_column.column, dataset_column.manifest_stats)


@ray.remote
def get_delta_stats(delta_locator: DeltaLocator,
                    columns: Optional[List[str]] = None,
                    deltacat_storage=unimplemented_deltacat_storage) -> DeltaStats:
    """Ray distributed task to compute and collect stats for a requested delta.
    If no columns are requested, stats will be computed for all columns.
    Args:
        delta_locator: A reference to the delta
        columns: Column names to specify for this delta. If not provided, all columns are considered.
        deltacat_storage: Client implementation of the DeltaCAT storage interface
    Returns:
        A delta wide stats container
    """

    manifest = deltacat_storage.get_delta_manifest(delta_locator)
    delta = Delta.of(delta_locator, None, None, None, manifest)
    return _collect_stats_by_columns(delta, columns, deltacat_storage)


@ray.remote
def get_deltas_from_range(
        source_partition_locator: PartitionLocator,
        start_position_inclusive: DeltaRange,
        end_position_inclusive: DeltaRange,
        deltacat_storage=unimplemented_deltacat_storage) -> List[Delta]:
    """Looks up deltas in the specified partition using Ray, given both starting and ending delta stream positions.

    Args:
        source_partition_locator: Reference to the partition locator tied to the given delta stream positions
        start_position_inclusive: Starting stream position of a range interval.
            Can be an int type to represent a closed bounded range, or a None type to represent unbounded infinity.
        end_position_inclusive: Ending stream position of a range interval.
            Can be an int type to represent a closed bounded range, or a None type to represent unbounded infinity.
        deltacat_storage: Client implementation of the DeltaCAT storage interface

    Returns:
        a list of delta objects
    """

    namespace, partition_values = source_partition_locator.namespace, source_partition_locator.partition_values
    table_name, table_version = source_partition_locator.table_name, source_partition_locator.table_version
    deltas_list_result = deltacat_storage.list_deltas(
        namespace,
        table_name,
        partition_values,
        table_version,
        start_position_inclusive,
        end_position_inclusive,
        ascending_order=True,
        include_manifest=False
    )
    return deltas_list_result.all_items()


def _collect_stats_by_columns(delta: Delta,
                              columns_to_compute: Optional[List[str]] = None,
                              deltacat_storage=unimplemented_deltacat_storage) -> DeltaStats:
    """Materializes one manifest entry at a time to save memory usage and calculate stats from each of its columns.
    Args:
        delta: A delta object to calculate stats for
        columns_to_compute: Columns to calculate stats for. If not provided, all columns are considered.
        deltacat_storage: Client implementation of the DeltaCAT storage interface
    Returns:
        A delta wide stats container
    """
    assert delta.manifest is not None, f"Manifest should not be missing from delta for stats calculation: {delta}"

    # Mapping of column_name -> [stats_file_idx_1, stats_file_idx_2, ... stats_file_idx_n]
    column_stats_map: Dict[str, List[Optional[StatsResult]]] = defaultdict(lambda: [None] * len(delta.manifest.entries))

    total_tables_size = 0
    for file_idx, manifest in enumerate(delta.manifest.entries):
        entry_pyarrow_table: LocalTable = \
            deltacat_storage.download_delta_manifest_entry(delta, file_idx, TableType.PYARROW, columns_to_compute)
        assert isinstance(entry_pyarrow_table, pyarrow.Table), \
            f"Stats collection is only supported for PyArrow tables, but received a table of " \
            f"type '{type(entry_pyarrow_table)}' for manifest entry {file_idx} of delta: {delta.locator}."
        total_tables_size += entry_pyarrow_table.nbytes
        if not columns_to_compute:
            columns_to_compute = entry_pyarrow_table.column_names

        for column_idx, pyarrow_column in enumerate(entry_pyarrow_table.columns):
            column_name = columns_to_compute[column_idx]
            column_stats_map[column_name][file_idx] = StatsResult.of(len(pyarrow_column), pyarrow_column.nbytes)

    # Add column-wide stats for a list of tables, these will be used for caching and retrieving later
    delta_ds_column_stats: List[DeltaColumnStats] = \
        _to_dataset_column_stats(delta.locator, columns_to_compute, column_stats_map)

    dataset_stats: DeltaStats = DeltaStats.of(delta_ds_column_stats)

    # Quick validation for calculations
    assert dataset_stats.stats.pyarrow_table_bytes == total_tables_size, \
        f"Expected the size of all PyArrow tables ({total_tables_size} bytes) " \
        f"to match the sum of each of its columns ({dataset_stats.stats.pyarrow_table_bytes} bytes)"

    return dataset_stats


def _to_dataset_column_stats(delta_locator: DeltaLocator,
                             column_names: List[str],
                             column_manifest_map: Dict[str, List[Optional[StatsResult]]]) \
        -> List[DeltaColumnStats]:
    dataset_stats: List[DeltaColumnStats] = []
    for column_name in column_names:
        column_manifest_stats = ManifestEntryStats.of(column_manifest_map[column_name], delta_locator)
        dataset_column_stats = DeltaColumnStats.of(column_name, column_manifest_stats)
        dataset_stats.append(dataset_column_stats)
    return dataset_stats