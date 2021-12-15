import logging

import pyarrow
import ray
from collections import defaultdict

import deltacat.compute.stats.utils.stats_completion_file as scf
from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.models.stats_completion_info import StatsCompletionInfo
from deltacat.compute.stats.models.dataset_column_stats import DatasetColumnStats
from deltacat.compute.stats.models.dataset_stats import DatasetStats, DatasetStatsCacheMiss

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.storage import PartitionLocator, Delta, DeltaLocator
from deltacat import logs, LocalTable, TableType
from deltacat.storage import interface as unimplemented_deltacat_storage

from typing import Dict, List, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def read_cached_delta_stats(delta: Delta,
                            columns_to_fetch: List[str],
                            stat_results_s3_bucket: str) -> DeltaStatsCacheResult:

    delta_locator = DeltaLocator.of(delta.partition_locator, delta.stream_position)
    column_stats_completion_info: List[DatasetColumnStats] = \
        scf.read_stats_completion_file_by_columns(stat_results_s3_bucket, columns_to_fetch, delta_locator)

    found_columns: List[DatasetColumnStats] = []
    missed_columns: List[str] = []
    for column_stats in column_stats_completion_info:
        if column_stats.manifest_stats:
            found_columns.append(column_stats)
        else:
            missed_columns.append(column_stats.column)

    found_stats: Optional[DatasetStats] = DatasetStats.of(found_columns) if found_columns else None
    missed_stats: Optional[DatasetStatsCacheMiss] = DatasetStatsCacheMiss(missed_columns, delta.locator) \
        if missed_columns else None

    return DeltaStatsCacheResult.of(found_stats, missed_stats)


@ray.remote
def cache_delta_column_stats(stat_results_s3_bucket: str,
                             dataset_column: DatasetColumnStats):
    scf.write_stats_completion_file(stat_results_s3_bucket, dataset_column.column, dataset_column.manifest_stats)


@ray.remote
def get_delta_stats(delta_locator: DeltaLocator,
                    columns: Optional[List[str]] = None,
                    deltacat_storage=unimplemented_deltacat_storage) -> DatasetStats:

    manifest = deltacat_storage.get_delta_manifest(delta_locator)
    delta = Delta.of(delta_locator, None, None, None, manifest)
    return _collect_stats_by_columns(delta, columns, deltacat_storage)


@ray.remote
def get_deltas_from_range(*args, **kwargs) -> List[Delta]:
    return _get_deltas_from_range(*args, **kwargs)


def _get_deltas_from_range(
        source_partition_locator: PartitionLocator,
        start_position_inclusive: int,
        end_position_inclusive: int,
        deltacat_storage=unimplemented_deltacat_storage) -> List[Delta]:

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
                              deltacat_storage=unimplemented_deltacat_storage) -> DatasetStats:
    """
    Materializes one manifest entry at a time to save memory usage and
    calculate statistics from each of its columns.
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
        if columns_to_compute is None:
            columns_to_compute = entry_pyarrow_table.column_names

        for column_idx, pyarrow_column in enumerate(entry_pyarrow_table.columns):
            column_name = columns_to_compute[column_idx]
            column_stats_map[column_name][file_idx] = StatsResult.of(len(pyarrow_column), pyarrow_column.nbytes)

    # Add column-wide stats for a list of tables, these will be used for caching and retrieving later
    delta_ds_column_stats: List[DatasetColumnStats] = \
        _to_dataset_column_stats(delta.locator, columns_to_compute, column_stats_map)

    dataset_stats: DatasetStats = DatasetStats.of(delta_ds_column_stats)

    # Quick validation for calculations
    assert dataset_stats.stats.pyarrow_table_bytes == total_tables_size, \
        f"Expected the size of all PyArrow tables ({total_tables_size} bytes) " \
        f"to match the sum of each of its columns ({dataset_stats.stats.pyarrow_table_bytes} bytes)"

    return dataset_stats


def _to_dataset_column_stats(delta_locator: DeltaLocator,
                             column_names: List[str],
                             column_manifest_map: Dict[str, List[Optional[StatsResult]]]) \
        -> List[DatasetColumnStats]:
    dataset_stats: List[DatasetColumnStats] = []
    for column_name in column_names:
        column_manifest_stats = StatsCompletionInfo.of(column_manifest_map[column_name], delta_locator)
        dataset_column_stats = DatasetColumnStats.of(column_name, column_manifest_stats)
        dataset_stats.append(dataset_column_stats)
    return dataset_stats
