import logging

import ray
import deltacat.compute.stats.utils.stats_completion_file as scf
from deltacat.aws.redshift import Manifest
from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.models.stats_completion_info import StatsCompletionInfo

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.storage import PartitionLocator, Delta, DeltaLocator
from deltacat import logs, LocalTable
from deltacat.storage import interface as unimplemented_deltacat_storage

from typing import Dict, List, Set, Tuple


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def read_cached_delta_stats(
        source_partition_locator: PartitionLocator,
        start_position_inclusive: int,
        end_position_inclusive: int,
        stat_results_s3_bucket: str,
        deltacat_storage=unimplemented_deltacat_storage) -> DeltaStatsCacheResult:

    deltas: List[Delta] = _get_deltas_from_range(source_partition_locator,
                                                 start_position_inclusive,
                                                 end_position_inclusive,
                                                 deltacat_storage)
    cache_hit_stats_completion_info, cache_miss_delta_locators = [], []

    for delta in deltas:
        delta_locator = DeltaLocator.of(delta.partition_locator, delta.stream_position)
        stats_completion_info_s3 = scf.read_stats_completion_file(stat_results_s3_bucket, delta_locator)
        if stats_completion_info_s3:
            cache_hit_stats_completion_info.append(stats_completion_info_s3)
        else:
            cache_miss_delta_locators.append(delta_locator)

    return DeltaStatsCacheResult.of(cache_hit_stats_completion_info, cache_miss_delta_locators)


@ray.remote
def cache_delta_stats(stat_results_s3_bucket: str,
                      stats_completion_info: StatsCompletionInfo):
    scf.write_stats_completion_file(stat_results_s3_bucket, stats_completion_info)


@ray.remote
def get_delta_stats(delta: DeltaLocator,
                    stat_types_to_collect: Set[StatsType],
                    deltacat_storage=unimplemented_deltacat_storage) -> StatsCompletionInfo:

    delta_stats = {}
    manifest = deltacat_storage.get_delta_manifest(delta)
    row_count, delta_bytes_pyarrow, delta_manifest_entry_stats = \
        _calculate_delta_stats(delta, manifest, deltacat_storage)

    if StatsType.ROW_COUNT in stat_types_to_collect:
        delta_stats[StatsType.ROW_COUNT.value] = row_count

    if StatsType.PYARROW_TABLE_BYTES in stat_types_to_collect:
        delta_stats[StatsType.PYARROW_TABLE_BYTES.value] = delta_bytes_pyarrow

    delta_stats = StatsResult(delta_stats)

    return StatsCompletionInfo.of(delta, delta_stats, delta_manifest_entry_stats)


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


def _calculate_delta_stats(delta: DeltaLocator,
                           manifest: Manifest,
                           deltacat_storage=unimplemented_deltacat_storage) -> Tuple[int, int, Dict[int, StatsResult]]:
    total_rows, total_pyarrow_bytes = 0, 0
    delta_manifest_entry_stats: Dict[int, StatsResult] = {}

    for file_idx, manifest in enumerate(manifest.entries):
        entry_pyarrow_table: LocalTable = deltacat_storage.download_delta_manifest_entry(delta, file_idx)
        entry_rows, entry_pyarrow_bytes = len(entry_pyarrow_table), entry_pyarrow_table.nbytes
        delta_manifest_entry_stats[file_idx] = StatsResult.of(entry_rows, entry_pyarrow_bytes)
        total_rows += len(entry_pyarrow_table)
        total_pyarrow_bytes += entry_pyarrow_table.nbytes

    return total_rows, total_pyarrow_bytes, delta_manifest_entry_stats


