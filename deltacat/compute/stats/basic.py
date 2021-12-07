import ray
from typing import Dict, Set, Tuple, List

from ray.types import ObjectRef

from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.models.stats_completion_info import StatsCompletionInfo
from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.compute.stats.utils.io import read_cached_delta_stats, cache_delta_stats, \
    get_delta_stats
from deltacat.compute.stats.utils.intervals import merge_intervals

from deltacat.storage import PartitionLocator, DeltaLocator
from deltacat.storage import interface as unimplemented_deltacat_storage


def collect(
        source_partition_locator: PartitionLocator,
        delta_stream_position_range_set: Set[Tuple[int, int]],  # TODO: R&D on RangeSet impl
        stat_types_to_collect: Set[StatsType],
        stat_results_s3_bucket: str = None,  # TODO: Decouple DeltaCAT from S3-based paths
        deltacat_storage=unimplemented_deltacat_storage) -> Dict[int, StatsResult]:
    delta_stream_range_stats: Dict[int, StatsResult] = {}
    discover_deltas_pending: List[ObjectRef] = []
    for range_pair in merge_intervals(delta_stream_position_range_set):
        begin_pos, end_pos = range_pair
        promise: ObjectRef[StatsCompletionInfo] = read_cached_delta_stats.remote(
            source_partition_locator,
            begin_pos,
            end_pos,
            stat_results_s3_bucket,
            deltacat_storage
        )
        discover_deltas_pending.append(promise)

    delta_stats_cached_list, delta_stats_pending_list = _get_cached_and_pending_stats(discover_deltas_pending,
                                                                                      stat_types_to_collect,
                                                                                      deltacat_storage)
    delta_stats_resolved_list: List[StatsCompletionInfo] = _resolve_pending_stats(delta_stats_pending_list)

    # Sequentially cache the stats into the file store
    delta_stats_to_cache: List[ObjectRef] = \
        [cache_delta_stats.remote(stat_results_s3_bucket, _) for _ in delta_stats_resolved_list]
    ray.get(delta_stats_to_cache)

    delta_stats_processed_list: List[StatsCompletionInfo] = [*delta_stats_cached_list, *delta_stats_resolved_list]
    for stats_info in delta_stats_processed_list:
        stream_position = stats_info.delta_locator.stream_position
        delta_stream_range_stats[stream_position] = stats_info.delta_stats

    return delta_stream_range_stats


def _get_cached_and_pending_stats(discover_deltas_pending: List[ObjectRef],
                                  stat_types_to_collect: Set[StatsType],
                                  deltacat_storage=unimplemented_deltacat_storage) \
        -> Tuple[List[StatsCompletionInfo], List[ObjectRef]]:
    """
    Returns a tuple of a list of delta stats fetched from the cache, and a list of Ray tasks which will
    calculate the stats for deltas on cache miss.
    """
    delta_stats_processed: List[StatsCompletionInfo] = []
    delta_stats_pending: List[ObjectRef] = []
    while discover_deltas_pending:
        ready, discover_deltas_pending = ray.wait(discover_deltas_pending)

        cached_results: List[DeltaStatsCacheResult] = ray.get(ready)
        missed_deltas: List[DeltaLocator] = []

        for cached_result in cached_results:
            stats, delta_locators = cached_result.stats, cached_result.cache_miss_delta_locators
            delta_stats_processed.extend(stats)
            missed_deltas.extend(delta_locators)

        for delta_locator in missed_deltas:
            delta_stats_pending.append(get_delta_stats.remote(
                delta_locator,
                stat_types_to_collect,
                deltacat_storage
            ))

    return delta_stats_processed, delta_stats_pending


def _resolve_pending_stats(delta_stats_pending_list: List[ObjectRef]) -> List[StatsCompletionInfo]:
    delta_stats_processed_list: List[StatsCompletionInfo] = []
    while delta_stats_pending_list:
        ready, delta_stats_pending_list = ray.wait(delta_stats_pending_list)
        processed_stats_batch: List[StatsCompletionInfo] = ray.get(ready)
        for processed_stats in processed_stats_batch:
            delta_stats_processed_list.append(processed_stats)

    return delta_stats_processed_list
