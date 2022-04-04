import ray
from typing import Dict, Set, Tuple, List, Optional

from deltacat.compute.stats.models.delta_stats import DeltaStats
from ray.types import ObjectRef

from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.compute.stats.utils.io import read_cached_delta_stats, cache_delta_column_stats, get_delta_stats, \
    get_deltas_from_range
from deltacat.compute.stats.utils.intervals import merge_intervals, DeltaRange

from deltacat.storage import PartitionLocator, DeltaLocator, Delta
from deltacat.storage import interface as unimplemented_deltacat_storage


# TODO (ricmiyam): Decouple DeltaCAT from S3-based paths
# TODO (ricmiyam): Determine cache eviction policy


def collect(
        source_partition_locator: PartitionLocator,
        delta_stream_position_range_set: Optional[Set[DeltaRange]] = None,
        columns: Optional[List[str]] = None,
        stat_results_s3_bucket: Optional[str] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> Dict[int, DeltaStats]:
    """Collects statistics on deltas, given a set of delta stream position ranges.

    Example:
        >>> collect(locator, set((1, 5), (4, 8), (13, 16)))
        {
            1: DeltaStats(),  # DeltaStats for stream positions 1 - 8
            13: DeltaStats()  # DeltaStats for stream positions 13 - 16
        }

    Args:
        source_partition_locator: Reference to the partition locator tied to the given delta stream positions
        delta_stream_position_range_set: A set of intervals with an int type representing finite,
            closed bounded values, and a None type representing unbounded infinity.
        columns: Columns can be optionally included to collect stats on specific columns.
            By default, all columns will be calculated.
        stat_results_s3_bucket: Used as a cache file storage for computed delta stats
        deltacat_storage: Client implementation of the DeltaCAT storage interface

    Returns:
        A mapping of stream positions to their corresponding delta stats.
    """
    if delta_stream_position_range_set is None:
        delta_stream_position_range_set = {(None, None)}
    delta_stream_range_stats: Dict[int, DeltaStats] = {}
    delta_range_lookup_pending: List[ObjectRef[List[Delta]]] = []

    if not columns:
        columns = deltacat_storage.get_table_version_column_names(source_partition_locator.namespace,
                                                                  source_partition_locator.table_name,
                                                                  source_partition_locator.table_version)
    for range_pair in merge_intervals(delta_stream_position_range_set):
        begin, end = range_pair
        promise: ObjectRef[List[Delta]] = get_deltas_from_range.remote(source_partition_locator, begin, end,
                                                                       deltacat_storage)
        delta_range_lookup_pending.append(promise)

    delta_list_by_ranges: List[List[Delta]] = ray.get(delta_range_lookup_pending)
    deltas = [delta for delta_list in delta_list_by_ranges for delta in delta_list]

    delta_stats_processed_list: List[DeltaStats] = _collect_stats_from_deltas(deltas,
                                                                              columns,
                                                                              stat_results_s3_bucket,
                                                                              deltacat_storage)

    for delta_column_stats in delta_stats_processed_list:
        assert len(delta_column_stats.column_stats) > 0, \
            f"Expected columns of `{delta_column_stats}` to be non-empty"
        stream_position = delta_column_stats.column_stats[0].manifest_stats.delta_locator.stream_position
        delta_stream_range_stats[stream_position] = delta_column_stats

    return delta_stream_range_stats


def collect_from_deltas(
        deltas: List[Delta],
        stat_types: Set[StatsType],
        columns: Optional[List[str]] = None,
        stat_results_s3_bucket: Optional[str] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> StatsResult:
    """
    Variant of the `collect` function that takes a list of deltas and computes
    the aggregate of all the delta stats.
    """
    if columns is None and deltas:
        delta_locator: DeltaLocator = deltas[0].locator
        columns = deltacat_storage.get_table_version_column_names(delta_locator.namespace,
                                                                  delta_locator.table_name,
                                                                  delta_locator.table_version)

    delta_stats_processed_list: List[DeltaStats] = \
        _collect_stats_from_deltas(deltas, columns, stat_results_s3_bucket, deltacat_storage)

    return StatsResult.merge([delta_ds.stats for delta_ds in delta_stats_processed_list], stat_types)


def _collect_stats_from_deltas(
        deltas: List[Delta],
        columns: Optional[List[str]] = None,
        stat_results_s3_bucket: Optional[str] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> List[DeltaStats]:
    delta_cache_lookup_pending: List[ObjectRef[DeltaStatsCacheResult]] = []
    delta_stats_compute_pending: List[ObjectRef[DeltaStats]] = []

    for delta in deltas:
        if stat_results_s3_bucket:
            promise: ObjectRef[DeltaStatsCacheResult] = \
                read_cached_delta_stats.remote(delta, columns, stat_results_s3_bucket)
            delta_cache_lookup_pending.append(promise)
            continue

        delta_stats_compute_pending.append(get_delta_stats.remote(delta.locator, columns, deltacat_storage))

    return _process_stats(delta_cache_lookup_pending, delta_stats_compute_pending,
                          stat_results_s3_bucket, deltacat_storage)


def _process_stats(
        delta_cache_lookup_pending: List[ObjectRef[DeltaStatsCacheResult]],
        delta_stats_compute_pending: List[ObjectRef[DeltaStats]],
        stat_results_s3_bucket: Optional[str] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> List[DeltaStats]:
    if stat_results_s3_bucket:
        delta_stats_processed_list: List[DeltaStats] = _resolve_pending_stats_and_cache(delta_cache_lookup_pending,
                                                                                        stat_results_s3_bucket,
                                                                                        deltacat_storage)
    else:
        delta_stats_processed_list: List[DeltaStats] = _resolve_pending_stats(delta_stats_compute_pending)

    return delta_stats_processed_list


def _resolve_pending_stats_and_cache(delta_cache_lookup_pending: List[ObjectRef[DeltaStatsCacheResult]],
                                     stat_results_s3_bucket: str,
                                     deltacat_storage) -> List[DeltaStats]:
    delta_stats_cached_list, delta_stats_pending_list = \
        _get_cached_and_pending_stats(delta_cache_lookup_pending, deltacat_storage)
    delta_stats_resolved_list: List[DeltaStats] = _resolve_pending_stats(delta_stats_pending_list)

    # Cache the stats into the file store
    delta_stats_to_cache: List[ObjectRef] = [cache_delta_column_stats.remote(stat_results_s3_bucket, dcs)
                                             for dataset_stats in delta_stats_resolved_list
                                             for dcs in dataset_stats.column_stats]
    ray.get(delta_stats_to_cache)

    return [*delta_stats_cached_list, *delta_stats_resolved_list]


def _get_cached_and_pending_stats(discover_deltas_pending: List[ObjectRef[DeltaStatsCacheResult]],
                                  deltacat_storage=unimplemented_deltacat_storage) \
        -> Tuple[List[DeltaStats], List[ObjectRef[DeltaStats]]]:
    """
    Returns a tuple of a list of delta stats fetched from the cache, and a list of Ray tasks which will
    calculate the stats for deltas on cache miss.
    """
    delta_stats_processed: List[DeltaStats] = []
    delta_stats_pending: List[ObjectRef[DeltaStats]] = []
    while discover_deltas_pending:
        ready, discover_deltas_pending = ray.wait(discover_deltas_pending)

        cached_results: List[DeltaStatsCacheResult] = ray.get(ready)
        for cached_result in cached_results:
            if cached_result.hits:
                delta_stats_processed.append(cached_result.hits)

            if cached_result.misses:
                missed_column_names: List[str] = cached_result.misses.column_names
                delta_locator: DeltaLocator = cached_result.misses.delta_locator
                delta_stats_pending.append(get_delta_stats.remote(delta_locator, missed_column_names, deltacat_storage))

    return delta_stats_processed, delta_stats_pending


def _resolve_pending_stats(delta_stats_pending_list: List[ObjectRef[DeltaStats]]) \
        -> List[DeltaStats]:
    delta_stats_processed_list: List[DeltaStats] = []
    while delta_stats_pending_list:
        ready, delta_stats_pending_list = ray.wait(delta_stats_pending_list)
        processed_stats_batch: List[DeltaStats] = ray.get(ready)
        delta_stats_processed_list.extend(processed_stats_batch)

    return delta_stats_processed_list
