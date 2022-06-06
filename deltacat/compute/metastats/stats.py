import ray
import logging
from typing import Dict, Set, Tuple, List, Optional, Any
from collections import defaultdict

from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.compute.stats.models.stats_result import StatsResult
from ray.types import ObjectRef

from deltacat import logs
from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.utils.io import cache_delta_column_stats, get_delta_stats
from deltacat.compute.metastats.utils.io import cache_inflation_rate_data_for_delta_stats_ready

from deltacat.storage import PartitionLocator, DeltaLocator, Delta
from deltacat.storage import interface as unimplemented_deltacat_storage

from deltacat.aws.clients import client_cache
from deltacat.aws import s3u as s3_utils


from deltacat.compute.stats.models.manifest_entry_stats import ManifestEntryStats
from deltacat.compute.stats.models.delta_column_stats import DeltaColumnStats

from deltacat.compute.compactor import DeltaAnnotated

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def start_stats_collection(batched_delta_stats_compute_list: List[DeltaAnnotated],
                           columns: List[str],
                           stat_results_s3_bucket: Optional[str]=None,
                           metastats_results_s3_bucket: Optional[str]=None,
                           deltacat_storage=unimplemented_deltacat_storage) -> Dict[str, List[DeltaStats]]:
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
            metastats_results_s3_bucket: Used as cache file storage for inflation rate meta stats
            deltacat_storage: Client implementation of the DeltaCAT storage interface
        Returns:
            A mapping of stream positions to their corresponding delta stats.
    """
    # TODO: Add CompactionEventDispatcher for stats collection started event
    delta_stats_compute_pending: List[ObjectRef[Dict[str, List[StatsResult, int]]]] = []

    for batched_deltas in batched_delta_stats_compute_list:
        delta_stats_compute_pending.append(get_delta_stats.remote(batched_deltas, columns, deltacat_storage))

    logger.info(f"List of deltas to collect stats {batched_delta_stats_compute_list}")
    column_stats_map = _process_stats(delta_stats_compute_pending)

    if not batched_delta_stats_compute_list:
        logger.info("No new delta need stats collection")
    else:
        stats_res_list = resolve_annotated_delta_stats_to_original_deltas_stats(column_stats_map, columns, batched_delta_stats_compute_list[0])

        _cache_delta_res_to_s3(stat_results_s3_bucket, stats_res_list)
        delta_stream_range_stats: Dict[int, DeltaStats] = {}
        for delta_column_stats in stats_res_list:
            assert len(delta_column_stats.column_stats) > 0, \
                f"Expected columns of `{delta_column_stats}` to be non-empty"
            stream_position = delta_column_stats.column_stats[0].manifest_stats.delta_locator.stream_position
            delta_stream_range_stats[stream_position] = delta_column_stats

        base_path = s3_utils.parse_s3_url(metastats_results_s3_bucket).url
        inflation_rate_stats_s3_url = f"{base_path}/inflation-rates.json"
        cache_inflation_rate_data_for_delta_stats_ready(delta_stream_range_stats, inflation_rate_stats_s3_url,
                                                         deltacat_storage)
        # TODO: Add CompactionEventDispatcher for stats collection completed event
        return delta_stream_range_stats


def _get_account_id() -> str:
    client = client_cache("sts", None)
    account_id = client.get_caller_identity()["Account"]
    return account_id


def _process_stats(delta_stats_compute_pending: List[ObjectRef[DeltaStats]]) -> List[DeltaStats]:
    delta_stats_processed_list: List[DeltaStats] = _resolve_pending_stats(delta_stats_compute_pending)

    return delta_stats_processed_list


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


def _resolve_pending_stats(delta_stats_pending_list: List[ObjectRef[DeltaStats]]) -> List[DeltaStats]:
    delta_stats_processed_list: List[DeltaStats] = []

    while delta_stats_pending_list:
        ready, delta_stats_pending_list = ray.wait(delta_stats_pending_list)
        processed_stats_batch: List[DeltaStats] = ray.get(ready)
        delta_stats_processed_list.extend(processed_stats_batch)
    logger.info(f"List of processed delta stats {delta_stats_processed_list}")

    return delta_stats_processed_list


def _cache_delta_res_to_s3(stat_results_s3_bucket,
                           delta_stats_processed_list):
    if stat_results_s3_bucket:
        # Cache the stats into the file store
        delta_stats_to_cache: List[ObjectRef] = [cache_delta_column_stats.remote(stat_results_s3_bucket, dcs)
                                                 for dataset_stats in delta_stats_processed_list
                                                 for dcs in dataset_stats.column_stats]
        ray.get(delta_stats_to_cache)


def resolve_annotated_delta_stats_to_original_deltas_stats(column_stats_map, column_names, delta_annotated) -> \
List[DeltaStats]:

    partition_values = delta_annotated["deltaLocator"]["partitionLocator"]["partitionValues"]
    partition_id = delta_annotated["deltaLocator"]["partitionLocator"]["partitionId"]
    stream_locator = delta_annotated["deltaLocator"]["partitionLocator"]["streamLocator"]
    partition_locator = PartitionLocator.of(stream_locator, partition_values, partition_id)

    # Dict[stream_position: List[StatsResult]]
    manifest_column_stats_list = defaultdict(lambda: [])
    for i in range(len(column_stats_map)):
        for column_name in column_names:
            for j in range(len(column_stats_map[i][column_name])):
                manifest_column_stats_list[column_stats_map[i][column_name][j][1]].append(
                    [column_stats_map[i][column_name][j][0], column_name])

    stats_res: List[DeltaStats] = []
    for key, value in manifest_column_stats_list.items():
        delta_locator = DeltaLocator.of(partition_locator, key)
        # Dict[column_name: List[StatsResult]]
        manifest_stats_list = defaultdict(lambda: [])
        for manifest_stat in value:
            manifest_stats_list[manifest_stat[1]].append(manifest_stat[0])
        delta_ds_column_stats: List[DeltaColumnStats] = []
        for column_name, column_manifest_stats_list in manifest_stats_list.items():

            column_manifest_stats = ManifestEntryStats.of(column_manifest_stats_list, delta_locator)
            dataset_column_stats = DeltaColumnStats.of(column_name, column_manifest_stats)
            delta_ds_column_stats.append(dataset_column_stats)

        dataset_stats: DeltaStats = DeltaStats.of(delta_ds_column_stats)
        stats_res.append(dataset_stats)

    return stats_res