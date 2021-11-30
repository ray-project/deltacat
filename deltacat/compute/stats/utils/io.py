import logging

import ray
import deltacat.compute.stats.utils.stats_completion_file as scf
from deltacat.compute.stats.models.stats_completion_info import StatsCompletionInfo

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.constants import PYARROW_INFLATION_MULTIPLIER
from deltacat.storage import PartitionLocator, Delta, DeltaLocator
from deltacat import logs
from deltacat.storage import interface as unimplemented_deltacat_storage

from typing import Dict, List, Set, Optional

from ray.types import ObjectRef

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def discover_deltas_stats(
        source_partition_locator: PartitionLocator,
        start_position_inclusive: int,
        end_position_inclusive: int,
        stat_types_to_collect: Set[StatsType],
        stat_results_s3_bucket: str = None,
        deltacat_storage=unimplemented_deltacat_storage) -> Dict[int, StatsResult]:

    processed_stats: Dict[int, StatsResult] = {}
    pending_stats: List[ObjectRef[StatsCompletionInfo]] = []
    deltas: List[Delta] = _get_deltas_from_range(
        source_partition_locator,
        start_position_inclusive,
        end_position_inclusive,
        deltacat_storage
    )

    for delta in deltas:
        stats_completion_info_s3: Optional[StatsCompletionInfo] = None

        # lookup delta stats in s3
        if stat_results_s3_bucket:
            delta_locator = DeltaLocator.of(source_partition_locator, delta.stream_position)
            stats_completion_info_s3 = scf.read_stats_completion_file(stat_results_s3_bucket, delta_locator)

        if stats_completion_info_s3:
            processed_stats[delta.stream_position] = stats_completion_info_s3.stats_result
        else:
            pending_stats.append(_get_delta_stats.remote(delta, stat_types_to_collect, deltacat_storage))

    for promise in pending_stats:
        stats_completion_info: StatsCompletionInfo = ray.get(promise)

        # cache delta stats in s3
        if stat_results_s3_bucket:
            scf.write_stats_completion_file(stat_results_s3_bucket, stats_completion_info)

        processed_stats[stats_completion_info.high_watermark] = stats_completion_info.stats_result

    return processed_stats


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


@ray.remote
def _get_delta_stats(delta: Delta,
                     stat_types_to_collect: Set[StatsType],
                     deltacat_storage) -> StatsCompletionInfo:
    delta_bytes, delta_bytes_pyarrow, row_count = 0, 0, 0
    manifest = deltacat_storage.get_delta_manifest(delta)
    delta.manifest = manifest
    manifest_entries = delta.manifest.entries
    delta_stats = {}
    for entry in manifest_entries:
        row_count += entry.meta.record_count
        delta_bytes += entry.meta.content_length
        delta_bytes_pyarrow = delta_bytes * PYARROW_INFLATION_MULTIPLIER

    if StatsType.ROW_COUNT in stat_types_to_collect:
        delta_stats[StatsType.ROW_COUNT.value] = row_count

    if StatsType.PYARROW_TABLE_BYTES in stat_types_to_collect:
        delta_stats[StatsType.PYARROW_TABLE_BYTES.value] = delta_bytes_pyarrow

    return StatsCompletionInfo.of(delta.stream_position, delta.locator, StatsResult(delta_stats))


