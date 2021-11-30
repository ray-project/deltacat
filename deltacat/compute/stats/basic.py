from typing import Dict, Set, Tuple

from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.stats.types import StatsType
from deltacat.compute.stats.utils.io import discover_deltas_stats

from deltacat.storage import PartitionLocator
from deltacat.storage import interface as unimplemented_deltacat_storage


def collect(
        source_partition_locator: PartitionLocator,
        delta_stream_position_range_set: Set[Tuple[int, int]],  # TODO: R&D on RangeSet impl
        stat_types_to_collect: Set[StatsType],
        stat_results_s3_bucket: str = None,  # TODO: Decouple DeltaCAT from S3-based paths
        deltacat_storage=unimplemented_deltacat_storage
) -> Dict[int, StatsResult]:
    delta_stream_range_stats: Dict[int, StatsResult] = {}
    for range_pair in delta_stream_position_range_set:
        begin_pos, end_pos = range_pair
        delta_stats: Dict[int, StatsResult] = discover_deltas_stats(
            source_partition_locator,
            begin_pos,
            end_pos,
            stat_types_to_collect,
            stat_results_s3_bucket,
            deltacat_storage
        )
        delta_stream_range_stats.update(delta_stats)

    return delta_stream_range_stats


