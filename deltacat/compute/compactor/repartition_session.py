import ray
import time
import logging
from deltacat import logs
from deltacat.utils.common import ReadKwargsProvider
import functools
import itertools
from deltacat.compute.compactor import (
    RoundCompletionInfo,
    SortKey,
)
from deltacat.types.media import ContentType
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
    round_robin_options_provider,
)

from deltacat.compute.compactor.model.repartition_result import RepartitionResult
from deltacat.utils.placement import PlacementGroupConfig
from typing import List, Optional, Dict, Any
from deltacat.utils.ray_utils.runtime import live_node_resource_keys
from deltacat.compute.compactor.utils import io
from deltacat.compute.compactor.utils import round_completion_file as rcf
from deltacat.compute.compactor.steps import repartition as repar
from deltacat.compute.compactor.steps.repartition import RepartitionType
from deltacat.storage import (
    Delta,
    DeltaLocator,
    PartitionLocator,
    interface as unimplemented_deltacat_storage,
)
from deltacat.utils.metrics import MetricsConfig

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


# TODO: move this repartition function to a separate module under compute
def repartition(
    source_partition_locator: PartitionLocator,
    destination_partition_locator: PartitionLocator,
    repartition_args: Any,
    repartition_completion_file_s3_url: str,
    last_stream_position_to_compact: int,
    repartition_type: RepartitionType = RepartitionType.RANGE,
    sort_keys: List[SortKey] = None,
    records_per_repartitioned_file: int = 4_000_000,
    min_file_count: int = 1000,
    min_delta_bytes: int = 200 * 2**20,
    repartitioned_file_content_type: ContentType = ContentType.PARQUET,
    enable_profiler: bool = False,
    metrics_config: Optional[MetricsConfig] = None,
    pg_config: Optional[PlacementGroupConfig] = None,
    list_deltas_kwargs: Optional[Dict[str, Any]] = None,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    **kwargs,
) -> Optional[str]:

    node_resource_keys = None
    if pg_config:  # use resource in each placement group
        cluster_resources = pg_config.resource
        cluster_cpus = cluster_resources["CPU"]
    else:  # use all cluster resource
        cluster_resources = ray.cluster_resources()
        logger.info(f"Total cluster resources: {cluster_resources}")
        logger.info(f"Available cluster resources: {ray.available_resources()}")
        cluster_cpus = int(cluster_resources["CPU"])
        logger.info(f"Total cluster CPUs: {cluster_cpus}")
        node_resource_keys = live_node_resource_keys()
        logger.info(
            f"Found {len(node_resource_keys)} live cluster nodes: "
            f"{node_resource_keys}"
        )

    # create a remote options provider to round-robin tasks across all nodes or allocated bundles
    logger.info(f"Setting round robin scheduling with node id:{node_resource_keys}")
    round_robin_opt_provider = functools.partial(
        round_robin_options_provider,
        resource_keys=node_resource_keys,
        pg_config=pg_config.opts if pg_config else None,
    )

    deltas = io._discover_deltas(
        source_partition_locator,
        None,
        deltacat_storage.get_partition(
            source_partition_locator.stream_locator,
            source_partition_locator.partition_values,
        ).stream_position,
        deltacat_storage,
        **list_deltas_kwargs,
    )

    uniform_deltas = []
    for delta in deltas:
        uniform_deltas_part = DeltaAnnotated.rebatch(
            [DeltaAnnotated.of(delta)],
            min_delta_bytes=min_delta_bytes,
            min_file_counts=min_file_count,
        )
        uniform_deltas.extend(uniform_deltas_part)

    logger.info(f"Retrieved a total of {len(uniform_deltas)} uniform deltas.")

    max_parallelism = cluster_cpus
    # create a new stream for this round
    compacted_stream_locator = destination_partition_locator.stream_locator
    stream = deltacat_storage.get_stream(
        compacted_stream_locator.namespace,
        compacted_stream_locator.table_name,
        compacted_stream_locator.table_version,
    )
    partition = deltacat_storage.stage_partition(
        stream,
        destination_partition_locator.partition_values,
    )
    new_compacted_partition_locator = partition.locator
    repar_start = time.time()
    repar_tasks_pending = invoke_parallel(
        items=uniform_deltas,
        ray_task=repar.repartition,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        repartition_type=repartition_type,
        repartition_args=repartition_args,
        max_records_per_output_file=records_per_repartitioned_file,
        destination_partition=partition,
        enable_profiler=enable_profiler,
        metrics_config=metrics_config,
        read_kwargs_provider=read_kwargs_provider,
        repartitioned_file_content_type=repartitioned_file_content_type,
        deltacat_storage=deltacat_storage,
    )
    logger.info(f"Getting {len(repar_tasks_pending)} task results...")
    repar_results: List[RepartitionResult] = ray.get(repar_tasks_pending)
    repar_results: List[Delta] = [rp.range_deltas for rp in repar_results]
    transposed = list(itertools.zip_longest(*repar_results, fillvalue=None))
    ordered_deltas: List[Delta] = [
        i for sublist in transposed for i in sublist if i is not None
    ]
    repar_end = time.time()
    logger.info(f"repartition {repar_end - repar_start} seconds")
    logger.info(f"Got {len(ordered_deltas)} task results.")
    # ordered_deltas are ordered as [cold1, cold2, coldN, hot1, hot2, hotN]
    merged_delta = Delta.merge_deltas(ordered_deltas)
    compacted_delta = deltacat_storage.commit_delta(
        merged_delta, properties=kwargs.get("properties", {})
    )
    deltacat_storage.commit_partition(partition)
    logger.info(f"Committed final delta: {compacted_delta}")
    logger.info(f"Job run completed successfully!")
    new_compacted_delta_locator = DeltaLocator.of(
        new_compacted_partition_locator,
        compacted_delta.stream_position,
    )
    bit_width_of_sort_keys = SortKey.validate_sort_keys(
        source_partition_locator,
        sort_keys,
        deltacat_storage,
    )
    repartition_completion_info = RoundCompletionInfo.of(
        last_stream_position_to_compact,
        new_compacted_delta_locator,
        None,
        bit_width_of_sort_keys,
        None,
    )
    return rcf.write_round_completion_file(
        None,
        None,
        repartition_completion_info,
        repartition_completion_file_s3_url,
    )
