import ray
import time
import logging
from deltacat import logs
from deltacat.utils.common import ReadKwargsProvider
import sungate as sg
import functools
import itertools
from deltacat.compute.compactor import (
    RoundCompletionInfo,
    SortKey,
)
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
    round_robin_options_provider,
)
from deltacat.compute.compactor.model.repartition_result import RePartitionResult
from deltacat.utils.placement import PlacementGroupConfig
from typing import List, Optional, Dict, Any
from deltacat.utils.ray_utils.runtime import live_node_resource_keys
from deltacat.compute.compactor.utils import io
from deltacat.compute.compactor.utils import round_completion_file as rcf
from deltacat.compute.compactor.steps import repartition as repar
from deltacat.storage import (
    Delta,
    DeltaLocator,
    PartitionLocator,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))
deltacat_storage = sg.andes


def repartition(
    source_partition_locator: PartitionLocator,
    destination_partition_locator: PartitionLocator,
    repartition_type: str,
    repartition_args: Any,
    compaction_artifact_s3_bucket: str,
    last_stream_position_to_compact: int,
    rebase_source_partition_locator: Optional[PartitionLocator] = None,
    rebase_source_partition_high_watermark: Optional[int] = None,
    sort_keys: List[SortKey] = None,
    pg_config: Optional[PlacementGroupConfig] = None,
    list_deltas_kwargs: Optional[Dict[str, Any]] = None,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
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

    (deltas, _,) = io.discover_deltas(
        source_partition_locator,
        None,
        last_stream_position_to_compact,
        destination_partition_locator,
        rebase_source_partition_locator,
        rebase_source_partition_high_watermark,
        deltacat_storage,
        **list_deltas_kwargs,
    )

    uniform_deltas = []
    for delta in deltas:
        # limit the input deltas to fit on this cluster and convert them to
        # annotated deltas of equivalent size for easy parallel distribution
        uniform_deltas_part = DeltaAnnotated.rebatch(
            [DeltaAnnotated.of(delta)],
            200 * 2**20,  # 200 MiB compressed data per task,
            min_file_counts=1000,
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
        destination_partition=partition,
        enable_profiler=False,
        metrics_config=None,
        read_kwargs_provider=read_kwargs_provider,
        deltacat_storage=deltacat_storage,
    )
    logger.info(f"Getting {len(repar_tasks_pending)} task results...")
    repar_results: List[RePartitionResult] = ray.get(repar_tasks_pending)
    repar_results: List[Delta] = [rp.range_deltas for rp in repar_results]
    # Transpose the list, filling in with None for shorter lists
    transposed = list(itertools.zip_longest(*repar_results, fillvalue=None))
    # Flatten the list and remove None values
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
    new_round_completion_info = RoundCompletionInfo.of(
        last_stream_position_to_compact,
        new_compacted_delta_locator,
        None,
        bit_width_of_sort_keys,
        None,
    )
    rcf_source_partition_locator = source_partition_locator
    round_completion_file_s3_url = None
    round_completion_file_s3_url = rcf.write_round_completion_file(
        compaction_artifact_s3_bucket,
        rcf_source_partition_locator,
        new_round_completion_info,
    )
    return round_completion_file_s3_url
