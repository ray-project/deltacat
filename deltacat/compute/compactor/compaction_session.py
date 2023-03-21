import importlib
from contextlib import nullcontext
import functools
import logging
import ray
from deltacat import logs
import pyarrow as pa
from deltacat.compute.compactor import (
    PyArrowWriteResult,
    RoundCompletionInfo,
    SortKey,
)
from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.storage import (
    Delta,
    DeltaLocator,
    Partition,
    PartitionLocator,
    interface as unimplemented_deltacat_storage,
)
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
    round_robin_options_provider,
)
from deltacat.utils.ray_utils.runtime import live_node_resource_keys
from deltacat.compute.compactor.steps import dedupe as dd
from deltacat.compute.compactor.steps import hash_bucket as hb
from deltacat.compute.compactor.steps import materialize as mat
from deltacat.compute.compactor.utils import io
from deltacat.compute.compactor.utils import round_completion_file as rcf

from deltacat.types.media import ContentType
from deltacat.utils.placement import PlacementGroupConfig
from typing import List, Set, Optional, Tuple, Dict, Any
from collections import defaultdict
from deltacat.utils.metrics import MetricsConfig

if importlib.util.find_spec("memray"):
    import memray


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def check_preconditions(
    source_partition_locator: PartitionLocator,
    destination_partition_locator: PartitionLocator,
    sort_keys: List[SortKey],
    max_records_per_output_file: int,
    new_hash_bucket_count: Optional[int],
    deltacat_storage=unimplemented_deltacat_storage,
) -> int:

    assert (
        source_partition_locator.partition_values
        == destination_partition_locator.partition_values
    ), (
        "In-place compaction must use the same partition values for the "
        "source and destination."
    )
    assert (
        max_records_per_output_file >= 1
    ), "Max records per output file must be a positive value"
    if new_hash_bucket_count is not None:
        assert (
            new_hash_bucket_count >= 1
        ), "New hash bucket count must be a positive value"
    return SortKey.validate_sort_keys(
        source_partition_locator,
        sort_keys,
        deltacat_storage,
    )


def compact_partition(
    source_partition_locator: PartitionLocator,
    destination_partition_locator: PartitionLocator,
    primary_keys: Set[str],
    compaction_artifact_s3_bucket: str,
    last_stream_position_to_compact: int,
    *,
    hash_bucket_count: Optional[int] = None,
    sort_keys: List[SortKey] = None,
    records_per_compacted_file: int = 4_000_000,
    input_deltas_stats: Dict[int, DeltaStats] = None,
    min_hash_bucket_chunk_size: int = 0,
    compacted_file_content_type: ContentType = ContentType.PARQUET,
    pg_config: Optional[PlacementGroupConfig] = None,
    schema_on_read: Optional[
        pa.schema
    ] = None,  # TODO (ricmiyam): Remove this and retrieve schema from storage API
    rebase_source_partition_locator: Optional[PartitionLocator] = None,
    rebase_source_partition_high_watermark: Optional[int] = None,
    enable_profiler: Optional[bool] = False,
    metrics_config: Optional[MetricsConfig] = None,
    list_deltas_kwargs: Optional[Dict[str, Any]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    **kwargs,
) -> Optional[str]:

    if not importlib.util.find_spec("memray"):
        logger.info(f"memray profiler not available, disabling all profiling")
        enable_profiler = False

    # memray official documentation link:
    # https://bloomberg.github.io/memray/getting_started.html
    with memray.Tracker(
        f"compaction_partition.bin"
    ) if enable_profiler else nullcontext():
        partition = None
        new_rcf_s3_url = None
        (new_partition, new_rci, new_rcf_s3_url,) = _execute_compaction_round(
            source_partition_locator,
            destination_partition_locator,
            primary_keys,
            compaction_artifact_s3_bucket,
            last_stream_position_to_compact,
            hash_bucket_count,
            sort_keys,
            records_per_compacted_file,
            input_deltas_stats,
            min_hash_bucket_chunk_size,
            compacted_file_content_type,
            pg_config,
            schema_on_read,
            rebase_source_partition_locator,
            rebase_source_partition_high_watermark,
            enable_profiler,
            metrics_config,
            list_deltas_kwargs,
            deltacat_storage,
            **kwargs,
        )
        if new_partition:
            partition = new_partition

        logger.info(
            f"Partition-{source_partition_locator.partition_values}-> Compaction session data processing completed"
        )
        if partition:
            logger.info(f"Committing compacted partition to: {partition.locator}")
            partition = deltacat_storage.commit_partition(partition)
            logger.info(f"Committed compacted partition: {partition}")
        logger.info(f"Completed compaction session for: {source_partition_locator}")
        return new_rcf_s3_url


def _execute_compaction_round(
    source_partition_locator: PartitionLocator,
    destination_partition_locator: PartitionLocator,
    primary_keys: Set[str],
    compaction_artifact_s3_bucket: str,
    last_stream_position_to_compact: int,
    hash_bucket_count: Optional[int],
    sort_keys: List[SortKey],
    records_per_compacted_file: int,
    input_deltas_stats: Dict[int, DeltaStats],
    min_hash_bucket_chunk_size: int,
    compacted_file_content_type: ContentType,
    pg_config: Optional[PlacementGroupConfig],
    schema_on_read: Optional[pa.schema],
    rebase_source_partition_locator: Optional[PartitionLocator],
    rebase_source_partition_high_watermark: Optional[int],
    enable_profiler: Optional[bool],
    metrics_config: Optional[MetricsConfig],
    list_deltas_kwargs=Optional[Dict[str, Any]],
    deltacat_storage=unimplemented_deltacat_storage,
    **kwargs,
) -> Tuple[Optional[Partition], Optional[RoundCompletionInfo], Optional[str]]:

    if not primary_keys:
        # TODO (pdames): run simple rebatch to reduce all deltas into 1 delta
        #  with normalized manifest entry sizes
        raise NotImplementedError(
            "Compaction only supports tables with 1 or more primary keys"
        )
    if sort_keys is None:
        sort_keys = []
    # TODO (pdames): detect and handle schema evolution (at least ensure that
    #  we don't recompact simple backwards-compatible changes like constraint
    #  widening and null column additions).
    # TODO (pdames): detect and optimize in-place compaction

    # check preconditions before doing any computationally expensive work
    bit_width_of_sort_keys = check_preconditions(
        source_partition_locator,
        destination_partition_locator,
        sort_keys,
        records_per_compacted_file,
        hash_bucket_count,
        deltacat_storage,
    )

    # sort primary keys to produce the same pk digest regardless of input order
    primary_keys = sorted(primary_keys)

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

    # set max task parallelism equal to total cluster CPUs...
    # we assume here that we're running on a fixed-size cluster - this
    # assumption could be removed but we'd still need to know the maximum
    # "safe" number of parallel tasks that our autoscaling cluster could handle
    max_parallelism = int(cluster_cpus)
    logger.info(f"Max parallelism: {max_parallelism}")

    # read the results from any previously completed compaction round
    round_completion_info = None
    if not rebase_source_partition_locator:
        round_completion_info = rcf.read_round_completion_file(
            compaction_artifact_s3_bucket, source_partition_locator
        )
        if not round_completion_info:
            logger.info(
                f"Both rebase partition and round completion file not found. Performing a entire backfill on source."
            )
        logger.info(f"Round completion file: {round_completion_info}")

    # discover input delta files
    # For rebase:
    # Copy the old compacted table to a new destination, plus any new deltas from rebased source

    # For incremental compaction:
    # Input deltas from two sources: One: new delta; Two: compacted table

    high_watermark = (
        round_completion_info.high_watermark if round_completion_info else None
    )

    (
        input_deltas,
        previous_last_stream_position_compacted_on_destination_table,
    ) = io.discover_deltas(
        source_partition_locator,
        high_watermark,
        last_stream_position_to_compact,
        destination_partition_locator,
        rebase_source_partition_locator,
        rebase_source_partition_high_watermark,
        deltacat_storage,
        **list_deltas_kwargs,
    )

    if not input_deltas:
        logger.info("No input deltas found to compact.")
        return None, None, None

    # limit the input deltas to fit on this cluster and convert them to
    # annotated deltas of equivalent size for easy parallel distribution

    (
        uniform_deltas,
        hash_bucket_count,
        last_stream_position_compacted,
    ) = io.limit_input_deltas(
        input_deltas,
        cluster_resources,
        hash_bucket_count,
        min_hash_bucket_chunk_size,
        input_deltas_stats=input_deltas_stats,
        deltacat_storage=deltacat_storage,
    )

    assert hash_bucket_count is not None and hash_bucket_count > 0, (
        f"Expected hash bucket count to be a positive integer, but found "
        f"`{hash_bucket_count}`"
    )
    # parallel step 1:
    # group like primary keys together by hashing them into buckets
    if not round_completion_info and rebase_source_partition_locator:
        # generate a rc_info for hb and materialize to tell whether a delta is src or destination in case of rebase
        dest_delta_locator = DeltaLocator.of(
            partition_locator=rebase_source_partition_locator, stream_position=None
        )
        round_completion_info = RoundCompletionInfo.of(
            None, dest_delta_locator, None, 0, None
        )
    hb_tasks_pending = invoke_parallel(
        items=uniform_deltas,
        ray_task=hb.hash_bucket,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        round_completion_info=round_completion_info,
        primary_keys=primary_keys,
        sort_keys=sort_keys,
        num_buckets=hash_bucket_count,
        num_groups=max_parallelism,
        enable_profiler=enable_profiler,
        metrics_config=metrics_config,
        deltacat_storage=deltacat_storage,
    )
    logger.info(f"Getting {len(hb_tasks_pending)} hash bucket results...")
    hb_results = ray.get(hb_tasks_pending)
    logger.info(f"Got {len(hb_results)} hash bucket results.")
    all_hash_group_idx_to_obj_id = defaultdict(list)
    for hash_group_idx_to_obj_id in hb_results:
        for hash_group_index, object_id in enumerate(hash_group_idx_to_obj_id):
            if object_id:
                all_hash_group_idx_to_obj_id[hash_group_index].append(object_id)
    hash_group_count = len(all_hash_group_idx_to_obj_id)
    logger.info(f"Hash bucket groups created: {hash_group_count}")

    # TODO (pdames): when resources are freed during the last round of hash
    #  bucketing, start running dedupe tasks that read existing dedupe
    #  output from S3 then wait for hash bucketing to finish before continuing

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

    # parallel step 2:
    # discover records with duplicate primary keys in each hash bucket, and
    # identify the index of records to keep or drop based on sort keys
    num_materialize_buckets = max_parallelism
    logger.info(f"Materialize Bucket Count: {num_materialize_buckets}")
    dd_tasks_pending = invoke_parallel(
        items=all_hash_group_idx_to_obj_id.values(),
        ray_task=dd.dedupe,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        kwargs_provider=lambda index, item: {
            "dedupe_task_index": index,
            "object_ids": item,
        },
        sort_keys=sort_keys,
        num_materialize_buckets=num_materialize_buckets,
        enable_profiler=enable_profiler,
        metrics_config=metrics_config,
    )
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe results...")
    dd_results = ray.get(dd_tasks_pending)
    logger.info(f"Got {len(dd_results)} dedupe results.")
    all_mat_buckets_to_obj_id = defaultdict(list)
    for mat_bucket_idx_to_obj_id in dd_results:
        for (
            bucket_idx,
            dd_task_index_and_object_id_tuple,
        ) in mat_bucket_idx_to_obj_id.items():
            all_mat_buckets_to_obj_id[bucket_idx].append(
                dd_task_index_and_object_id_tuple
            )
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe result stat(s)...")
    logger.info(f"Materialize buckets created: " f"{len(all_mat_buckets_to_obj_id)}")

    # TODO(pdames): when resources are freed during the last round of deduping
    #  start running materialize tasks that read materialization source file
    #  tables from S3 then wait for deduping to finish before continuing

    # TODO(pdames): balance inputs to materialization tasks to ensure that each
    #  task has an approximately equal amount of input to materialize

    # TODO(pdames): garbage collect hash bucket output since it's no longer
    #  needed

    # parallel step 3:
    # materialize records to keep by index
    mat_tasks_pending = invoke_parallel(
        items=all_mat_buckets_to_obj_id.items(),
        ray_task=mat.materialize,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        kwargs_provider=lambda index, mat_bucket_index_to_obj_id: {
            "mat_bucket_index": mat_bucket_index_to_obj_id[0],
            "dedupe_task_idx_and_obj_id_tuples": mat_bucket_index_to_obj_id[1],
        },
        schema=schema_on_read,
        round_completion_info=round_completion_info,
        source_partition_locator=source_partition_locator,
        partition=partition,
        max_records_per_output_file=records_per_compacted_file,
        compacted_file_content_type=compacted_file_content_type,
        enable_profiler=enable_profiler,
        metrics_config=metrics_config,
        deltacat_storage=deltacat_storage,
    )
    logger.info(f"Getting {len(mat_tasks_pending)} materialize result(s)...")
    mat_results = ray.get(mat_tasks_pending)
    logger.info(f"Got {len(mat_results)} materialize result(s).")

    mat_results = sorted(mat_results, key=lambda m: m.task_index)
    deltas = [m.delta for m in mat_results]
    merged_delta = Delta.merge_deltas(deltas)
    compacted_delta = deltacat_storage.commit_delta(
        merged_delta, properties=kwargs.get("properties", {})
    )
    logger.info(f"Committed compacted delta: {compacted_delta}")

    new_compacted_delta_locator = DeltaLocator.of(
        new_compacted_partition_locator,
        compacted_delta.stream_position,
    )

    rci_high_watermark = (
        rebase_source_partition_high_watermark
        if rebase_source_partition_high_watermark
        else last_stream_position_compacted
    )

    last_rebase_source_partition_locator = rebase_source_partition_locator or (
        round_completion_info.rebase_source_partition_locator
        if round_completion_info
        else None
    )
    new_round_completion_info = RoundCompletionInfo.of(
        rci_high_watermark,
        new_compacted_delta_locator,
        PyArrowWriteResult.union([m.pyarrow_write_result for m in mat_results]),
        bit_width_of_sort_keys,
        last_rebase_source_partition_locator,
    )
    rcf_source_partition_locator = (
        rebase_source_partition_locator
        if rebase_source_partition_locator
        else source_partition_locator
    )
    round_completion_file_s3_url = rcf.write_round_completion_file(
        compaction_artifact_s3_bucket,
        rcf_source_partition_locator,
        new_round_completion_info,
    )
    if last_stream_position_compacted.get(
        source_partition_locator
    ) < last_stream_position_to_compact or (
        not rebase_source_partition_locator
        and last_stream_position_compacted.get(destination_partition_locator)
        < previous_last_stream_position_compacted_on_destination_table
    ):
        logger.info(
            f"Compaction can not be completed in one round. Either increase cluster size or decrease input"
        )
    logger.info(
        f"partition-{source_partition_locator.partition_values},"
        f"compacted at: {last_stream_position_compacted},"
        f"last position: {last_stream_position_to_compact}"
    )
    return (
        partition,
        new_round_completion_info,
        round_completion_file_s3_url,
    )
