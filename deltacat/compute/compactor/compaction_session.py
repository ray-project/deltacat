import importlib
from contextlib import nullcontext
import functools
import logging
import ray
import time
import json
from deltacat.aws import s3u as s3_utils
import deltacat
from deltacat import logs
import pyarrow as pa
from deltacat.compute.compactor import (
    PyArrowWriteResult,
    RoundCompletionInfo,
)
from deltacat.storage.model.sort_key import SortKey
from deltacat.compute.compactor.model.dedupe_result import DedupeResult
from deltacat.compute.compactor.model.hash_bucket_result import HashBucketResult
from deltacat.io.object_store import IObjectStore
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.storage import (
    Delta,
    DeltaLocator,
    Partition,
    PartitionLocator,
    interface as unimplemented_deltacat_storage,
)
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
    round_robin_options_provider,
)
from deltacat.utils.common import ReadKwargsProvider
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
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.compute.compactor.utils.sort_key import validate_sort_keys
from deltacat.utils.resources import get_current_process_peak_memory_usage_in_bytes


if importlib.util.find_spec("memray"):
    import memray


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

DEFAULT_DEDUPE_MAX_PARALLELISM_RATIO_ARG: int = 1
DEFAULT_PROPERTIES_ARG: Dict[str, Any] = {}


def check_preconditions(
    source_partition_locator: PartitionLocator,
    destination_partition_locator: PartitionLocator,
    sort_keys: List[SortKey],
    max_records_per_output_file: int,
    new_hash_bucket_count: Optional[int],
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> int:
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}
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
    return validate_sort_keys(
        source_partition_locator,
        sort_keys,
        deltacat_storage,
        deltacat_storage_kwargs,
        **kwargs,
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
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    object_store: Optional[IObjectStore] = RayPlasmaObjectStore(),
    s3_client_kwargs: Optional[Dict[str, Any]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Optional[str]:
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}
    if not importlib.util.find_spec("memray"):
        logger.info(f"memray profiler not available, disabling all profiling")
        enable_profiler = False

    if s3_client_kwargs is None:
        s3_client_kwargs = {}

    # memray official documentation link:
    # https://bloomberg.github.io/memray/getting_started.html
    with memray.Tracker(
        f"compaction_partition.bin"
    ) if enable_profiler else nullcontext():
        partition = None
        (
            new_partition,
            new_rci,
            new_rcf_partition_locator,
        ) = _execute_compaction_round(
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
            read_kwargs_provider,
            s3_table_writer_kwargs,
            object_store,
            s3_client_kwargs,
            deltacat_storage,
            deltacat_storage_kwargs,
            **kwargs,
        )
        if new_partition:
            partition = new_partition

        logger.info(
            f"Partition-{source_partition_locator.partition_values}-> Compaction session data processing completed"
        )
        round_completion_file_s3_url = None
        if partition:
            logger.info(f"Committing compacted partition to: {partition.locator}")
            partition = deltacat_storage.commit_partition(
                partition, **deltacat_storage_kwargs
            )
            logger.info(f"Committed compacted partition: {partition}")

            round_completion_file_s3_url = rcf.write_round_completion_file(
                compaction_artifact_s3_bucket,
                new_rcf_partition_locator,
                partition.locator,
                new_rci,
                **s3_client_kwargs,
            )
        logger.info(f"Completed compaction session for: {source_partition_locator}")
        return round_completion_file_s3_url


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
    list_deltas_kwargs: Optional[Dict[str, Any]],
    read_kwargs_provider: Optional[ReadKwargsProvider],
    s3_table_writer_kwargs: Optional[Dict[str, Any]],
    object_store: Optional[IObjectStore],
    s3_client_kwargs: Optional[Dict[str, Any]],
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Tuple[Optional[Partition], Optional[RoundCompletionInfo], Optional[str]]:
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}
    rcf_source_partition_locator = (
        rebase_source_partition_locator
        if rebase_source_partition_locator
        else source_partition_locator
    )
    base_audit_url = rcf_source_partition_locator.path(
        f"s3://{compaction_artifact_s3_bucket}/compaction-audit"
    )
    audit_url = f"{base_audit_url}.json"

    logger.info(f"Compaction audit will be written to {audit_url}")

    compaction_audit = CompactionSessionAuditInfo(
        deltacat.__version__,
        ray.__version__,
        audit_url,
    ).set_compactor_version(CompactorVersion.V1.value)

    compaction_start = time.monotonic()

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
        deltacat_storage_kwargs,
        **kwargs,
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
            compaction_artifact_s3_bucket,
            source_partition_locator,
            destination_partition_locator,
            **s3_client_kwargs,
        )
        if not round_completion_info:
            logger.info(
                f"Both rebase partition and round completion file not found. Performing an entire backfill on source."
            )
        logger.info(f"Round completion file: {round_completion_info}")

    enable_manifest_entry_copy_by_reference = (
        False if rebase_source_partition_locator else True
    )
    logger.info(
        f"Enable manifest entry copy by reference is set to: {enable_manifest_entry_copy_by_reference}"
    )

    # discover input delta files
    # For rebase:
    # Copy the old compacted table to a new destination, plus any new deltas from rebased source

    # For incremental compaction:
    # Input deltas from two sources: One: new delta; Two: compacted table

    high_watermark = (
        round_completion_info.high_watermark if round_completion_info else None
    )

    delta_discovery_start = time.monotonic()
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
        deltacat_storage_kwargs,
        list_deltas_kwargs,
    )

    delta_discovery_end = time.monotonic()
    compaction_audit.set_delta_discovery_time_in_seconds(
        delta_discovery_end - delta_discovery_start
    )

    s3_utils.upload(
        compaction_audit.audit_url,
        str(json.dumps(compaction_audit)),
        **s3_client_kwargs,
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
        require_multiple_rounds,
    ) = (
        io.fit_input_deltas(
            input_deltas,
            cluster_resources,
            compaction_audit,
            hash_bucket_count,
            deltacat_storage=deltacat_storage,
            deltacat_storage_kwargs=deltacat_storage_kwargs,
            **kwargs,
        )
        if input_deltas_stats is None
        else io.limit_input_deltas(
            input_deltas,
            cluster_resources,
            hash_bucket_count,
            min_hash_bucket_chunk_size,
            compaction_audit=compaction_audit,
            input_deltas_stats=input_deltas_stats,
            deltacat_storage=deltacat_storage,
            deltacat_storage_kwargs=deltacat_storage_kwargs,
            **kwargs,
        )
    )

    compaction_audit.set_uniform_deltas_created(len(uniform_deltas))

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

    if require_multiple_rounds:
        logger.info(
            f"Compaction can not be completed in one round. Either increase cluster size or decrease input"
        )
        raise AssertionError(
            "Multiple rounds are not supported. Please increase the cluster size and run again."
        )
    hb_start = time.monotonic()
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
        read_kwargs_provider=read_kwargs_provider,
        object_store=object_store,
        deltacat_storage=deltacat_storage,
        deltacat_storage_kwargs=deltacat_storage_kwargs,
        **kwargs,
    )
    hb_invoke_end = time.monotonic()

    logger.info(f"Getting {len(hb_tasks_pending)} hash bucket results...")
    hb_results: List[HashBucketResult] = ray.get(hb_tasks_pending)
    logger.info(f"Got {len(hb_results)} hash bucket results.")
    hb_end = time.monotonic()
    hb_results_retrieved_at = time.time()

    telemetry_time_hb = compaction_audit.save_step_stats(
        CompactionSessionAuditInfo.HASH_BUCKET_STEP_NAME,
        hb_results,
        hb_results_retrieved_at,
        hb_invoke_end - hb_start,
        hb_end - hb_start,
    )

    s3_utils.upload(
        compaction_audit.audit_url,
        str(json.dumps(compaction_audit)),
        **s3_client_kwargs,
    )

    all_hash_group_idx_to_obj_id = defaultdict(list)
    for hb_result in hb_results:
        for hash_group_index, object_id in enumerate(
            hb_result.hash_bucket_group_to_obj_id
        ):
            if object_id:
                all_hash_group_idx_to_obj_id[hash_group_index].append(object_id)
    hash_group_count = len(all_hash_group_idx_to_obj_id)
    logger.info(f"Hash bucket groups created: {hash_group_count}")
    total_hb_record_count = sum([hb_result.hb_record_count for hb_result in hb_results])
    logger.info(
        f"Got {total_hb_record_count} hash bucket records from hash bucketing step..."
    )

    compaction_audit.set_input_records(total_hb_record_count.item())
    # TODO (pdames): when resources are freed during the last round of hash
    #  bucketing, start running dedupe tasks that read existing dedupe
    #  output from S3 then wait for hash bucketing to finish before continuing

    # create a new stream for this round
    compacted_stream_locator = destination_partition_locator.stream_locator
    stream = deltacat_storage.get_stream(
        compacted_stream_locator.namespace,
        compacted_stream_locator.table_name,
        compacted_stream_locator.table_version,
        **deltacat_storage_kwargs,
    )
    partition = deltacat_storage.stage_partition(
        stream,
        destination_partition_locator.partition_values,
        **deltacat_storage_kwargs,
    )
    new_compacted_partition_locator = partition.locator
    # parallel step 2:
    # discover records with duplicate primary keys in each hash bucket, and
    # identify the index of records to keep or drop based on sort keys
    num_materialize_buckets = max_parallelism
    logger.info(f"Materialize Bucket Count: {num_materialize_buckets}")

    dedupe_start = time.monotonic()
    dd_max_parallelism = int(
        max_parallelism
        * kwargs.get(
            "dd_max_parallelism_ratio", DEFAULT_DEDUPE_MAX_PARALLELISM_RATIO_ARG
        )
    )
    logger.info(
        f"dd max_parallelism is set to {dd_max_parallelism}, max_parallelism is {max_parallelism}"
    )
    dd_tasks_pending = invoke_parallel(
        items=all_hash_group_idx_to_obj_id.values(),
        ray_task=dd.dedupe,
        max_parallelism=dd_max_parallelism,
        options_provider=round_robin_opt_provider,
        kwargs_provider=lambda index, item: {
            "dedupe_task_index": index,
            "object_ids": item,
        },
        sort_keys=sort_keys,
        num_materialize_buckets=num_materialize_buckets,
        enable_profiler=enable_profiler,
        metrics_config=metrics_config,
        object_store=object_store,
    )

    dedupe_invoke_end = time.monotonic()
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe results...")
    dd_results: List[DedupeResult] = ray.get(dd_tasks_pending)
    logger.info(f"Got {len(dd_results)} dedupe results.")

    # we use time.time() here because time.monotonic() has no reference point
    # whereas time.time() measures epoch seconds. Hence, it will be reasonable
    # to compare time.time()s captured in different nodes.
    dedupe_results_retrieved_at = time.time()
    dedupe_end = time.monotonic()

    total_dd_record_count = sum([ddr.deduped_record_count for ddr in dd_results])
    logger.info(f"Deduped {total_dd_record_count} records...")

    telemetry_time_dd = compaction_audit.save_step_stats(
        CompactionSessionAuditInfo.DEDUPE_STEP_NAME,
        dd_results,
        dedupe_results_retrieved_at,
        dedupe_invoke_end - dedupe_start,
        dedupe_end - dedupe_start,
    )

    compaction_audit.set_records_deduped(total_dd_record_count.item())
    all_mat_buckets_to_obj_id = defaultdict(list)
    for dd_result in dd_results:
        for (
            bucket_idx,
            dd_task_index_and_object_id_tuple,
        ) in dd_result.mat_bucket_idx_to_obj_id.items():
            all_mat_buckets_to_obj_id[bucket_idx].append(
                dd_task_index_and_object_id_tuple
            )
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe result stat(s)...")
    logger.info(f"Materialize buckets created: " f"{len(all_mat_buckets_to_obj_id)}")

    compaction_audit.set_materialize_buckets(len(all_mat_buckets_to_obj_id))
    # TODO(pdames): when resources are freed during the last round of deduping
    #  start running materialize tasks that read materialization source file
    #  tables from S3 then wait for deduping to finish before continuing

    # TODO(pdames): balance inputs to materialization tasks to ensure that each
    #  task has an approximately equal amount of input to materialize

    # TODO(pdames): garbage collect hash bucket output since it's no longer
    #  needed

    # parallel step 3:
    # materialize records to keep by index

    s3_utils.upload(
        compaction_audit.audit_url,
        str(json.dumps(compaction_audit)),
        **s3_client_kwargs,
    )

    materialize_start = time.monotonic()
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
        enable_manifest_entry_copy_by_reference=enable_manifest_entry_copy_by_reference,
        max_records_per_output_file=records_per_compacted_file,
        compacted_file_content_type=compacted_file_content_type,
        enable_profiler=enable_profiler,
        metrics_config=metrics_config,
        read_kwargs_provider=read_kwargs_provider,
        s3_table_writer_kwargs=s3_table_writer_kwargs,
        object_store=object_store,
        deltacat_storage=deltacat_storage,
        deltacat_storage_kwargs=deltacat_storage_kwargs,
    )

    materialize_invoke_end = time.monotonic()

    logger.info(f"Getting {len(mat_tasks_pending)} materialize result(s)...")
    mat_results: List[MaterializeResult] = ray.get(mat_tasks_pending)

    logger.info(f"Got {len(mat_results)} materialize result(s).")

    materialize_end = time.monotonic()
    materialize_results_retrieved_at = time.time()

    telemetry_time_materialize = compaction_audit.save_step_stats(
        CompactionSessionAuditInfo.MATERIALIZE_STEP_NAME,
        mat_results,
        materialize_results_retrieved_at,
        materialize_invoke_end - materialize_start,
        materialize_end - materialize_start,
    )

    mat_results = sorted(mat_results, key=lambda m: m.task_index)
    deltas = [m.delta for m in mat_results]

    # Note: An appropriate last stream position must be set
    # to avoid correctness issue.
    merged_delta = Delta.merge_deltas(
        deltas,
        stream_position=last_stream_position_to_compact,
    )

    record_info_msg = (
        f"Hash bucket records: {total_hb_record_count},"
        f" Deduped records: {total_dd_record_count}, "
        f" Materialized records: {merged_delta.meta.record_count}"
    )
    logger.info(record_info_msg)

    assert (
        total_hb_record_count - total_dd_record_count == merged_delta.meta.record_count
    ), (
        f"Number of hash bucket records minus the number of deduped records"
        f" does not match number of materialized records.\n"
        f" {record_info_msg}"
    )
    compacted_delta = deltacat_storage.commit_delta(
        merged_delta,
        properties=kwargs.get("properties", DEFAULT_PROPERTIES_ARG),
        **deltacat_storage_kwargs,
    )
    logger.info(f"Committed compacted delta: {compacted_delta}")

    compaction_end = time.monotonic()
    compaction_audit.set_compaction_time_in_seconds(compaction_end - compaction_start)

    new_compacted_delta_locator = DeltaLocator.of(
        new_compacted_partition_locator,
        compacted_delta.stream_position,
    )

    last_rebase_source_partition_locator = rebase_source_partition_locator or (
        round_completion_info.rebase_source_partition_locator
        if round_completion_info
        else None
    )

    pyarrow_write_result = PyArrowWriteResult.union(
        [m.pyarrow_write_result for m in mat_results]
    )

    session_peak_memory = get_current_process_peak_memory_usage_in_bytes()
    compaction_audit.set_peak_memory_used_bytes_by_compaction_session_process(
        session_peak_memory
    )

    compaction_audit.save_round_completion_stats(mat_results)
    compaction_audit.set_telemetry_time_in_seconds(
        telemetry_time_hb + telemetry_time_dd + telemetry_time_materialize
    )

    s3_utils.upload(
        compaction_audit.audit_url,
        str(json.dumps(compaction_audit)),
        **s3_client_kwargs,
    )

    new_round_completion_info = RoundCompletionInfo.of(
        last_stream_position_compacted,
        new_compacted_delta_locator,
        pyarrow_write_result,
        bit_width_of_sort_keys,
        last_rebase_source_partition_locator,
        compaction_audit.untouched_file_ratio,
        audit_url,
        hash_bucket_count,
        None,
        CompactorVersion.V1.value,
    )

    logger.info(
        f"partition-{source_partition_locator.partition_values},"
        f"compacted at: {last_stream_position_compacted},"
        f"last position: {last_stream_position_to_compact}"
    )

    return (
        partition,
        new_round_completion_info,
        rcf_source_partition_locator,
    )


def compact_partition_from_request(
    compact_partition_params: CompactPartitionParams,
    *compact_partition_pos_args,
) -> Optional[str]:
    """
    Wrapper for compact_partition that allows for the compact_partition parameters to be
    passed in as a custom dictionary-like CompactPartitionParams object along with any compact_partition positional arguments.
    :param compact_partition_params:
    """
    return compact_partition(*compact_partition_pos_args, **compact_partition_params)
