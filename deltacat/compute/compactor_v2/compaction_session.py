import importlib
from contextlib import nullcontext
import numpy as np
import functools
import logging
import ray
import time
import json
from deltacat.aws import s3u as s3_utils
import deltacat
from deltacat import logs
from deltacat.compute.compactor import (
    PyArrowWriteResult,
    RoundCompletionInfo,
)
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.storage import (
    Delta,
    DeltaLocator,
    Partition,
)
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
    task_resource_options_provider,
)
from deltacat.compute.compactor_v2.steps import merge as mg
from deltacat.compute.compactor_v2.steps import hash_bucket as hb
from deltacat.compute.compactor_v2.utils import io
from deltacat.compute.compactor.utils import round_completion_file as rcf

from typing import List, Optional, Tuple
from collections import defaultdict
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.utils.resources import (
    get_current_node_peak_memory_usage_in_bytes,
)
from deltacat.compute.compactor_v2.utils.task_options import (
    hash_bucket_resource_options_provider,
    merge_resource_options_provider,
)
from deltacat.utils.resources import ClusterUtilizationOverTimeRange
from deltacat.compute.compactor.model.compactor_version import CompactorVersion

if importlib.util.find_spec("memray"):
    import memray


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def compact_partition(params: CompactPartitionParams, **kwargs) -> Optional[str]:

    assert (
        params.hash_bucket_count is not None and params.hash_bucket_count >= 1
    ), "hash_bucket_count is a required arg for compactor v2"

    with memray.Tracker(
        f"compaction_partition.bin"
    ) if params.enable_profiler else nullcontext(), ClusterUtilizationOverTimeRange() as cluster_util:
        (new_partition, new_rci, new_rcf_partition_locator,) = _execute_compaction(
            params,
            cluster_util=cluster_util,
            **kwargs,
        )

        logger.info(
            f"Partition-{params.source_partition_locator} -> "
            f"Compaction session data processing completed"
        )
        round_completion_file_s3_url = None
        if new_partition:
            logger.info(f"Committing compacted partition to: {new_partition.locator}")
            partition = params.deltacat_storage.commit_partition(
                new_partition, **params.deltacat_storage_kwargs
            )
            logger.info(f"Committed compacted partition: {partition}")

            round_completion_file_s3_url = rcf.write_round_completion_file(
                params.compaction_artifact_s3_bucket,
                new_rcf_partition_locator,
                new_rci,
                **params.s3_client_kwargs,
            )
        else:
            logger.warn("No new partition was committed during compaction.")

        logger.info(
            f"Completed compaction session for: {params.source_partition_locator}"
        )
        return round_completion_file_s3_url


def _execute_compaction(
    params: CompactPartitionParams, **kwargs
) -> Tuple[Optional[Partition], Optional[RoundCompletionInfo], Optional[str]]:

    rcf_source_partition_locator = (
        params.rebase_source_partition_locator or params.source_partition_locator
    )

    base_audit_url = rcf_source_partition_locator.path(
        f"s3://{params.compaction_artifact_s3_bucket}/compaction-audit"
    )
    audit_url = f"{base_audit_url}.json"
    logger.info(f"Compaction audit will be written to {audit_url}")
    compaction_audit = (
        CompactionSessionAuditInfo(deltacat.__version__, ray.__version__, audit_url)
        .set_hash_bucket_count(params.hash_bucket_count)
        .set_compactor_version(CompactorVersion.V2.value)
    )

    compaction_start = time.monotonic()

    task_max_parallelism = params.task_max_parallelism

    if params.pg_config:
        logger.info(
            "pg_config specified. Tasks will be scheduled in a placement group."
        )
        cluster_resources = params.pg_config.resource
        cluster_cpus = cluster_resources["CPU"]
        cluster_memory = cluster_resources["memory"]
        task_max_parallelism = cluster_cpus
        compaction_audit.set_total_cluster_memory_bytes(cluster_memory)

    # read the results from any previously completed compaction round
    round_completion_info = None
    high_watermark = None
    previous_compacted_delta_manifest = None

    if not params.rebase_source_partition_locator:
        round_completion_info = rcf.read_round_completion_file(
            params.compaction_artifact_s3_bucket,
            params.source_partition_locator,
            **params.s3_client_kwargs,
        )
        if not round_completion_info:
            logger.info(
                f"Both rebase partition and round completion file not found. Performing an entire backfill on source."
            )
        else:
            compacted_delta_locator = round_completion_info.compacted_delta_locator

            previous_compacted_delta_manifest = (
                params.deltacat_storage.get_delta_manifest(
                    compacted_delta_locator, **params.deltacat_storage_kwargs
                )
            )

            high_watermark = round_completion_info.high_watermark
            logger.info(f"Setting round completion high watermark: {high_watermark}")
            assert (
                params.hash_bucket_count == round_completion_info.hash_bucket_count
            ), (
                "The hash bucket count has changed. "
                "Kindly run rebase compaction and trigger incremental again. "
                f"Hash Bucket count in RCF={round_completion_info.hash_bucket_count} "
                f"not equal to Hash bucket count in args={params.hash_bucket_count}."
            )

        logger.info(f"Round completion file: {round_completion_info}")

    delta_discovery_start = time.monotonic()

    input_deltas = io.discover_deltas(
        params.source_partition_locator,
        params.last_stream_position_to_compact,
        params.rebase_source_partition_locator,
        params.rebase_source_partition_high_watermark,
        high_watermark,
        params.deltacat_storage,
        params.deltacat_storage_kwargs,
        params.list_deltas_kwargs,
    )

    uniform_deltas = io.create_uniform_input_deltas(
        input_deltas=input_deltas,
        hash_bucket_count=params.hash_bucket_count,
        compaction_audit=compaction_audit,
        deltacat_storage=params.deltacat_storage,
        previous_inflation=params.previous_inflation,
        min_delta_bytes=params.min_delta_bytes_in_batch,
        min_file_counts=params.min_files_in_batch,
        # disable input split during rebase as the rebase files are already uniform
        enable_input_split=params.rebase_source_partition_locator is None,
        deltacat_storage_kwargs=params.deltacat_storage_kwargs,
    )

    delta_discovery_end = time.monotonic()

    compaction_audit.set_uniform_deltas_created(len(uniform_deltas))
    compaction_audit.set_delta_discovery_time_in_seconds(
        delta_discovery_end - delta_discovery_start
    )

    s3_utils.upload(
        compaction_audit.audit_url,
        str(json.dumps(compaction_audit)),
        **params.s3_client_kwargs,
    )

    if not input_deltas:
        logger.info("No input deltas found to compact.")
        return None, None, None

    hb_options_provider = functools.partial(
        task_resource_options_provider,
        pg_config=params.pg_config,
        resource_amount_provider=hash_bucket_resource_options_provider,
        previous_inflation=params.previous_inflation,
        average_record_size_bytes=params.average_record_size_bytes,
        primary_keys=params.primary_keys,
        ray_custom_resources=params.ray_custom_resources,
    )

    hb_start = time.monotonic()

    def hash_bucket_input_provider(index, item):
        return {
            "input": HashBucketInput.of(
                item,
                primary_keys=params.primary_keys,
                num_hash_buckets=params.hash_bucket_count,
                num_hash_groups=params.hash_group_count,
                enable_profiler=params.enable_profiler,
                metrics_config=params.metrics_config,
                read_kwargs_provider=params.read_kwargs_provider,
                object_store=params.object_store,
                deltacat_storage=params.deltacat_storage,
                deltacat_storage_kwargs=params.deltacat_storage_kwargs,
            )
        }

    hb_tasks_pending = invoke_parallel(
        items=uniform_deltas,
        ray_task=hb.hash_bucket,
        max_parallelism=task_max_parallelism,
        options_provider=hb_options_provider,
        kwargs_provider=hash_bucket_input_provider,
    )

    hb_invoke_end = time.monotonic()

    logger.info(f"Getting {len(hb_tasks_pending)} hash bucket results...")
    hb_results: List[HashBucketResult] = ray.get(hb_tasks_pending)
    logger.info(f"Got {len(hb_results)} hash bucket results.")
    hb_end = time.monotonic()

    # we use time.time() here because time.monotonic() has no reference point
    # whereas time.time() measures epoch seconds. Hence, it will be reasonable
    # to compare time.time()s captured in different nodes.
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
        **params.s3_client_kwargs,
    )

    all_hash_group_idx_to_obj_id = defaultdict(list)
    all_hash_group_idx_to_size_bytes = defaultdict(int)
    all_hash_group_idx_to_num_rows = defaultdict(int)
    hb_data_processed_size_bytes = np.int64(0)
    total_hb_record_count = np.int64(0)

    # initialize all hash groups
    for hb_group in range(params.hash_group_count):
        all_hash_group_idx_to_num_rows[hb_group] = 0
        all_hash_group_idx_to_obj_id[hb_group] = []
        all_hash_group_idx_to_size_bytes[hb_group] = 0

    for hb_result in hb_results:
        hb_data_processed_size_bytes += hb_result.hb_size_bytes
        total_hb_record_count += hb_result.hb_record_count

        for hash_group_index, object_id_size_tuple in enumerate(
            hb_result.hash_bucket_group_to_obj_id_tuple
        ):
            if object_id_size_tuple:
                all_hash_group_idx_to_obj_id[hash_group_index].append(
                    object_id_size_tuple[0]
                )
                all_hash_group_idx_to_size_bytes[
                    hash_group_index
                ] += object_id_size_tuple[1].item()
                all_hash_group_idx_to_num_rows[
                    hash_group_index
                ] += object_id_size_tuple[2].item()

    logger.info(
        f"Got {total_hb_record_count} hash bucket records from hash bucketing step..."
    )

    compaction_audit.set_input_records(total_hb_record_count.item())
    compaction_audit.set_hash_bucket_processed_size_bytes(
        hb_data_processed_size_bytes.item()
    )

    # create a new stream for this round
    compacted_stream_locator = params.destination_partition_locator.stream_locator
    compacted_stream = params.deltacat_storage.get_stream(
        compacted_stream_locator.namespace,
        compacted_stream_locator.table_name,
        compacted_stream_locator.table_version,
        **params.deltacat_storage_kwargs,
    )
    compacted_partition = params.deltacat_storage.stage_partition(
        compacted_stream,
        params.destination_partition_locator.partition_values,
        **params.deltacat_storage_kwargs,
    )

    # BSP Step 2: Merge
    merge_options_provider = functools.partial(
        task_resource_options_provider,
        pg_config=params.pg_config,
        resource_amount_provider=merge_resource_options_provider,
        num_hash_groups=params.hash_group_count,
        hash_group_size_bytes=all_hash_group_idx_to_size_bytes,
        hash_group_num_rows=all_hash_group_idx_to_num_rows,
        round_completion_info=round_completion_info,
        compacted_delta_manifest=previous_compacted_delta_manifest,
        primary_keys=params.primary_keys,
        deltacat_storage=params.deltacat_storage,
        deltacat_storage_kwargs=params.deltacat_storage_kwargs,
        ray_custom_resources=params.ray_custom_resources,
    )

    def merge_input_provider(index, item):
        return {
            "input": MergeInput.of(
                dfe_groups_refs=item[1],
                write_to_partition=compacted_partition,
                compacted_file_content_type=params.compacted_file_content_type,
                primary_keys=params.primary_keys,
                sort_keys=params.sort_keys,
                merge_task_index=index,
                hash_bucket_count=params.hash_bucket_count,
                drop_duplicates=params.drop_duplicates,
                hash_group_index=item[0],
                num_hash_groups=params.hash_group_count,
                max_records_per_output_file=params.records_per_compacted_file,
                enable_profiler=params.enable_profiler,
                metrics_config=params.metrics_config,
                s3_table_writer_kwargs=params.s3_table_writer_kwargs,
                read_kwargs_provider=params.read_kwargs_provider,
                round_completion_info=round_completion_info,
                object_store=params.object_store,
                deltacat_storage=params.deltacat_storage,
                deltacat_storage_kwargs=params.deltacat_storage_kwargs,
            )
        }

    merge_start = time.monotonic()

    merge_tasks_pending = invoke_parallel(
        items=all_hash_group_idx_to_obj_id.items(),
        ray_task=mg.merge,
        max_parallelism=task_max_parallelism,
        options_provider=merge_options_provider,
        kwargs_provider=merge_input_provider,
    )

    merge_invoke_end = time.monotonic()
    logger.info(f"Getting {len(merge_tasks_pending)} merge results...")
    merge_results: List[MergeResult] = ray.get(merge_tasks_pending)
    logger.info(f"Got {len(merge_results)} merge results.")

    merge_results_retrieved_at = time.time()
    merge_end = time.monotonic()

    total_dd_record_count = sum([ddr.deduped_record_count for ddr in merge_results])
    logger.info(f"Deduped {total_dd_record_count} records...")

    telemetry_time_merge = compaction_audit.save_step_stats(
        CompactionSessionAuditInfo.MERGE_STEP_NAME,
        merge_results,
        merge_results_retrieved_at,
        merge_invoke_end - merge_start,
        merge_end - merge_start,
    )

    compaction_audit.set_records_deduped(total_dd_record_count.item())

    mat_results = []
    for merge_result in merge_results:
        mat_results.extend(merge_result.materialize_results)

    mat_results: List[MaterializeResult] = sorted(
        mat_results, key=lambda m: m.task_index
    )

    hb_id_to_entry_indices_range = {}
    file_index = 0
    previous_task_index = -1

    for mat_result in mat_results:
        assert (
            mat_result.pyarrow_write_result.files >= 1
        ), "Atleast one file must be materialized"
        assert (
            mat_result.task_index != previous_task_index
        ), f"Multiple materialize results found for a hash bucket: {mat_result.task_index}"

        hb_id_to_entry_indices_range[str(mat_result.task_index)] = (
            file_index,
            file_index + mat_result.pyarrow_write_result.files,
        )

        file_index += mat_result.pyarrow_write_result.files
        previous_task_index = mat_result.task_index

    s3_utils.upload(
        compaction_audit.audit_url,
        str(json.dumps(compaction_audit)),
        **params.s3_client_kwargs,
    )

    deltas = [m.delta for m in mat_results]

    # Note: An appropriate last stream position must be set
    # to avoid correctness issue.
    merged_delta = Delta.merge_deltas(
        deltas,
        stream_position=params.last_stream_position_to_compact,
    )

    record_info_msg = (
        f"Hash bucket records: {total_hb_record_count},"
        f" Deduped records: {total_dd_record_count}, "
        f" Materialized records: {merged_delta.meta.record_count}"
    )
    logger.info(record_info_msg)

    compacted_delta = params.deltacat_storage.commit_delta(
        merged_delta,
        properties=kwargs.get("properties", {}),
        **params.deltacat_storage_kwargs,
    )

    logger.info(f"Committed compacted delta: {compacted_delta}")

    compaction_end = time.monotonic()
    compaction_audit.set_compaction_time_in_seconds(compaction_end - compaction_start)

    new_compacted_delta_locator = DeltaLocator.of(
        compacted_partition.locator,
        compacted_delta.stream_position,
    )

    pyarrow_write_result = PyArrowWriteResult.union(
        [m.pyarrow_write_result for m in mat_results]
    )

    session_peak_memory = get_current_node_peak_memory_usage_in_bytes()
    compaction_audit.set_peak_memory_used_bytes_by_compaction_session_process(
        session_peak_memory
    )

    compaction_audit.save_round_completion_stats(
        mat_results, telemetry_time_hb + telemetry_time_merge
    )

    cluster_util: ClusterUtilizationOverTimeRange = kwargs.get("cluster_util")

    if cluster_util:
        compaction_audit.set_total_cpu_seconds(cluster_util.total_vcpu_seconds)
        compaction_audit.set_used_cpu_seconds(cluster_util.used_vcpu_seconds)
        compaction_audit.set_used_memory_gb_seconds(cluster_util.used_memory_gb_seconds)
        compaction_audit.set_total_memory_gb_seconds(
            cluster_util.total_memory_gb_seconds
        )
        compaction_audit.set_cluster_cpu_max(cluster_util.max_cpu)

    s3_utils.upload(
        compaction_audit.audit_url,
        str(json.dumps(compaction_audit)),
        **params.s3_client_kwargs,
    )

    input_inflation = None
    input_average_record_size_bytes = None
    # Note: we only consider inflation for incremental delta
    if (
        compaction_audit.input_size_bytes
        and compaction_audit.hash_bucket_processed_size_bytes
    ):
        input_inflation = (
            compaction_audit.hash_bucket_processed_size_bytes
            / compaction_audit.input_size_bytes
        )

    if (
        compaction_audit.hash_bucket_processed_size_bytes
        and compaction_audit.input_records
    ):
        input_average_record_size_bytes = (
            compaction_audit.hash_bucket_processed_size_bytes
            / compaction_audit.input_records
        )

    logger.info(
        f"The inflation of input deltas={input_inflation}"
        f" and average record size={input_average_record_size_bytes}"
    )

    # After all incremental delta related calculations, we update
    # the input sizes to accomodate the compacted table
    if round_completion_info:
        compaction_audit.set_input_file_count(
            (compaction_audit.input_file_count or 0)
            + round_completion_info.compacted_pyarrow_write_result.files
        )
        compaction_audit.set_input_size_bytes(
            (compaction_audit.input_size_bytes or 0.0)
            + round_completion_info.compacted_pyarrow_write_result.file_bytes
        )
        compaction_audit.set_input_records(
            (compaction_audit.input_records or 0)
            + round_completion_info.compacted_pyarrow_write_result.records
        )

    new_round_completion_info = RoundCompletionInfo.of(
        high_watermark=params.last_stream_position_to_compact,
        compacted_delta_locator=new_compacted_delta_locator,
        compacted_pyarrow_write_result=pyarrow_write_result,
        sort_keys_bit_width=params.bit_width_of_sort_keys,
        manifest_entry_copied_by_reference_ratio=compaction_audit.untouched_file_ratio,
        compaction_audit_url=audit_url,
        hash_bucket_count=params.hash_bucket_count,
        hb_index_to_entry_range=hb_id_to_entry_indices_range,
        compactor_version=CompactorVersion.V2.value,
        input_inflation=input_inflation,
        input_average_record_size_bytes=input_average_record_size_bytes,
    )

    logger.info(
        f"partition-{params.source_partition_locator.partition_values},"
        f"compacted at: {params.last_stream_position_to_compact},"
    )

    return (
        compacted_partition,
        new_round_completion_info,
        rcf_source_partition_locator,
    )
