import numpy as np
import functools
import logging
import ray
import time
import json
from math import ceil

from deltacat.compute.compactor import (
    PyArrowWriteResult,
    HighWatermark,
    RoundCompletionInfo,
)
from deltacat.aws import s3u as s3_utils
from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.compute.compactor_v2.model.evaluate_compaction_result import (
    ExecutionCompactionResult,
)
from deltacat.compute.compactor_v2.model.merge_file_group import (
    RemoteMergeFileGroupsProvider,
)
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput

from deltacat import logs
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.compute.compactor_v2.utils.merge import (
    generate_local_merge_input,
)
from deltacat.compute.compactor_v2.utils.task_options import (
    hash_bucket_resource_options_provider,
)
from deltacat.compute.compactor.utils import round_completion_file as rcf
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.compute.compactor_v2.utils.delta import contains_delete_deltas
from deltacat.compute.compactor_v2.deletes.delete_strategy import (
    DeleteStrategy,
)
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from deltacat.compute.compactor_v2.deletes.utils import prepare_deletes

from deltacat.storage import (
    Delta,
    DeltaType,
    DeltaLocator,
    Partition,
    Manifest,
    Stream,
    StreamLocator,
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

from typing import List, Optional
from collections import defaultdict
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.compute.compactor_v2.utils.task_options import (
    merge_resource_options_provider,
    local_merge_resource_options_provider,
)


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _fetch_compaction_metadata(
    params: CompactPartitionParams,
) -> tuple[Optional[Manifest], Optional[RoundCompletionInfo]]:

    # read the results from any previously completed compaction round
    round_completion_info: Optional[RoundCompletionInfo] = None
    high_watermark: Optional[HighWatermark] = None
    previous_compacted_delta_manifest: Optional[Manifest] = None

    if not params.rebase_source_partition_locator:
        round_completion_info = rcf.read_round_completion_file(
            params.compaction_artifact_s3_bucket,
            params.source_partition_locator,
            params.destination_partition_locator,
            **params.s3_client_kwargs,
        )
        if not round_completion_info:
            logger.info(
                "Both rebase partition and round completion file not found. Performing an entire backfill on source."
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
    return (
        previous_compacted_delta_manifest,
        round_completion_info,
    )


def _build_uniform_deltas(
    params: CompactPartitionParams,
    mutable_compaction_audit: CompactionSessionAuditInfo,
    input_deltas: List[Delta],
    delta_discovery_start: float,
) -> tuple[List[DeltaAnnotated], DeleteStrategy, List[DeleteFileEnvelope], Partition]:

    delete_strategy: Optional[DeleteStrategy] = None
    delete_file_envelopes: Optional[List[DeleteFileEnvelope]] = None
    delete_file_size_bytes: int = 0
    if contains_delete_deltas(input_deltas):
        input_deltas, delete_file_envelopes, delete_strategy = prepare_deletes(
            params, input_deltas
        )
        for delete_file_envelope in delete_file_envelopes:
            delete_file_size_bytes += delete_file_envelope.table_size_bytes
        logger.info(
            f" Input deltas contain {DeltaType.DELETE}-type deltas. Total delete file size={delete_file_size_bytes}."
            f" Total length of delete file envelopes={len(delete_file_envelopes)}"
        )
    uniform_deltas: List[DeltaAnnotated] = io.create_uniform_input_deltas(
        input_deltas=input_deltas,
        hash_bucket_count=params.hash_bucket_count,
        compaction_audit=mutable_compaction_audit,
        compact_partition_params=params,
        deltacat_storage=params.deltacat_storage,
        deltacat_storage_kwargs=params.deltacat_storage_kwargs,
    )
    delta_discovery_end: float = time.monotonic()

    mutable_compaction_audit.set_uniform_deltas_created(len(uniform_deltas))
    mutable_compaction_audit.set_delta_discovery_time_in_seconds(
        delta_discovery_end - delta_discovery_start
    )

    s3_utils.upload(
        mutable_compaction_audit.audit_url,
        str(json.dumps(mutable_compaction_audit)),
        **params.s3_client_kwargs,
    )

    return (
        uniform_deltas,
        delete_strategy,
        delete_file_envelopes,
    )


def _group_uniform_deltas(
    params: CompactPartitionParams, uniform_deltas: List[DeltaAnnotated]
) -> List[List[DeltaAnnotated]]:
    num_deltas = len(uniform_deltas)
    num_rounds = params.num_rounds
    if num_rounds == 1:
        return [uniform_deltas]
    assert (
        num_rounds > 0
    ), f"num_rounds parameter should be greater than zero but is {params.num_rounds}"
    assert (
        num_rounds <= num_deltas
    ), f"{params.num_rounds} rounds should be less than the number of uniform deltas, which is {len(uniform_deltas)}"
    size = ceil(num_deltas / num_rounds)
    uniform_deltas_grouped = list(
        map(
            lambda x: uniform_deltas[x * size : x * size + size],
            list(range(num_rounds)),
        )
    )
    num_deltas_after_grouping = sum(len(sublist) for sublist in uniform_deltas_grouped)
    assert (
        num_deltas_after_grouping == num_deltas
    ), f"uniform_deltas_grouped expected to have {num_deltas} deltas, but has {num_deltas_after_grouping}"
    return uniform_deltas_grouped


def _stage_new_partition(params: CompactPartitionParams) -> Partition:
    compacted_stream_locator: Optional[
        StreamLocator
    ] = params.destination_partition_locator.stream_locator
    compacted_stream: Stream = params.deltacat_storage.get_stream(
        compacted_stream_locator.namespace,
        compacted_stream_locator.table_name,
        compacted_stream_locator.table_version,
        **params.deltacat_storage_kwargs,
    )
    compacted_partition: Partition = params.deltacat_storage.stage_partition(
        compacted_stream,
        params.destination_partition_locator.partition_values,
        **params.deltacat_storage_kwargs,
    )
    return compacted_partition


def _run_hash_and_merge(
    params: CompactPartitionParams,
    uniform_deltas: List[DeltaAnnotated],
    round_completion_info: RoundCompletionInfo,
    delete_strategy: Optional[DeleteStrategy],
    delete_file_envelopes: Optional[DeleteFileEnvelope],
    mutable_compaction_audit: CompactionSessionAuditInfo,
    previous_compacted_delta_manifest: Optional[Manifest],
    compacted_partition: Partition,
) -> List[MergeResult]:
    telemetry_time_hb = 0
    total_input_records_count = np.int64(0)
    total_hb_record_count = np.int64(0)
    if params.hash_bucket_count == 1:
        logger.info("Hash bucket count set to 1. Running local merge")
        merge_start: float = time.monotonic()
        merge_results, total_input_records_count = _run_local_merge(
            params,
            uniform_deltas,
            compacted_partition,
            round_completion_info,
            delete_strategy,
            delete_file_envelopes,
            mutable_compaction_audit,
            previous_compacted_delta_manifest,
            total_input_records_count,
        )
        merge_invoke_end = time.monotonic()
    else:
        # hash bucket
        hb_start = time.monotonic()
        all_hash_group_idx_to_obj_id = defaultdict(list)
        all_hash_group_idx_to_size_bytes = defaultdict(int)
        all_hash_group_idx_to_num_rows = defaultdict(int)
        (hb_results, hb_invoke_end) = _hash_bucket(params, uniform_deltas)
        hb_end = time.monotonic()

        # we use time.time() here because time.monotonic() has no reference point
        # whereas time.time() measures epoch seconds. Hence, it will be reasonable
        # to compare time.time()s captured in different nodes.
        hb_results_retrieved_at = time.time()

        telemetry_time_hb = mutable_compaction_audit.save_step_stats(
            CompactionSessionAuditInfo.HASH_BUCKET_STEP_NAME,
            hb_results,
            hb_results_retrieved_at,
            hb_invoke_end - hb_start,
            hb_end - hb_start,
        )

        s3_utils.upload(
            mutable_compaction_audit.audit_url,
            str(json.dumps(mutable_compaction_audit)),
            **params.s3_client_kwargs,
        )

        hb_data_processed_size_bytes = np.int64(0)

        # initialize all hash groups
        for hb_group in range(params.hash_group_count):
            all_hash_group_idx_to_num_rows[hb_group] = 0
            all_hash_group_idx_to_obj_id[hb_group] = []
            all_hash_group_idx_to_size_bytes[hb_group] = 0

        for hb_result in hb_results:
            hb_data_processed_size_bytes += hb_result.hb_size_bytes
            total_input_records_count += hb_result.hb_record_count
            for hash_group_index, object_id_size_tuple in enumerate(
                hb_result.hash_bucket_group_to_obj_id_tuple
            ):
                if object_id_size_tuple:
                    all_hash_group_idx_to_obj_id[hash_group_index].append(
                        object_id_size_tuple[0],
                    )
                    all_hash_group_idx_to_size_bytes[
                        hash_group_index
                    ] += object_id_size_tuple[1].item()
                    all_hash_group_idx_to_num_rows[
                        hash_group_index
                    ] += object_id_size_tuple[2].item()
        logger.info(
            f"Got {total_input_records_count} hash bucket records from hash bucketing step..."
        )

        total_hb_record_count = total_input_records_count
        mutable_compaction_audit.set_hash_bucket_processed_size_bytes(
            hb_data_processed_size_bytes.item()
        )

        # BSP Step 2: Merge
        # NOTE: DELETE-type deltas are stored in Plasma object store
        # in prepare_deletes and therefore don't need to included
        # in merge task resource estimation
        merge_start = time.monotonic()
        merge_results, merge_invoke_end = _merge(
            params,
            task_resource_options_provider,
            merge_resource_options_provider,
            all_hash_group_idx_to_size_bytes,
            all_hash_group_idx_to_num_rows,
            round_completion_info,
            previous_compacted_delta_manifest,
            all_hash_group_idx_to_obj_id,
            compacted_partition,
            delete_strategy,
            delete_file_envelopes,
        )
    logger.info(f"Got {len(merge_results)} merge results.")

    merge_results_retrieved_at: float = time.time()
    merge_end: float = time.monotonic()

    total_dd_record_count = sum([ddr.deduped_record_count for ddr in merge_results])
    total_deleted_record_count = sum(
        [ddr.deleted_record_count for ddr in merge_results]
    )
    logger.info(
        f"Deduped {total_dd_record_count} records and deleted {total_deleted_record_count} records..."
    )

    mutable_compaction_audit.set_input_records(total_input_records_count.item())

    telemetry_time_merge = mutable_compaction_audit.save_step_stats(
        CompactionSessionAuditInfo.MERGE_STEP_NAME,
        merge_results,
        merge_results_retrieved_at,
        merge_invoke_end - merge_start,
        merge_end - merge_start,
    )

    mutable_compaction_audit.set_records_deduped(total_dd_record_count.item())
    mutable_compaction_audit.set_records_deleted(total_deleted_record_count.item())
    record_info_msg: str = (
        f"Hash bucket records: {total_hb_record_count},"
        f" Deduped records: {total_dd_record_count}, "
        f" Deleted records: {total_deleted_record_count}, "
    )
    logger.info(record_info_msg)
    telemetry_this_round = telemetry_time_hb + telemetry_time_merge
    previous_telemetry = (
        mutable_compaction_audit.telemetry_time_in_seconds
        if mutable_compaction_audit.telemetry_time_in_seconds
        else 0.0
    )

    mutable_compaction_audit.set_telemetry_time_in_seconds(
        telemetry_this_round + previous_telemetry
    )
    if params.num_rounds > 1:
        logger.info(
            f"Detected number of rounds to be {params.num_rounds}, preparing to clear object store..."
        )
        params.object_store.clear()
    else:
        logger.info(
            f"Detected number of rounds to be {params.num_rounds}, not cleaning up object store..."
        )

    return merge_results


def _merge(
    params: CompactPartitionParams,
    task_resource_options_provider: callable,
    merge_resource_options_provider: callable,
    all_hash_group_idx_to_size_bytes: dict,
    all_hash_group_idx_to_num_rows: dict,
    round_completion_info: RoundCompletionInfo,
    previous_compacted_delta_manifest: Manifest,
    all_hash_group_idx_to_obj_id: dict,
    compacted_partition: Partition,
    delete_strategy: DeleteStrategy,
    delete_file_envelopes: DeleteFileEnvelope,
) -> tuple[List[MergeResult], float]:
    merge_options_provider = functools.partial(
        task_resource_options_provider,
        pg_config=params.pg_config,
        resource_amount_provider=merge_resource_options_provider,
        num_hash_groups=params.hash_group_count,
        hash_group_size_bytes=all_hash_group_idx_to_size_bytes,
        hash_group_num_rows=all_hash_group_idx_to_num_rows,
        total_memory_buffer_percentage=params.total_memory_buffer_percentage,
        round_completion_info=round_completion_info,
        compacted_delta_manifest=previous_compacted_delta_manifest,
        primary_keys=params.primary_keys,
        deltacat_storage=params.deltacat_storage,
        deltacat_storage_kwargs=params.deltacat_storage_kwargs,
        ray_custom_resources=params.ray_custom_resources,
        memory_logs_enabled=params.memory_logs_enabled,
        estimate_resources_params=params.estimate_resources_params,
    )

    def merge_input_provider(index, item) -> dict[str, MergeInput]:
        return {
            "input": MergeInput.of(
                merge_file_groups_provider=RemoteMergeFileGroupsProvider(
                    hash_group_index=item[0],
                    dfe_groups_refs=item[1],
                    hash_bucket_count=params.hash_bucket_count,
                    num_hash_groups=params.hash_group_count,
                    object_store=params.object_store,
                ),
                write_to_partition=compacted_partition,
                compacted_file_content_type=params.compacted_file_content_type,
                primary_keys=params.primary_keys,
                sort_keys=params.sort_keys,
                merge_task_index=index,
                drop_duplicates=params.drop_duplicates,
                max_records_per_output_file=params.records_per_compacted_file,
                enable_profiler=params.enable_profiler,
                metrics_config=params.metrics_config,
                s3_table_writer_kwargs=params.s3_table_writer_kwargs,
                read_kwargs_provider=params.read_kwargs_provider,
                round_completion_info=round_completion_info,
                object_store=params.object_store,
                deltacat_storage=params.deltacat_storage,
                deltacat_storage_kwargs=params.deltacat_storage_kwargs,
                delete_strategy=delete_strategy,
                delete_file_envelopes=delete_file_envelopes,
                memory_logs_enabled=params.memory_logs_enabled,
                disable_copy_by_reference=params.disable_copy_by_reference,
            )
        }

    merge_tasks_pending = invoke_parallel(
        items=all_hash_group_idx_to_obj_id.items(),
        ray_task=mg.merge,
        max_parallelism=params.task_max_parallelism,
        options_provider=merge_options_provider,
        kwargs_provider=merge_input_provider,
    )
    merge_invoke_end = time.monotonic()
    logger.info(f"Getting {len(merge_tasks_pending)} merge results...")
    merge_results: List[MergeResult] = ray.get(merge_tasks_pending)

    return merge_results, merge_invoke_end


def _hash_bucket(
    params: CompactPartitionParams,
    uniform_deltas: List[DeltaAnnotated],
) -> tuple[List[HashBucketResult], float]:

    hb_options_provider = functools.partial(
        task_resource_options_provider,
        pg_config=params.pg_config,
        resource_amount_provider=hash_bucket_resource_options_provider,
        previous_inflation=params.previous_inflation,
        average_record_size_bytes=params.average_record_size_bytes,
        total_memory_buffer_percentage=params.total_memory_buffer_percentage,
        primary_keys=params.primary_keys,
        ray_custom_resources=params.ray_custom_resources,
        memory_logs_enabled=params.memory_logs_enabled,
        estimate_resources_params=params.estimate_resources_params,
    )

    def hash_bucket_input_provider(index, item) -> dict[str, HashBucketInput]:
        return {
            "input": HashBucketInput.of(
                item,
                primary_keys=params.primary_keys,
                hb_task_index=index,
                num_hash_buckets=params.hash_bucket_count,
                num_hash_groups=params.hash_group_count,
                enable_profiler=params.enable_profiler,
                metrics_config=params.metrics_config,
                read_kwargs_provider=params.read_kwargs_provider,
                object_store=params.object_store,
                deltacat_storage=params.deltacat_storage,
                deltacat_storage_kwargs=params.deltacat_storage_kwargs,
                memory_logs_enabled=params.memory_logs_enabled,
            )
        }

    hb_tasks_pending = invoke_parallel(
        items=uniform_deltas,
        ray_task=hb.hash_bucket,
        max_parallelism=params.task_max_parallelism,
        options_provider=hb_options_provider,
        kwargs_provider=hash_bucket_input_provider,
    )
    hb_invoke_end = time.monotonic()

    logger.info(f"Getting {len(hb_tasks_pending)} hash bucket results...")
    hb_results: List[HashBucketResult] = ray.get(hb_tasks_pending)
    logger.info(f"Got {len(hb_results)} hash bucket results.")

    return (hb_results, hb_invoke_end)


def _run_local_merge(
    params: CompactPartitionParams,
    uniform_deltas: List[DeltaAnnotated],
    compacted_partition: Partition,
    round_completion_info: RoundCompletionInfo,
    delete_strategy: Optional[DeleteStrategy],
    delete_file_envelopes: Optional[DeleteFileEnvelope],
    mutable_compaction_audit: CompactionSessionAuditInfo,
    previous_compacted_delta_manifest: Optional[Manifest],
    total_input_records_count: np.int64,
) -> tuple[List[MergeResult], np.int64]:
    local_merge_input: MergeInput = generate_local_merge_input(
        params,
        uniform_deltas,
        compacted_partition,
        round_completion_info,
        delete_strategy,
        delete_file_envelopes,
    )
    estimated_da_bytes = (
        mutable_compaction_audit.estimated_in_memory_size_bytes_during_discovery
    )
    estimated_num_records: int = sum(
        [
            entry.meta.record_count
            for delta in uniform_deltas
            for entry in delta.manifest.entries
        ]
    )
    local_merge_options = local_merge_resource_options_provider(
        estimated_da_size=estimated_da_bytes,
        estimated_num_rows=estimated_num_records,
        total_memory_buffer_percentage=params.total_memory_buffer_percentage,
        round_completion_info=round_completion_info,
        compacted_delta_manifest=previous_compacted_delta_manifest,
        ray_custom_resources=params.ray_custom_resources,
        primary_keys=params.primary_keys,
        memory_logs_enabled=params.memory_logs_enabled,
        estimate_resources_params=params.estimate_resources_params,
    )
    local_merge_result = ray.get(
        mg.merge.options(**local_merge_options).remote(local_merge_input)
    )
    total_input_records_count += local_merge_result.input_record_count
    merge_results = [local_merge_result]
    return merge_results, total_input_records_count


def _process_merge_results(
    params: CompactPartitionParams,
    merge_results: List[MergeResult],
    mutable_compaction_audit: CompactionSessionAuditInfo,
) -> tuple[Delta, List[MaterializeResult], dict]:
    mat_results = []
    for merge_result in merge_results:
        mat_results.extend(merge_result.materialize_results)

    mat_results: List[MaterializeResult] = sorted(
        mat_results, key=lambda m: m.task_index
    )
    hb_id_to_entry_indices_range = {}
    file_index = 0
    previous_task_index = -1

    duplicate_hash_bucket_mat_results = 0
    for mat_result in mat_results:
        assert (
            mat_result.pyarrow_write_result.files >= 1
        ), "At least one file must be materialized"
        if mat_result.task_index == previous_task_index:
            duplicate_hash_bucket_mat_results += 1
        else:
            duplicate_hash_bucket_mat_results = 0
        assert duplicate_hash_bucket_mat_results < params.num_rounds, (
            f"Duplicate record count ({duplicate_hash_bucket_mat_results}) is as large "
            f"as or greater than params.num_rounds, which is {params.num_rounds}"
        )
        # ensure start index is the first file index if task index is same
        hb_id_to_entry_indices_range[str(mat_result.task_index)] = (
            hb_id_to_entry_indices_range.get(str(mat_result.task_index), [file_index])[
                0
            ],
            file_index + mat_result.pyarrow_write_result.files,
        )

        file_index += mat_result.pyarrow_write_result.files
        previous_task_index = mat_result.task_index

    s3_utils.upload(
        mutable_compaction_audit.audit_url,
        str(json.dumps(mutable_compaction_audit)),
        **params.s3_client_kwargs,
    )
    deltas: List[Delta] = [m.delta for m in mat_results]
    # Note: An appropriate last stream position must be set
    # to avoid correctness issue.
    merged_delta: Delta = Delta.merge_deltas(
        deltas,
        stream_position=params.last_stream_position_to_compact,
    )

    return merged_delta, mat_results, hb_id_to_entry_indices_range


def _update_and_upload_compaction_audit(
    params: CompactPartitionParams,
    mutable_compaction_audit: CompactionSessionAuditInfo,
    round_completion_info: Optional[RoundCompletionInfo] = None,
) -> None:

    # After all incremental delta related calculations, we update
    # the input sizes to accommodate the compacted table
    if round_completion_info:
        mutable_compaction_audit.set_input_file_count(
            (mutable_compaction_audit.input_file_count or 0)
            + round_completion_info.compacted_pyarrow_write_result.files
        )
        mutable_compaction_audit.set_input_size_bytes(
            (mutable_compaction_audit.input_size_bytes or 0.0)
            + round_completion_info.compacted_pyarrow_write_result.file_bytes
        )
        mutable_compaction_audit.set_input_records(
            (mutable_compaction_audit.input_records or 0)
            + round_completion_info.compacted_pyarrow_write_result.records
        )

    s3_utils.upload(
        mutable_compaction_audit.audit_url,
        str(json.dumps(mutable_compaction_audit)),
        **params.s3_client_kwargs,
    )
    return


def _write_new_round_completion_file(
    params: CompactPartitionParams,
    mutable_compaction_audit: CompactionSessionAuditInfo,
    compacted_partition: Partition,
    audit_url: str,
    hb_id_to_entry_indices_range: dict,
    rcf_source_partition_locator: rcf.PartitionLocator,
    new_compacted_delta_locator: DeltaLocator,
    pyarrow_write_result: PyArrowWriteResult,
    prev_round_completion_info: Optional[RoundCompletionInfo] = None,
) -> ExecutionCompactionResult:
    input_inflation = None
    input_average_record_size_bytes = None
    # Note: we only consider inflation for incremental delta
    if (
        mutable_compaction_audit.input_size_bytes
        and mutable_compaction_audit.hash_bucket_processed_size_bytes
    ):
        input_inflation = (
            mutable_compaction_audit.hash_bucket_processed_size_bytes
            / mutable_compaction_audit.input_size_bytes
        )

    if (
        mutable_compaction_audit.hash_bucket_processed_size_bytes
        and mutable_compaction_audit.input_records
    ):
        input_average_record_size_bytes = (
            mutable_compaction_audit.hash_bucket_processed_size_bytes
            / mutable_compaction_audit.input_records
        )

    logger.info(
        f"The inflation of input deltas={input_inflation}"
        f" and average record size={input_average_record_size_bytes}"
    )

    mutable_compaction_audit.set_observed_input_inflation(input_inflation)
    mutable_compaction_audit.set_observed_input_average_record_size_bytes(
        input_average_record_size_bytes
    )

    _update_and_upload_compaction_audit(
        params,
        mutable_compaction_audit,
        prev_round_completion_info,
    )

    new_round_completion_info = RoundCompletionInfo.of(
        high_watermark=params.last_stream_position_to_compact,
        compacted_delta_locator=new_compacted_delta_locator,
        compacted_pyarrow_write_result=pyarrow_write_result,
        sort_keys_bit_width=params.bit_width_of_sort_keys,
        manifest_entry_copied_by_reference_ratio=mutable_compaction_audit.untouched_file_ratio,
        compaction_audit_url=audit_url,
        hash_bucket_count=params.hash_bucket_count,
        hb_index_to_entry_range=hb_id_to_entry_indices_range,
        compactor_version=CompactorVersion.V2.value,
        input_inflation=input_inflation,
        input_average_record_size_bytes=input_average_record_size_bytes,
    )

    logger.info(
        f"Partition-{params.source_partition_locator.partition_values},"
        f"compacted at: {params.last_stream_position_to_compact},"
    )
    logger.info(
        f"Checking if partition {rcf_source_partition_locator} is inplace compacted against {params.destination_partition_locator}..."
    )
    is_inplace_compacted: bool = (
        rcf_source_partition_locator.partition_values
        == params.destination_partition_locator.partition_values
        and rcf_source_partition_locator.stream_id
        == params.destination_partition_locator.stream_id
    )
    if is_inplace_compacted:
        logger.info(
            "Overriding round completion file source partition locator as in-place compacted. "
            + f"Got compacted partition partition_id of {compacted_partition.locator.partition_id} "
            f"and rcf source partition_id of {rcf_source_partition_locator.partition_id}."
        )
        rcf_source_partition_locator = compacted_partition.locator

    round_completion_file_s3_url = rcf.write_round_completion_file(
        params.compaction_artifact_s3_bucket,
        rcf_source_partition_locator,
        compacted_partition.locator,
        new_round_completion_info,
        **params.s3_client_kwargs,
    )

    return ExecutionCompactionResult(
        compacted_partition,
        new_round_completion_info,
        round_completion_file_s3_url,
        is_inplace_compacted,
    )


def _commit_compaction_result(
    params: CompactPartitionParams,
    execute_compaction_result: ExecutionCompactionResult,
) -> None:
    compaction_session_type: str = (
        "INPLACE" if execute_compaction_result.is_inplace_compacted else "NON-INPLACE"
    )
    logger.info(
        f"Partition-{params.source_partition_locator} -> "
        f"{compaction_session_type} Compaction session data processing completed"
    )
    if execute_compaction_result.new_compacted_partition:
        previous_partition: Optional[Partition] = None
        if execute_compaction_result.is_inplace_compacted:
            previous_partition: Optional[
                Partition
            ] = params.deltacat_storage.get_partition(
                params.source_partition_locator.stream_locator,
                params.source_partition_locator.partition_values,
                **params.deltacat_storage_kwargs,
            )
            # NOTE: Retrieving the previous partition again as the partition_id may have changed by the time commit_partition is called.
        logger.info(
            f"Committing compacted partition to: {execute_compaction_result.new_compacted_partition.locator} "
            f"using previous partition: {previous_partition.locator if previous_partition else None}"
        )
        committed_partition: Partition = params.deltacat_storage.commit_partition(
            execute_compaction_result.new_compacted_partition,
            previous_partition,
            **params.deltacat_storage_kwargs,
        )
        logger.info(f"Committed compacted partition: {committed_partition}")
    else:
        logger.warning("No new partition was committed during compaction.")

    logger.info(f"Completed compaction session for: {params.source_partition_locator}")
