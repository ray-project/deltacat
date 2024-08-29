import importlib
from contextlib import nullcontext
import logging
import time
import ray

import deltacat
from deltacat.compute.compactor import (
    PyArrowWriteResult,
    RoundCompletionInfo,
)
from deltacat import logs
from deltacat.compute.compactor_v2.model.evaluate_compaction_result import (
    ExecutionCompactionResult,
)
from deltacat.compute.compactor.model.compactor_version import CompactorVersion
from deltacat.compute.compactor.utils import round_completion_file as rcf
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.compute.compactor_v2.deletes.delete_strategy import (
    DeleteStrategy,
)
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from deltacat.storage import (
    Delta,
    DeltaLocator,
    Manifest,
    Partition,
)
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.utils.resources import (
    get_current_process_peak_memory_usage_in_bytes,
)
from deltacat.compute.compactor_v2.private.compaction_utils import (
    _fetch_compaction_metadata,
    _build_uniform_deltas,
    _group_uniform_deltas,
    _stage_new_partition,
    _run_hash_and_merge,
    _process_merge_results,
    _write_new_round_completion_file,
    _commit_compaction_result,
)
from deltacat.utils.metrics import metrics
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)

from typing import List, Optional
from deltacat.compute.compactor_v2.utils import io
from deltacat.exceptions import categorize_errors
from deltacat.compute.compactor_v2.constants import COMPACT_PARTITION_METRIC_PREFIX

if importlib.util.find_spec("memray"):
    import memray


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@metrics(prefix=COMPACT_PARTITION_METRIC_PREFIX)
@categorize_errors
def compact_partition(params: CompactPartitionParams, **kwargs) -> Optional[str]:
    assert (
        params.hash_bucket_count is not None and params.hash_bucket_count >= 1
    ), "hash_bucket_count is a required arg for compactor v2"
    if params.num_rounds > 1:
        assert (
            not params.drop_duplicates
        ), "num_rounds > 1, drop_duplicates must be False but is True"

    with memray.Tracker(
        "compaction_partition.bin"
    ) if params.enable_profiler else nullcontext():
        execute_compaction_result: ExecutionCompactionResult = _execute_compaction(
            params,
            **kwargs,
        )
        _commit_compaction_result(params, execute_compaction_result)
        return execute_compaction_result.round_completion_file_s3_url


def _execute_compaction(
    params: CompactPartitionParams, **kwargs
) -> ExecutionCompactionResult:
    compaction_start_time: float = time.monotonic()
    # Fetch round completion info for previously compacted partition, if it exists
    fetch_compaction_metadata_result: tuple[
        Optional[Manifest], Optional[RoundCompletionInfo]
    ] = _fetch_compaction_metadata(params)
    (
        previous_compacted_delta_manifest,
        round_completion_info,
    ) = fetch_compaction_metadata_result
    rcf_source_partition_locator: rcf.PartitionLocator = (
        params.rebase_source_partition_locator or params.source_partition_locator
    )

    base_audit_url: str = rcf_source_partition_locator.path(
        f"s3://{params.compaction_artifact_s3_bucket}/compaction-audit"
    )
    audit_url: str = f"{base_audit_url}.json"
    logger.info(f"Compaction audit will be written to {audit_url}")
    compaction_audit: CompactionSessionAuditInfo = (
        CompactionSessionAuditInfo(deltacat.__version__, ray.__version__, audit_url)
        .set_hash_bucket_count(params.hash_bucket_count)
        .set_compactor_version(CompactorVersion.V2.value)
    )

    if params.pg_config:
        logger.info(
            "pg_config specified. Tasks will be scheduled in a placement group."
        )
        cluster_resources = params.pg_config.resource
        cluster_memory = cluster_resources["memory"]
        compaction_audit.set_total_cluster_memory_bytes(cluster_memory)
    high_watermark = (
        round_completion_info.high_watermark if round_completion_info else None
    )
    audit_url = compaction_audit.audit_url if compaction_audit else None
    # discover and build uniform deltas
    delta_discovery_start = time.monotonic()
    input_deltas: List[Delta] = io.discover_deltas(
        params.source_partition_locator,
        params.last_stream_position_to_compact,
        params.rebase_source_partition_locator,
        params.rebase_source_partition_high_watermark,
        high_watermark,
        params.deltacat_storage,
        params.deltacat_storage_kwargs,
        params.list_deltas_kwargs,
    )
    if not input_deltas:
        logger.info("No input deltas found to compact.")
        return ExecutionCompactionResult(None, None, None, False)
    build_uniform_deltas_result: tuple[
        List[DeltaAnnotated], DeleteStrategy, List[DeleteFileEnvelope], Partition
    ] = _build_uniform_deltas(
        params, compaction_audit, input_deltas, delta_discovery_start
    )
    (
        uniform_deltas,
        delete_strategy,
        delete_file_envelopes,
    ) = build_uniform_deltas_result
    logger.info(f"Number of rounds parameter is set to: {params.num_rounds}")
    uniform_deltas_grouped = _group_uniform_deltas(params, uniform_deltas)
    logger.info(f"Length of grouped uniform deltas is: {len(uniform_deltas_grouped)}")
    merge_result_list: List[MergeResult] = []
    compacted_partition = _stage_new_partition(params)
    for uniform_deltas in uniform_deltas_grouped:
        # run hash and merge
        _run_hash_and_merge_result: List[MergeResult] = _run_hash_and_merge(
            params,
            uniform_deltas,
            round_completion_info,
            delete_strategy,
            delete_file_envelopes,
            compaction_audit,
            previous_compacted_delta_manifest,
            compacted_partition,
        )
        merge_result_list.extend(_run_hash_and_merge_result)
    # process merge results
    process_merge_results: tuple[
        Delta, list[MaterializeResult], dict
    ] = _process_merge_results(params, merge_result_list, compaction_audit)
    merged_delta, mat_results, hb_id_to_entry_indices_range = process_merge_results
    # Record information, logging, and return ExecutionCompactionResult
    record_info_msg: str = f" Materialized records: {merged_delta.meta.record_count}"
    logger.info(record_info_msg)
    compacted_delta: Delta = params.deltacat_storage.commit_delta(
        merged_delta,
        properties=kwargs.get("properties", {}),
        **params.deltacat_storage_kwargs,
    )

    logger.info(f"Committed compacted delta: {compacted_delta}")
    compaction_end_time: float = time.monotonic()
    compaction_audit.set_compaction_time_in_seconds(
        compaction_end_time - compaction_start_time
    )
    new_compacted_delta_locator: DeltaLocator = DeltaLocator.of(
        compacted_partition.locator,
        compacted_delta.stream_position,
    )
    pyarrow_write_result: PyArrowWriteResult = PyArrowWriteResult.union(
        [m.pyarrow_write_result for m in mat_results]
    )

    session_peak_memory = get_current_process_peak_memory_usage_in_bytes()
    compaction_audit.set_peak_memory_used_bytes_by_compaction_session_process(
        session_peak_memory
    )

    compaction_audit.save_round_completion_stats(mat_results)

    compaction_result: ExecutionCompactionResult = _write_new_round_completion_file(
        params,
        compaction_audit,
        compacted_partition,
        audit_url,
        hb_id_to_entry_indices_range,
        rcf_source_partition_locator,
        new_compacted_delta_locator,
        pyarrow_write_result,
        round_completion_info,
    )
    return compaction_result
