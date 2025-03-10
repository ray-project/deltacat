import importlib
import logging
import time
from contextlib import nullcontext
from typing import List, Optional, Tuple
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
import numpy as np
import ray
from deltacat import logs
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor_v2.utils.delta import read_delta_file_envelopes
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    group_hash_bucket_indices,
    group_by_pk_hash_bucket,
)
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics, failure_metric, success_metric
from deltacat.utils.resources import (
    get_current_process_peak_memory_usage_in_bytes,
    ProcessUtilizationOverTimeRange,
)
from deltacat.constants import BYTES_PER_GIBIBYTE
from deltacat.compute.compactor_v2.constants import (
    HASH_BUCKET_TIME_IN_SECONDS,
    HASH_BUCKET_FAILURE_COUNT,
    HASH_BUCKET_SUCCESS_COUNT,
)
from deltacat.exceptions import (
    categorize_errors,
)

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _group_file_records_by_pk_hash_bucket(
    annotated_delta: DeltaAnnotated,
    num_hash_buckets: int,
    primary_keys: List[str],
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
) -> Tuple[Optional[DeltaFileEnvelopeGroups], int, int]:
    # read input parquet s3 objects into a list of delta file envelopes
    (
        delta_file_envelopes,
        total_record_count,
        total_size_bytes,
    ) = read_delta_file_envelopes(
        annotated_delta,
        read_kwargs_provider,
        deltacat_storage,
        deltacat_storage_kwargs,
    )

    if delta_file_envelopes is None:
        return None, 0, 0

    logger.info(
        f"Read all delta file envelopes: {len(delta_file_envelopes)} "
        f"and total_size_bytes={total_size_bytes} and records={total_record_count}"
    )

    # group the data by primary key hash value
    hb_to_delta_file_envelopes = np.empty([num_hash_buckets], dtype="object")
    for dfe in delta_file_envelopes:
        logger.info("Grouping by pk hash bucket")
        group_start = time.monotonic()
        hash_bucket_to_table = group_by_pk_hash_bucket(
            table=dfe.table, num_buckets=num_hash_buckets, primary_keys=primary_keys
        )
        group_end = time.monotonic()
        logger.info(f"Grouping took: {group_end - group_start}")
        for hb, table in enumerate(hash_bucket_to_table):
            if table:
                if hb_to_delta_file_envelopes[hb] is None:
                    hb_to_delta_file_envelopes[hb] = []
                hb_to_delta_file_envelopes[hb].append(
                    DeltaFileEnvelope.of(
                        stream_position=dfe.stream_position,
                        file_index=dfe.file_index,
                        delta_type=dfe.delta_type,
                        table=table,
                    )
                )
    return hb_to_delta_file_envelopes, total_record_count, total_size_bytes


@success_metric(name=HASH_BUCKET_SUCCESS_COUNT)
@failure_metric(name=HASH_BUCKET_FAILURE_COUNT)
@categorize_errors
def _timed_hash_bucket(input: HashBucketInput):
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"hash_bucket_{worker_id}_{task_id}.bin"
    ) if input.enable_profiler else nullcontext():
        (
            delta_file_envelope_groups,
            total_record_count,
            total_size_bytes,
        ) = _group_file_records_by_pk_hash_bucket(
            annotated_delta=input.annotated_delta,
            num_hash_buckets=input.num_hash_buckets,
            primary_keys=input.primary_keys,
            read_kwargs_provider=input.read_kwargs_provider,
            deltacat_storage=input.deltacat_storage,
            deltacat_storage_kwargs=input.deltacat_storage_kwargs,
        )
        hash_bucket_group_to_obj_id_tuple = group_hash_bucket_indices(
            hash_bucket_object_groups=delta_file_envelope_groups,
            num_buckets=input.num_hash_buckets,
            num_groups=input.num_hash_groups,
            object_store=input.object_store,
        )

        peak_memory_usage_bytes = get_current_process_peak_memory_usage_in_bytes()
        logger.info(
            f"Peak memory usage in bytes after hash bucketing: {peak_memory_usage_bytes}"
        )
        return HashBucketResult(
            hash_bucket_group_to_obj_id_tuple,
            np.int64(total_size_bytes),
            np.int64(total_record_count),
            np.double(peak_memory_usage_bytes),
            np.double(0.0),
            np.double(time.time()),
        )


@ray.remote
def hash_bucket(input: HashBucketInput) -> HashBucketResult:
    with ProcessUtilizationOverTimeRange() as process_util:
        logger.info(f"Starting hash bucket task {input.hb_task_index}...")

        # Log node peak memory utilization every 10 seconds
        def log_peak_memory():
            logger.debug(
                f"Process peak memory utilization so far: {process_util.max_memory} bytes "
                f"({process_util.max_memory/BYTES_PER_GIBIBYTE} GB)"
            )

        if input.memory_logs_enabled:
            process_util.schedule_callback(log_peak_memory, 10)

        hash_bucket_result, duration = timed_invocation(
            func=_timed_hash_bucket, input=input
        )

        emit_metrics_time = 0.0
        if input.metrics_config:
            emit_result, latency = timed_invocation(
                func=emit_timer_metrics,
                metrics_name=HASH_BUCKET_TIME_IN_SECONDS,
                value=duration,
                metrics_config=input.metrics_config,
            )
            emit_metrics_time = latency

        logger.info(f"Finished hash bucket task {input.hb_task_index}...")
        return HashBucketResult(
            hash_bucket_result[0],
            hash_bucket_result[1],
            hash_bucket_result[2],
            hash_bucket_result[3],
            np.double(emit_metrics_time),
            hash_bucket_result[5],
        )
