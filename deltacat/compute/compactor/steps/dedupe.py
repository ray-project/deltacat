import importlib
import logging
from typing import Optional
import time
from collections import defaultdict
from contextlib import nullcontext
from typing import Any, Dict, List, Tuple
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray

from deltacat import logs
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
    DeltaFileLocator,
)
from deltacat.storage.model.sort_key import SortKey, SortOrder
from deltacat.compute.compactor.model.dedupe_result import DedupeResult
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics, MetricsConfig
from deltacat.io.object_store import IObjectStore
from deltacat.utils.resources import get_current_process_peak_memory_usage_in_bytes

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

MaterializeBucketIndex = int
DeltaFileLocatorToRecords = Dict[DeltaFileLocator, np.ndarray]
DedupeTaskIndex, PickledObjectRef = int, str
DedupeTaskIndexWithObjectId = Tuple[DedupeTaskIndex, PickledObjectRef]


def _union_primary_key_indices(
    hash_bucket_index: int, df_envelopes_list: List[List[DeltaFileEnvelope]]
) -> pa.Table:
    logger.info(
        f"[Hash bucket index {hash_bucket_index}] Reading dedupe input for "
        f"{len(df_envelopes_list)} delta file envelope lists..."
    )
    hb_tables = []
    # sort by delta file stream position now instead of sorting every row later
    df_envelopes = [d for dfe_list in df_envelopes_list for d in dfe_list]
    df_envelopes = sorted(
        df_envelopes,
        key=lambda df: (df.stream_position, df.file_index),
        reverse=False,  # ascending
    )
    for df_envelope in df_envelopes:
        hb_tables.append(sc.project_delta_file_metadata_on_table(df_envelope))

    hb_table = pa.concat_tables(hb_tables)

    logger.info(
        f"Total records in hash bucket {hash_bucket_index} is {hb_table.num_rows}"
    )
    return hb_table


def _drop_duplicates_by_primary_key_hash(table: pa.Table) -> pa.Table:
    value_to_last_row_idx = {}

    pk_hash_np = sc.pk_hash_column_np(table)
    op_type_np = sc.delta_type_column_np(table)

    assert len(pk_hash_np) == len(op_type_np), (
        f"Primary key digest column length ({len(pk_hash_np)}) doesn't "
        f"match delta type column length ({len(op_type_np)})."
    )

    # TODO(raghumdani): move the dedupe to C++ using arrow methods or similar.
    row_idx = 0
    pk_op_val_iter = zip(pk_hash_np, op_type_np)
    for (pk_val, op_val) in pk_op_val_iter:

        # operation type is True for `UPSERT` and False for `DELETE`
        if op_val:
            # UPSERT this row
            value_to_last_row_idx[pk_val] = row_idx
        else:
            # DELETE this row
            value_to_last_row_idx.pop(pk_val, None)

        row_idx += 1

    return table.take(list(value_to_last_row_idx.values()))


def delta_file_locator_to_mat_bucket_index(
    df_locator: DeltaFileLocator, materialize_bucket_count: int
) -> int:
    digest = df_locator.digest()
    return int.from_bytes(digest, "big") % materialize_bucket_count


def _timed_dedupe(
    object_ids: List[Any],
    sort_keys: List[SortKey],
    num_materialize_buckets: int,
    dedupe_task_index: int,
    enable_profiler: bool,
    object_store: Optional[IObjectStore],
    **kwargs,
):
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"dedupe_{worker_id}_{task_id}.bin"
    ) if enable_profiler else nullcontext():
        # TODO (pdames): mitigate risk of running out of memory here in cases of severe skew of primary key updates in deltas
        logger.info(
            f"[Dedupe task {dedupe_task_index}] Getting delta file envelope "
            f"groups for {len(object_ids)} object refs..."
        )
        delta_file_envelope_groups_list: List[object] = object_store.get_many(
            object_ids
        )
        hb_index_to_delta_file_envelopes_list = defaultdict(list)
        for delta_file_envelope_groups in delta_file_envelope_groups_list:
            for hb_idx, dfes in enumerate(delta_file_envelope_groups):
                if dfes is not None:
                    hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)
        src_file_id_to_row_indices = defaultdict(list)
        deduped_tables = []
        logger.info(
            f"[Dedupe task {dedupe_task_index}] Running {len(hb_index_to_delta_file_envelopes_list)} "
            f"dedupe rounds..."
        )
        total_deduped_records = 0
        for hb_idx, dfe_list in hb_index_to_delta_file_envelopes_list.items():
            logger.info(
                f"{dedupe_task_index}: union primary keys for hb_index: {hb_idx}"
            )

            table, union_time = timed_invocation(
                func=_union_primary_key_indices,
                hash_bucket_index=hb_idx,
                df_envelopes_list=dfe_list,
            )
            logger.info(
                f"[Dedupe {dedupe_task_index}] Dedupe round input "
                f"record count: {len(table)}, took {union_time}s"
            )

            # sort by sort keys
            if len(sort_keys):
                # TODO (pdames): convert to O(N) dedupe w/ sort keys
                sort_keys.extend(
                    [
                        SortKey.of(
                            sc._PARTITION_STREAM_POSITION_COLUMN_NAME,
                            SortOrder.ASCENDING,
                        ),
                        SortKey.of(
                            sc._ORDERED_FILE_IDX_COLUMN_NAME, SortOrder.ASCENDING
                        ),
                    ]
                )
                table = table.take(pc.sort_indices(table, sort_keys=sort_keys))

            # drop duplicates by primary key hash column
            logger.info(
                f"[Dedupe task index {dedupe_task_index}] Dropping duplicates for {hb_idx}"
            )

            hb_table_record_count = len(table)
            table, drop_time = timed_invocation(
                func=_drop_duplicates_by_primary_key_hash,
                table=table,
            )
            deduped_record_count = hb_table_record_count - len(table)
            total_deduped_records += deduped_record_count

            logger.info(
                f"[Dedupe task index {dedupe_task_index}] Dedupe round output "
                f"record count: {len(table)}, took: {drop_time}s"
            )

            deduped_tables.append((hb_idx, table))

            stream_position_col = sc.stream_position_column_np(table)
            file_idx_col = sc.file_index_column_np(table)
            row_idx_col = sc.record_index_column_np(table)
            is_source_col = sc.is_source_column_np(table)
            file_record_count_col = sc.file_record_count_column_np(table)
            for row_idx in range(len(table)):
                src_dfl = DeltaFileLocator.of(
                    is_source_col[row_idx],
                    stream_position_col[row_idx],
                    file_idx_col[row_idx],
                    file_record_count_col[row_idx],
                )
                # TODO(pdames): merge contiguous record number ranges
                src_file_id_to_row_indices[src_dfl].append(row_idx_col[row_idx])

        logger.info(f"Finished all dedupe rounds...")
        mat_bucket_to_src_file_records: Dict[
            MaterializeBucketIndex, DeltaFileLocatorToRecords
        ] = defaultdict(dict)
        for src_dfl, src_row_indices in src_file_id_to_row_indices.items():
            mat_bucket = delta_file_locator_to_mat_bucket_index(
                src_dfl,
                num_materialize_buckets,
            )
            mat_bucket_to_src_file_records[mat_bucket][src_dfl] = np.array(
                src_row_indices,
            )

        mat_bucket_to_dd_idx_obj_id: Dict[
            MaterializeBucketIndex, DedupeTaskIndexWithObjectId
        ] = {}
        for mat_bucket, src_file_records in mat_bucket_to_src_file_records.items():
            object_ref = object_store.put(src_file_records)
            mat_bucket_to_dd_idx_obj_id[mat_bucket] = (
                dedupe_task_index,
                object_ref,
            )
            del object_ref
        logger.info(
            f"Count of materialize buckets with object refs: "
            f"{len(mat_bucket_to_dd_idx_obj_id)}"
        )

        peak_memory_usage_bytes = get_current_process_peak_memory_usage_in_bytes()
        return DedupeResult(
            mat_bucket_to_dd_idx_obj_id,
            np.int64(total_deduped_records),
            np.double(peak_memory_usage_bytes),
            np.double(0.0),
            np.double(time.time()),
        )


@ray.remote
def dedupe(
    object_ids: List[Any],
    sort_keys: List[SortKey],
    num_materialize_buckets: int,
    dedupe_task_index: int,
    enable_profiler: bool,
    metrics_config: MetricsConfig,
    object_store: Optional[IObjectStore],
    **kwargs,
) -> DedupeResult:
    logger.info(f"[Dedupe task {dedupe_task_index}] Starting dedupe task...")
    dedupe_result, duration = timed_invocation(
        func=_timed_dedupe,
        object_ids=object_ids,
        sort_keys=sort_keys,
        num_materialize_buckets=num_materialize_buckets,
        dedupe_task_index=dedupe_task_index,
        enable_profiler=enable_profiler,
        object_store=object_store,
        **kwargs,
    )

    emit_metrics_time = 0.0
    if metrics_config:
        emit_result, latency = timed_invocation(
            func=emit_timer_metrics,
            metrics_name="dedupe",
            value=duration,
            metrics_config=metrics_config,
        )
        emit_metrics_time = latency

    logger.info(f"[Dedupe task index {dedupe_task_index}] Finished dedupe task...")
    return DedupeResult(
        dedupe_result[0],
        dedupe_result[1],
        dedupe_result[2],
        np.double(emit_metrics_time),
        dedupe_result[4],
    )
