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

from deltacat.compute.compactor import (
    MaterializeResult,
    PyArrowWriteResult,
)

from deltacat.storage import (
    Partition,
)
from deltacat.storage import interface as unimplemented_deltacat_storage

from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES, ContentType
from deltacat.types.tables import TABLE_CLASS_TO_SIZE_FUNC

from deltacat import logs
from deltacat.compute.compactor import (
    SortKey,
    SortOrder,
    DeltaFileEnvelope,
    DeltaFileLocator,
)
from deltacat.compute.compactor.model.dedupe_result import DedupeResult
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics, MetricsConfig
from deltacat.io.object_store import IObjectStore
from deltacat.utils.resources import get_current_node_peak_memory_usage_in_bytes

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
    partition: Partition,
    enable_profiler: bool,
    max_records_per_output_file: int,
    compacted_file_content_type: ContentType,
    object_store: Optional[IObjectStore],
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
):

    # TODO (rkenmi): Add docstrings for the steps in the compaction workflow
    #  https://github.com/ray-project/deltacat/issues/79
    def _materialize(
        hash_bucket_index, compacted_tables: List[pa.Table]
    ) -> MaterializeResult:
        compacted_table = pa.concat_tables(compacted_tables)
        if compacted_file_content_type in DELIMITED_TEXT_CONTENT_TYPES:
            # TODO (rkenmi): Investigate if we still need to convert this table to pandas DataFrame
            # TODO (pdames): compare performance to pandas-native materialize path
            df = compacted_table.to_pandas(split_blocks=True, self_destruct=True)
            compacted_table = df
        delta, stage_delta_time = timed_invocation(
            deltacat_storage.stage_delta,
            compacted_table,
            partition,
            max_records_per_entry=max_records_per_output_file,
            content_type=compacted_file_content_type,
            s3_table_writer_kwargs=s3_table_writer_kwargs,
        )
        compacted_table_size = TABLE_CLASS_TO_SIZE_FUNC[type(compacted_table)](
            compacted_table
        )
        logger.debug(
            f"Time taken for materialize task"
            f" to upload {len(compacted_table)} records"
            f" of size {compacted_table_size} is: {stage_delta_time}s"
        )
        manifest = delta.manifest
        manifest_records = manifest.meta.record_count
        assert (
            manifest_records == len(compacted_table),
            f"Unexpected Error: Materialized delta manifest record count "
            f"({manifest_records}) does not equal compacted table record count "
            f"({len(compacted_table)})",
        )
        materialize_result = MaterializeResult.of(
            delta=delta,
            task_index=hash_bucket_index,
            # TODO (pdames): Generalize WriteResult to contain in-memory-table-type
            #  and in-memory-table-bytes instead of tight coupling to paBytes
            pyarrow_write_result=PyArrowWriteResult.of(
                len(manifest.entries),
                TABLE_CLASS_TO_SIZE_FUNC[type(compacted_table)](compacted_table),
                manifest.meta.content_length,
                len(compacted_table),
            ),
        )
        logger.info(f"Materialize result: {materialize_result}")
        return materialize_result

    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"dedupe_{worker_id}_{task_id}.bin"
    ) if enable_profiler else nullcontext():
        # TODO (pdames): mitigate risk of running out of memory here in cases of
        #  severe skew of primary key updates in deltas
        logger.info(
            f"[Dedupe task {dedupe_task_index}] Getting delta file envelope "
            f"groups for {len(object_ids)} object refs..."
        )

        delta_file_envelope_groups_list = object_store.get_many(object_ids)
        hb_index_to_delta_file_envelopes_list = defaultdict(list)
        for delta_file_envelope_groups in delta_file_envelope_groups_list:
            for hb_idx, dfes in enumerate(delta_file_envelope_groups):
                if dfes is not None:
                    hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)

        logger.info(
            f"[Dedupe task {dedupe_task_index}] Running {len(hb_index_to_delta_file_envelopes_list)} "
            f"dedupe rounds..."
        )
        total_deduped_records = 0

        materialized_results: List[MaterializeResult] = []

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
                func=_drop_duplicates_by_primary_key_hash, table=table
            )
            deduped_record_count = hb_table_record_count - len(table)
            total_deduped_records += deduped_record_count

            logger.info(
                f"[Dedupe task index {dedupe_task_index}] Dedupe round output "
                f"record count: {len(table)}, took: {drop_time}s"
            )

            materialized_results.append(_materialize(hb_idx, table))

        peak_memory_usage_bytes = get_current_node_peak_memory_usage_in_bytes()

        return DedupeResult(
            materialized_results,
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
    partition: Partition,
    enable_profiler: bool,
    max_records_per_output_file: int,
    compacted_file_content_type: ContentType,
    object_store: Optional[IObjectStore],
    metrics_config: MetricsConfig,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
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
        partition=partition,
        max_records_per_output_file=max_records_per_output_file,
        compacted_file_content_type=compacted_file_content_type,
        s3_table_writer_kwargs=s3_table_writer_kwargs,
        deltacat_storage=deltacat_storage,
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
