import logging
import pyarrow as pa
import ray
import time
import pyarrow.compute as pc
import numpy as np
from deltacat.compute.compactor.utils.system_columns import get_minimal_hb_schema
from deltacat.utils.pyarrow import ReadKwargsProviderPyArrowSchemaOverride
from ray import cloudpickle
from ray.types import ObjectRef
import sys

from deltacat import logs
from collections import defaultdict
from itertools import repeat
from deltacat.storage import DeltaType
from deltacat.compute.compactor import SortKey, SortOrder, \
    RoundCompletionInfo, PrimaryKeyIndexVersionLocator, DeltaFileEnvelope, \
    DeltaFileLocator, PyArrowWriteResult
from deltacat.compute.compactor.utils import system_columns as sc, \
    primary_key_index as pki

from typing import Any, Dict, List, Optional, Tuple
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


MaterializeBucketIndex = int
DeltaFileLocatorToRecords = Dict[DeltaFileLocator, np.ndarray]
DedupeTaskIndex, PickledObjectRef = int, str
DedupeTaskIndexWithObjectId = Tuple[DedupeTaskIndex, PickledObjectRef]
DedupeResult = Tuple[
    Dict[MaterializeBucketIndex, DedupeTaskIndexWithObjectId],
    List[ObjectRef[DeltaFileLocatorToRecords]],
    PyArrowWriteResult
]


def union_primary_key_indices(
        s3_bucket: str,
        round_completion_info: RoundCompletionInfo,
        hash_bucket_index: int,
        df_envelopes_list: List[List[DeltaFileEnvelope]]) -> pa.Table:

    logger.info(f"Reading dedupe input for {len(df_envelopes_list)} "
                f"delta file envelope lists...")
    # read compacted input parquet files first
    # (which implicitly have older stream positions than deltas)
    hb_tables = []
    if round_completion_info:
        tables = pki.download_hash_bucket_entries(
            s3_bucket,
            hash_bucket_index,
            round_completion_info.primary_key_index_version_locator,
            # Enforce consistent column ordering by reading from a schema, to prevent schema mismatch errors
            file_reader_kwargs_provider=ReadKwargsProviderPyArrowSchemaOverride(schema=get_minimal_hb_schema())
        )
        if tables:
            prior_pk_index_table = pa.concat_tables(tables)
            hb_tables.append(prior_pk_index_table)

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

    return hb_table


def drop_duplicates_by_primary_key_hash(table: pa.Table) -> pa.Table:
    value_to_last_row_idx = {}
    row_idx = 0
    pk_op_chunk_iter = zip(
        sc.pk_hash_column(table).iterchunks(),
        sc.delta_type_column(table).iterchunks(),
    )
    for (pk_chunk, op_chunk) in pk_op_chunk_iter:
        pk_op_val_iter = zip(
            pk_chunk.to_numpy(zero_copy_only=False),
            op_chunk.to_numpy(zero_copy_only=False),
        )
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


def write_new_primary_key_index(
        s3_bucket: str,
        new_primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
        max_rows_per_index_file: int,
        max_rows_per_mat_file: int,
        num_materialize_buckets: int,
        dedupe_task_index: int,
        deduped_tables: List[Tuple[int, pa.Table]]) -> PyArrowWriteResult:

    logger.info(f"Writing new deduped primary key index: "
                f"{new_primary_key_index_version_locator}")

    pki_results = []
    for hb_index, table in deduped_tables:
        # this is always true, saving some space
        table = table.drop([sc._IS_SOURCE_COLUMN_NAME])
        hb_pki_result = pki.write_primary_key_index_files(
            table,
            new_primary_key_index_version_locator,
            s3_bucket,
            hb_index,
            max_rows_per_index_file,
        )
        pki_results.append(hb_pki_result)

    result = PyArrowWriteResult.union(pki_results)
    logger.info(f"Wrote new deduped primary key index: "
                f"{new_primary_key_index_version_locator}. Result: {result}")
    return result


def delta_file_locator_to_mat_bucket_index(
        df_locator: DeltaFileLocator,
        materialize_bucket_count: int) -> int:
    digest = df_locator.digest()
    return int.from_bytes(digest, "big") % materialize_bucket_count


@ray.remote(num_returns=3)
def dedupe(
        compaction_artifact_s3_bucket: str,
        round_completion_info: Optional[RoundCompletionInfo],
        new_primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
        object_ids: List[Any],
        sort_keys: List[SortKey],
        max_records_per_index_file: int,
        max_records_per_materialized_file: int,
        num_materialize_buckets: int,
        dedupe_task_index: int,
        delete_old_primary_key_index: bool) -> DedupeResult:

    logger.info(f"{dedupe_task_index}: Starting dedupe task...")
    # TODO (pdames): mitigate risk of running out of memory here in cases of
    #  severe skew of primary key updates in deltas
    src_file_records_obj_refs = [obj_id_pkl for obj_id_pkl in object_ids]
    logger.info(f"{dedupe_task_index}: Getting delta file envelope groups for {len(src_file_records_obj_refs)} object refs...")
    st = time.time()
    delta_file_envelope_groups_list = ray.get(src_file_records_obj_refs)
    ed = time.time()
    logger.info(f"{dedupe_task_index}: Got the delta_file_envelope_groups_list in {ed - st}s for {len(src_file_records_obj_refs)} refs")
    hb_index_to_delta_file_envelopes_list = defaultdict(list)
    for delta_file_envelope_groups in delta_file_envelope_groups_list:
        for hb_idx, dfes in enumerate(delta_file_envelope_groups):
            if dfes is not None:
                hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)
    src_file_id_to_row_indices = defaultdict(list)
    deduped_tables = []
    logger.info(f"{dedupe_task_index}:Running {len(hb_index_to_delta_file_envelopes_list)} "
                f"dedupe rounds...")
    for hb_idx, dfe_list in hb_index_to_delta_file_envelopes_list.items():
        logger.info(f"{dedupe_task_index}: union primary keys for hb_index: {hb_idx}")
        st = time.time()
        table = union_primary_key_indices(
            compaction_artifact_s3_bucket,
            round_completion_info,
            hb_idx,
            dfe_list,
        )
        ed = time.time()
        logger.info(f"{dedupe_task_index}: Dedupe round input record count: {len(table)} and took {ed - st}")

        # sort by sort keys
        if len(sort_keys):
            # TODO (pdames): convert to O(N) dedupe w/ sort keys
            sort_keys.extend([
                SortKey.of(
                    sc._PARTITION_STREAM_POSITION_COLUMN_NAME,
                    SortOrder.ASCENDING
                ),
                SortKey.of(
                    sc._ORDERED_FILE_IDX_COLUMN_NAME,
                    SortOrder.ASCENDING
                ),
            ])
            table = table.take(pc.sort_indices(table, sort_keys=sort_keys))

        # drop duplicates by primary key hash column
        logger.info(f"{dedupe_task_index}: Dropping duplicates for {hb_idx}")
        st = time.time()
        table = drop_duplicates_by_primary_key_hash(table)
        ed = time.time()
        table = table.drop([sc._DELTA_TYPE_COLUMN_NAME])
        logger.info(f"{dedupe_task_index}: Dedupe round output record count: {len(table)}, took: {ed - st}")

        deduped_tables.append((hb_idx, table))

        stream_position_col = sc.stream_position_column_np(table)
        file_idx_col = sc.file_index_column_np(table)
        row_idx_col = sc.record_index_column_np(table)
        is_source_col = sc.is_source_column_np(table)
        for row_idx in range(len(table)):
            src_dfl = DeltaFileLocator.of(
                is_source_col[row_idx],
                stream_position_col[row_idx],
                file_idx_col[row_idx],
            )
            # TODO(pdames): merge contiguous record number ranges
            src_file_id_to_row_indices[src_dfl].append(row_idx_col[row_idx])

    logger.info(f"{dedupe_task_index}: Finished all dedupe rounds...")
    mat_bucket_to_src_file_record_count = defaultdict(dict)
    mat_bucket_to_src_file_records: Dict[MaterializeBucketIndex, DeltaFileLocatorToRecords] = defaultdict(dict)
    for src_dfl, src_row_indices in src_file_id_to_row_indices.items():
        mat_bucket = delta_file_locator_to_mat_bucket_index(
            src_dfl,
            num_materialize_buckets,
        )
        mat_bucket_to_src_file_records[mat_bucket][src_dfl] = np.array(
            src_row_indices,
        )
        mat_bucket_to_src_file_record_count[mat_bucket][src_dfl] = \
            len(src_row_indices)

    mat_bucket_to_dd_idx_obj_id: Dict[MaterializeBucketIndex, DedupeTaskIndexWithObjectId] = {}
    src_file_records_obj_refs: List[ObjectRef[DeltaFileLocatorToRecords]] = []
    for mat_bucket, src_file_records in mat_bucket_to_src_file_records.items():
        object_ref = ray.put(src_file_records)
        src_file_records_obj_refs.append(object_ref)
        mat_bucket_to_dd_idx_obj_id[mat_bucket] = (
            dedupe_task_index,
            object_ref,
        )
        del object_ref
    logger.info(f"{dedupe_task_index}: Count of materialize buckets with object refs: "
                f"{len(mat_bucket_to_dd_idx_obj_id)}")

    write_pki_result: PyArrowWriteResult = write_new_primary_key_index(
        compaction_artifact_s3_bucket,
        new_primary_key_index_version_locator,
        max_records_per_index_file,
        max_records_per_materialized_file,
        num_materialize_buckets,
        dedupe_task_index,
        deduped_tables
    )

    if delete_old_primary_key_index:
        pki.delete_primary_key_index_version(
            compaction_artifact_s3_bucket,
            round_completion_info.primary_key_index_version_locator,
        )
    logger.info(f"Finished dedupe task...")
    return mat_bucket_to_dd_idx_obj_id, \
        src_file_records_obj_refs, \
        write_pki_result
