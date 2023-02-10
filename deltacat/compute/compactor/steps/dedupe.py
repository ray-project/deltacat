import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray
from ray import cloudpickle
from ray.types import ObjectRef
from ray._private.internal_api import free
from itertools import repeat

from deltacat import logs
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
    DeltaFileLocator,
    PrimaryKeyIndexVersionLocator,
    PyArrowWriteResult,
    RoundCompletionInfo,
    SortKey,
    SortOrder,
)
from deltacat.compute.compactor.utils import primary_key_index as pki
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.compute.compactor.utils.system_columns import get_minimal_hb_schema
from deltacat.storage import DeltaType
from deltacat.compute.compactor import SortKey, SortOrder, \
    RoundCompletionInfo, PrimaryKeyIndexVersionLocator, DeltaFileEnvelope, \
    DeltaFileLocator, PyArrowWriteResult
from deltacat.compute.compactor.utils import system_columns as sc, \
    primary_key_index as pki
from deltacat.utils.performance import timed_invocation

from typing import Any, Dict, List, Optional, Tuple
from deltacat.utils.pyarrow import ReadKwargsProviderPyArrowSchemaOverride
from deltacat.compute.compactor.utils.materialize_utils import delta_file_locator_to_mat_bucket_index

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


MaterializeBucketIndex = int
DeltaFileLocatorToRecords = Dict[DeltaFileLocator, np.ndarray]
DedupeTaskIndex, PickledObjectRef = int, str
DedupeTaskIndexWithObjectId = Tuple[DedupeTaskIndex, PickledObjectRef]
DedupeResult = Tuple[
    Dict[MaterializeBucketIndex, DedupeTaskIndexWithObjectId]
]

def _union_primary_key_indices(
        s3_bucket: str,
        round_completion_info: RoundCompletionInfo,
        hash_bucket_index: int,
        df_envelopes_list: List[List[DeltaFileEnvelope]]) -> pa.Table:

    logger.info(f"[Hash bucket index {hash_bucket_index}] Reading dedupe input for "
                f"{len(df_envelopes_list)} delta file envelope lists...")
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
            # TODO(raghumdani): store partition locator representing
            # primary key index instead of assuming it will always be of compacted delta. 
            prev_compacted_delta_stream_pos = round_completion_info\
                .compacted_delta_locator \
                .stream_position
            if prev_compacted_delta_stream_pos is None:
                raise ValueError(f"Unexpected Error: No previous compacted "
                                 f"delta stream position found in round "
                                 f"completion info: {round_completion_info}")
            prior_pk_index_table = pa.concat_tables(tables)
            prior_pk_index_table = sc.append_stream_position_column(
                prior_pk_index_table,
                repeat(
                    prev_compacted_delta_stream_pos,
                    len(prior_pk_index_table),
                ),
            )
            prior_pk_index_table = sc.append_delta_type_col(
                prior_pk_index_table,
                repeat(
                    sc.delta_type_to_field(DeltaType.UPSERT),
                    len(prior_pk_index_table),
                )
            )
            prior_pk_index_table = sc.append_is_source_col(
                prior_pk_index_table,
                repeat(
                    False,
                    len(prior_pk_index_table),
                )
            )
            logger.info(f"Number of records in prior primary index for hash bucket"
                        f" {hash_bucket_index}: {prior_pk_index_table.num_rows}")
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

    if hb_tables:
        hb_table = pa.concat_tables(hb_tables)
        logger.info(f"Total records in hash bucket {hash_bucket_index} is {hb_table.num_rows}")
        return hb_table
    else:
        return None


def _drop_duplicates_by_primary_key_hash(table: pa.Table) -> pa.Table:
    value_to_last_row_idx = {}

    pk_hash_np = sc.pk_hash_column_np(table)
    op_type_np = sc.delta_type_column_np(table)

    assert len(pk_hash_np) == len(op_type_np), "both pk hash and delta type should have same length"

    for row_idx in range(len(pk_hash_np)):
        pk_val = pk_hash_np[row_idx]
        op_val = op_type_np[row_idx].item()

        # operation type is True for `UPSERT` and False for `DELETE`
        if op_val:
            # UPSERT this row
            value_to_last_row_idx[pk_val] = row_idx
        else:
            logger.info(f"Found DELETE delta type {pk_val}...")
            # DELETE this row
            value_to_last_row_idx.pop(pk_val, None)

        row_idx += 1

    return table.take(list(value_to_last_row_idx.values()))

@ray.remote
def dedupe(
        compaction_artifact_s3_bucket: str,
        round_completion_info: Optional[RoundCompletionInfo],
        new_primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
        object_ids: List[Any],
        sort_keys: List[SortKey],
        max_records_per_index_file: int,
        num_materialize_buckets: int,
        dedupe_task_index: int,
        hash_bucket_count: int,
        num_hash_groups: int,
        delete_old_primary_key_index: bool) -> DedupeResult:

    logger.info(f"[Dedupe task {dedupe_task_index}] Starting dedupe task...")
    # TODO (pdames): mitigate risk of running out of memory here in cases of
    #  severe skew of primary key updates in deltas
    src_file_records_obj_refs = [
        cloudpickle.loads(obj_id_pkl) for obj_id_pkl in object_ids]
    logger.info(f"[Dedupe task {dedupe_task_index}] Getting delta file envelope "
                f"groups for {len(src_file_records_obj_refs)} object refs...")

    delta_file_envelope_groups_list = ray.get(src_file_records_obj_refs)
    hb_index_to_delta_file_envelopes_list = defaultdict(list)
    for delta_file_envelope_groups in delta_file_envelope_groups_list:
        for hb_idx, dfes in enumerate(delta_file_envelope_groups):
            if dfes is not None:
                hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)
    src_file_id_to_row_indices = defaultdict(list)

    list_size = len(hb_index_to_delta_file_envelopes_list)
    logger.info(f"[Dedupe task {dedupe_task_index}] Running {list_size} "
                f"dedupe rounds...")

    logger.info(f"[Dedupe task {dedupe_task_index}] hash_bucket_count={hash_bucket_count}"
                f" and dedupe_task_id={dedupe_task_index}")
    for hb_idx in range(hash_bucket_count):
        hash_group_idx = hb_idx % num_hash_groups

        dfe_list = hb_index_to_delta_file_envelopes_list[hb_idx]

        if (list_size == 0 and dedupe_task_index < hash_bucket_count):
            print(f"[Dedupe task {dedupe_task_index}] hb_group_idx={hash_group_idx} num_hash_groups={num_hash_groups}, HB_idx={hb_idx} result = {hash_group_idx != dedupe_task_index}")

        if hash_group_idx != dedupe_task_index:
            assert not dfe_list, f"{hb_idx} cannot be present in {dedupe_task_index}" 
            continue

        logger.info(f"[Dedupe task {dedupe_task_index}] union primary keys for hb_index: {hb_idx} with {len(dfe_list)} dfe size")

        table, union_time = timed_invocation(
            func=_union_primary_key_indices,
            s3_bucket=compaction_artifact_s3_bucket,
            round_completion_info=round_completion_info,
            hash_bucket_index=hb_idx,
            df_envelopes_list=dfe_list)
        logger.info(f"[Dedupe {dedupe_task_index}] Dedupe round input "
                    f"record count: {len(table)}, took {union_time}s")

        if table is None:
            continue

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
        if len(dfe_list):
            logger.info(f"[Dedupe task index {dedupe_task_index}] Dropping duplicates for {hb_idx}")
            #logger.info(f"[Dedupe task index {dedupe_task_index}] The non deduplicated table is {table}")

            table, drop_time = timed_invocation(func=_drop_duplicates_by_primary_key_hash, table=table)

            logger.info(f"[Dedupe task index {dedupe_task_index}] Dedupe round output "
                        f"record count: {len(table)}, took: {drop_time}s")
            #logger.info(f"[Dedupe task index {dedupe_task_index}] The deduplicated table is {table}")
        else:
            logger.info(f"[Dedupe task index {dedupe_task_index}] dfe is empty but take has {len(table)} records..")

        stream_position_col = sc.stream_position_column_np(table)
        file_idx_col = sc.file_index_column_np(table)
        row_idx_col = sc.record_index_column_np(table)
        is_source_col = sc.is_source_column_np(table)
        pk_hash_col = sc.pk_hash_column_np(table)

        for row_idx in range(len(table)):
            src_dfl = DeltaFileLocator.of(
                is_source_col[row_idx],
                stream_position_col[row_idx],
                file_idx_col[row_idx],
            )
            src_file_id_to_row_indices[src_dfl].append((row_idx_col[row_idx], pk_hash_col[row_idx], hb_idx))

    # free up object store memory to persist deduped tables
    free(src_file_records_obj_refs)
    del src_file_records_obj_refs

    logger.info(f"Finished all dedupe rounds...")
    mat_bucket_to_src_file_records: Dict[MaterializeBucketIndex, DeltaFileLocatorToRecords] = defaultdict(dict)
    for src_dfl, src_row_hash_table in src_file_id_to_row_indices.items():
        mat_bucket = delta_file_locator_to_mat_bucket_index(
            src_dfl,
            num_materialize_buckets,
        )

        row_idx_lists = [list(t) for t in zip(*src_row_hash_table)]
        row_idx_table = pa.table([pa.array(row_idx_lists[1], sc._PK_HASH_COLUMN_TYPE)], 
                                            names=[sc._PK_HASH_COLUMN_NAME])
        row_idx_table = sc.append_record_idx_col(row_idx_table, row_idx_lists[0])
        row_idx_table = sc.append_hash_bucket_idx_col(row_idx_table, row_idx_lists[2])
        
        mat_bucket_to_src_file_records[mat_bucket][src_dfl] = row_idx_table

    mat_bucket_to_row_idx_obj_id: Dict[MaterializeBucketIndex, DedupeTaskIndexWithObjectId] = {}
    for mat_bucket, src_file_records in mat_bucket_to_src_file_records.items():
        object_ref = ray.put(src_file_records)
        pickled_object_ref = cloudpickle.dumps(object_ref)
        mat_bucket_to_row_idx_obj_id[mat_bucket] = pickled_object_ref
        del object_ref
        del pickled_object_ref
    logger.info(f"Count of materialize buckets with object refs: "
                f"{len(mat_bucket_to_row_idx_obj_id)}")

    logger.info(f"[Dedupe task index {dedupe_task_index}] Finished dedupe task...")
    return mat_bucket_to_row_idx_obj_id
