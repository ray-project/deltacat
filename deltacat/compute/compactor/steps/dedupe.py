import logging
import pyarrow as pa
import ray
import time
import pyarrow.compute as pc
import numpy as np
from ray import cloudpickle

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

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


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
        )
        if tables:
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
    # TODO (pdames): drop all primary key occurrences for DELETE delta types
    value_to_last_row_idx = {}
    row_idx = 0
    for chunk in sc.pk_hash_column(table).iterchunks():
        for val in chunk.to_numpy(zero_copy_only=False):
            value_to_last_row_idx[val] = row_idx
            row_idx += 1
    return table.take(list(value_to_last_row_idx.values()))


def write_new_primary_key_index(
        s3_bucket: str,
        new_primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
        max_rows_per_index_file: int,
        max_rows_per_mat_file: int,
        num_materialize_buckets: int,
        dedupe_task_index: int,
        deduped_tables: List[Tuple[int, pa.Table]],
        row_counts: Dict[int, Dict[Tuple[np.bool_, np.int64, np.int32],
                                   Dict[int, int]]]) -> Dict[str, int]:

    logger.info(f"Writing new deduped primary key index: "
                f"{new_primary_key_index_version_locator}")
    # TODO (pdames): move to RecordCountsPendingMaterialize.finalize()?
    file_idx = 0
    prev_file_idx = 0
    dest_file_indices = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(int)
        )
    )
    dest_file_row_indices = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(int)
        )
    )
    for mat_bucket in sorted(row_counts.keys()):
        mat_bucket_row_idx = 0
        sorted_src_dfls = sorted(row_counts[mat_bucket].keys())
        for src_dfl in sorted_src_dfls:
            sorted_dd_tasks = sorted(row_counts[mat_bucket][src_dfl].keys())
            for dd_task_idx in sorted_dd_tasks:
                dest_file_row_indices[mat_bucket][src_dfl][dd_task_idx] = \
                    mat_bucket_row_idx % max_rows_per_mat_file
                file_idx = prev_file_idx + int(
                    mat_bucket_row_idx / max_rows_per_mat_file
                )
                dest_file_indices[mat_bucket][src_dfl][dd_task_idx] = file_idx
                row_count = row_counts[mat_bucket][src_dfl][dd_task_idx]
                mat_bucket_row_idx += row_count
        prev_file_idx = file_idx + 1

    pki_results = []
    src_dfl_row_counts = defaultdict(int)
    for hb_index, table in deduped_tables:
        is_source_col = sc.is_source_column_np(table)
        stream_pos_col = sc.stream_position_column_np(table)
        file_idx_col = sc.file_index_column_np(table)
        dest_file_idx_col = []
        dest_file_row_idx_col = []
        for row_idx in range(len(table)):
            src_dfl = DeltaFileLocator.of(
                is_source_col[row_idx],
                stream_pos_col[row_idx],
                file_idx_col[row_idx],
            )
            mat_bucket = delta_file_locator_to_mat_bucket_index(
                src_dfl,
                num_materialize_buckets,
            )
            dest_file_start_idx = \
                dest_file_indices[mat_bucket][src_dfl][dedupe_task_index]
            dest_file_row_idx_offset = src_dfl_row_counts[src_dfl] + \
                dest_file_row_indices[mat_bucket][src_dfl][dedupe_task_index]
            dest_file_idx_offset = int(
                dest_file_row_idx_offset / max_rows_per_mat_file
            )
            dest_file_idx = dest_file_start_idx + dest_file_idx_offset
            dest_file_idx_col.append(dest_file_idx)
            dest_file_row_idx = dest_file_row_idx_offset % max_rows_per_mat_file
            dest_file_row_idx_col.append(dest_file_row_idx)
            src_dfl_row_counts[src_dfl] += 1
        table = table.drop([
            sc._IS_SOURCE_COLUMN_NAME,
            sc._PARTITION_STREAM_POSITION_COLUMN_NAME,
            sc._ORDERED_FILE_IDX_COLUMN_NAME,
            sc._ORDERED_RECORD_IDX_COLUMN_NAME,
        ])
        table = sc.append_file_idx_column(table, dest_file_idx_col)
        table = sc.append_record_idx_col(table, dest_file_row_idx_col)

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


@ray.remote
class RecordCountsPendingMaterialize:
    def __init__(self, expected_result_count: int):
        # materialize_bucket -> src_file_id
        self.record_counts = defaultdict(
            # delta_file_locator -> dedupe task index
            lambda: defaultdict(
                # dedupe task index -> row count
                lambda: defaultdict(int)
            )
        )
        self.expected_result_count = expected_result_count
        self.actual_result_count = 0

    def add_record_counts(
            self,
            result_idx: int,
            record_counts:
            Dict[int, Dict[Tuple[np.bool_, np.int64, np.int32], int]]) -> None:
        for mat_bucket, df_locator_rows in record_counts.items():
            for df_locator, rows in df_locator_rows.items():
                self.record_counts[mat_bucket][df_locator][result_idx] += rows
        self.actual_result_count += 1

    def get_record_counts(self) -> \
            Dict[int, Dict[Tuple[np.bool_, np.int64, np.int32],
                           Dict[int, int]]]:
        return self.record_counts

    def get_expected_result_count(self) -> int:
        return self.expected_result_count

    def get_actual_result_count(self) -> int:
        return self.actual_result_count

    def is_finalized(self) -> bool:
        return self.actual_result_count == self.expected_result_count


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
        delete_old_primary_key_index: bool,
        record_counts_pending_materialize: RecordCountsPendingMaterialize):

    logger.info(f"Starting dedupe task...")
    # TODO (pdames): mitigate risk of running out of memory here in cases of
    #  severe skew of primary key updates in deltas
    src_file_records_obj_refs = [
        cloudpickle.loads(obj_id_pkl) for obj_id_pkl in object_ids]
    logger.info(f"Getting delta file envelope groups object refs...")
    delta_file_envelope_groups_list = ray.get(src_file_records_obj_refs)
    hb_index_to_delta_file_envelopes_list = defaultdict(list)
    for delta_file_envelope_groups in delta_file_envelope_groups_list:
        for hb_idx, dfes in enumerate(delta_file_envelope_groups):
            if dfes is not None:
                hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)
    src_file_id_to_row_indices = defaultdict(list)
    deduped_tables = []
    logger.info(f"Running {len(hb_index_to_delta_file_envelopes_list)} "
                f"dedupe rounds...")
    for hb_idx, dfe_list in hb_index_to_delta_file_envelopes_list.items():
        table = union_primary_key_indices(
            compaction_artifact_s3_bucket,
            round_completion_info,
            hb_idx,
            dfe_list,
        )
        logger.info(f"Dedupe round input record count: {len(table)}")

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
        table = drop_duplicates_by_primary_key_hash(table)
        table = table.drop([sc._DELTA_TYPE_COLUMN_NAME])
        logger.info(f"Dedupe round output record count: {len(table)}")

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

    logger.info(f"Finished all dedupe rounds...")
    mat_bucket_to_src_file_record_count = defaultdict(dict)
    mat_bucket_to_src_file_records = defaultdict(dict)
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

    mat_bucket_to_dd_idx_obj_id = {}
    src_file_records_obj_refs = []
    for mat_bucket, src_file_records in mat_bucket_to_src_file_records.items():
        object_ref = ray.put(src_file_records)
        src_file_records_obj_refs.append(object_ref)
        mat_bucket_to_dd_idx_obj_id[mat_bucket] = (
            dedupe_task_index,
            cloudpickle.dumps(object_ref),
        )
    logger.info(f"Count of materialize buckets with object refs: "
                f"{len(mat_bucket_to_dd_idx_obj_id)}")

    record_counts_pending_materialize.add_record_counts.remote(
        dedupe_task_index,
        mat_bucket_to_src_file_record_count,
    )

    # wait for all dedupe tasks to reach this point before continuing
    logger.info(
        f"Waiting for all dedupe tasks to finish writing record counts...")
    finalized = False
    while not finalized:
        finalized = ray.get(
            record_counts_pending_materialize.is_finalized.remote()
        )
        time.sleep(0.25)
    record_counts = ray.get(
        record_counts_pending_materialize.get_record_counts.remote()
    )
    write_pki_result = write_new_primary_key_index(
        compaction_artifact_s3_bucket,
        new_primary_key_index_version_locator,
        max_records_per_index_file,
        max_records_per_materialized_file,
        num_materialize_buckets,
        dedupe_task_index,
        deduped_tables,
        record_counts,
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
