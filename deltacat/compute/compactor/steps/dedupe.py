import importlib
import logging
from collections import defaultdict
from contextlib import nullcontext
from typing import Any, Dict, List, Tuple, Optional
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray
import time
from ray import cloudpickle
from deltacat.utils.pyarrow import ReadKwargsProviderPyArrowSchemaOverride
from deltacat.compute.compactor.utils.system_columns import get_minimal_hb_schema
import asyncio
import pickle
from itertools import repeat
from deltacat import logs
from deltacat.compute.compactor import (
    SortKey,
    SortOrder,
    DeltaFileEnvelope,
    DeltaFileLocator,
)
from deltacat.aws.clients import resource_cache
from deltacat.storage.model.types import DeltaType
from deltacat.compute.compactor import (
    RoundCompletionInfo,
    PrimaryKeyIndexVersionLocator,
    PyArrowWriteResult,
)
from deltacat.compute.compactor.model.dedupe_result import DedupeResult
from deltacat.compute.compactor.utils import (
    system_columns as sc,
    primary_key_index as pki,
)
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics, MetricsConfig

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

MaterializeBucketIndex = int
DeltaFileLocatorToRecords = Dict[DeltaFileLocator, np.ndarray]
DedupeTaskIndex, PickledObjectRef = int, str
DedupeTaskIndexWithObjectId = Tuple[DedupeTaskIndex, PickledObjectRef]


def _union_primary_key_indices(
    s3_bucket: str,
    round_completion_info: Optional[RoundCompletionInfo],
    hash_bucket_index: int,
    df_envelopes_list: List[List[DeltaFileEnvelope]],
) -> pa.Table:
    logger.info(
        f"[Hash bucket index {hash_bucket_index}] Reading dedupe input for "
        f"{len(df_envelopes_list)} delta file envelope lists..."
    )
    hb_tables = []
    if round_completion_info:
        pki_download_start_time = time.time()
        tables = pki.download_hash_bucket_entries(
            s3_bucket,
            hash_bucket_index,
            round_completion_info.primary_key_index_version_locator,
            # Enforce consistent column ordering by reading from a schema, to prevent schema mismatch errors
            file_reader_kwargs_provider=ReadKwargsProviderPyArrowSchemaOverride(
                schema=get_minimal_hb_schema()
            ),
        )
        if hash_bucket_index == 0:
            print(
                f"before concating and appending cols. len(tables): {len(tables)}, table schema: {tables[0].schema}"
            )
        pki_download_end_time = time.time()
        if tables:
            # prior_pk_index_table = pa.concat_tables(tables)
            prev_compacted_delta_stream_pos = (
                round_completion_info.compacted_delta_locator.stream_position
            )
            if prev_compacted_delta_stream_pos is None:
                raise ValueError(
                    f"Unexpected Error: No previous compacted "
                    f"delta stream position found in round "
                    f"completion info: {round_completion_info}"
                )
            prior_pk_index_table = pa.concat_tables(tables)
            logger.info(
                f"Number of records in prior primary index for hash bucket"
                f" {hash_bucket_index}: {prior_pk_index_table.num_rows}"
            )
            # print(f"Number of records in prior primary index for hash bucket"
            #          f" {hash_bucket_index}: {prior_pk_index_table.num_rows}")
            # print(f"before appending, prior pk index table schema\n{prior_pk_index_table.schema}")
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
                ),
            )
            prior_pk_index_table = sc.append_is_source_col(
                prior_pk_index_table,
                repeat(
                    False,
                    len(prior_pk_index_table),
                ),
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

    logger.info(
        f"Total records in hash bucket {hash_bucket_index} is {hb_table.num_rows}"
    )
    print(
        f"Total records in hash bucket {hash_bucket_index} is {hb_table.num_rows}, \
          time to download prior pk index: {pki_download_end_time - pki_download_start_time} seconds"
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


def write_new_primary_key_index(
    s3_bucket: str,
    new_primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
    max_rows_per_index_file: int,
    max_rows_per_mat_file: int,
    num_materialize_buckets: int,
    dedupe_task_index: int,
    deduped_tables: List[Tuple[int, pa.Table]],
    row_counts_ref: Any,
) -> PyArrowWriteResult:

    logger.info(
        f"Writing new deduped primary key index: "
        f"{new_primary_key_index_version_locator}"
    )
    # TODO (pdames): move to RecordCountsPendingMaterialize.finalize()?
    row_counts_get_start_time = time.time()
    row_counts: Dict[
        int, Dict[Tuple[np.bool_, np.int64, np.int32], Dict[int, int]]
    ] = ray.get(row_counts_ref)
    row_counts_get_end_time = time.time()
    # print(f"dedupe_task_index: {dedupe_task_index}, length of row_counts: {len(row_counts)}")
    file_idx = 0
    prev_file_idx = 0
    dest_file_indices = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    dest_file_row_indices = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    total_pki_write = 0
    for mat_bucket in sorted(row_counts.keys()):
        mat_bucket_row_idx = 0
        sorted_src_dfls = sorted(row_counts[mat_bucket].keys())
        for src_dfl in sorted_src_dfls:
            sorted_dd_tasks = sorted(row_counts[mat_bucket][src_dfl].keys())
            for dd_task_idx in sorted_dd_tasks:
                dest_file_row_indices[mat_bucket][src_dfl][dd_task_idx] = (
                    mat_bucket_row_idx % max_rows_per_mat_file
                )
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
            dest_file_start_idx = dest_file_indices[mat_bucket][src_dfl][
                dedupe_task_index
            ]
            dest_file_row_idx_offset = (
                src_dfl_row_counts[src_dfl]
                + dest_file_row_indices[mat_bucket][src_dfl][dedupe_task_index]
            )
            dest_file_idx_offset = int(dest_file_row_idx_offset / max_rows_per_mat_file)
            dest_file_idx = dest_file_start_idx + dest_file_idx_offset
            dest_file_idx_col.append(dest_file_idx)
            dest_file_row_idx = dest_file_row_idx_offset % max_rows_per_mat_file
            dest_file_row_idx_col.append(dest_file_row_idx)
            src_dfl_row_counts[src_dfl] += 1
        table = table.drop(
            [
                sc._IS_SOURCE_COLUMN_NAME,
                sc._PARTITION_STREAM_POSITION_COLUMN_NAME,
                sc._ORDERED_FILE_IDX_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ]
        )
        table = sc.append_file_idx_column(table, dest_file_idx_col)
        table = sc.append_record_idx_col(table, dest_file_row_idx_col)
        # print(f"during write, table schema is {table.schema}")
        before_write_pki = time.time()
        hb_pki_result = pki.write_primary_key_index_files(
            table,
            new_primary_key_index_version_locator,
            s3_bucket,
            hb_index,
            max_rows_per_index_file,
        )
        pki_results.append(hb_pki_result)
        after_write_pki = time.time()
        total_pki_write += after_write_pki - before_write_pki
    predict_end = time.time()
    result = PyArrowWriteResult.union(pki_results)
    logger.info(
        f"Wrote new deduped primary key index: "
        f"{new_primary_key_index_version_locator}. Result: {result}"
    )
    print(
        f"get row counts {row_counts_get_end_time-row_counts_get_start_time}, predicting pki {predict_end-row_counts_get_end_time-total_pki_write} total time spent writing pki: {total_pki_write}"
    )
    # print(f"Wrote new deduped primary key index: "
    #       f"{new_primary_key_index_version_locator}. Result: {result}")
    return result


def delta_file_locator_to_mat_bucket_index(
    df_locator: DeltaFileLocator, materialize_bucket_count: int
) -> int:
    digest = df_locator.digest()
    return int.from_bytes(digest, "big") % materialize_bucket_count


def upload_to_s3(bucket: str, key: str, record_counts: Any, region: str):
    # s3 = boto3.resource('s3', region_name=region)
    s3 = resource_cache("s3", None)
    data = pickle.dumps(record_counts)
    s3.Object(bucket, key).put(Body=data)


@ray.remote(num_cpus=0)
def download_from_s3(bucket: str, key: str, region: str):
    # s3 = boto3.resource('s3', region_name=region)
    s3 = resource_cache("s3", None)
    try:
        serialized_data = s3.Object(bucket, key).get()["Body"].read()
    except Exception as e:
        logger.error(f"Failed to download {bucket}/{key} with error: {e}")
        print(f"Failed to download {bucket}/{key}")
        raise
    return pickle.loads(serialized_data)


@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.ready_event = asyncio.Event()

    def send(self, clear=False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait=True):
        if should_wait:
            await self.ready_event.wait()


@ray.remote(num_cpus=0.1)
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
        self.record_counts_ref = None
        self.expected_result_count = expected_result_count
        self.actual_result_count = 0

    def to_dict(self):
        record_counts_dict = {
            key: {subkey: dict(subvalue) for subkey, subvalue in value.items()}
            for key, value in self.record_counts.items()
        }
        return record_counts_dict

    def add_record_counts(
        self,
        result_idx: int,
        record_counts: Dict[int, Dict[Tuple[np.bool_, np.int64, np.int32], int]],
    ) -> None:
        # start = time.time()
        # TODO: use df directly
        for mat_bucket, df_locator_rows in record_counts.items():
            for df_locator, rows in df_locator_rows.items():
                self.record_counts[mat_bucket][df_locator][result_idx] += rows
        self.actual_result_count += 1
        # end = time.time()
        # print(f"received from task {result_idx}, time taken {(end-start):.2f}")

    def get_record_counts(self):
        return self.record_counts_ref

    def get_expected_result_count(self) -> int:
        return self.expected_result_count

    def get_actual_result_count(self) -> int:
        return self.actual_result_count

    def is_finalized(self) -> bool:
        return self.actual_result_count == self.expected_result_count

    def to_s3(self, bucket, key, region):
        serializable_record_counts = self.to_dict()
        upload_to_s3(bucket, key, serializable_record_counts, region)
        return len(self.record_counts)

    def from_s3(self, bucket, keys, region, num_chunk=2):
        # reduce the concurrency by splitting the keys into num_chunk chunks
        keys_chunks = [keys[i::num_chunk] for i in range(num_chunk)]
        record_counts = []
        for keys_chunk in keys_chunks:
            record_counts_chunk = ray.get(
                [download_from_s3.remote(bucket, key, region) for key in keys_chunk]
            )
            record_counts.extend(record_counts_chunk)
        final_record = {}
        for rc in record_counts:
            final_record.update(rc)
        self.record_counts_ref = ray.put(final_record)
        record_counts = None
        self.record_counts = None
        del record_counts
        del self.record_counts
        return len(final_record)


def _timed_dedupe(
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
    record_counts_pending_materialize: List[RecordCountsPendingMaterialize],
    signalactor: SignalActor,
    enable_profiler: bool,
):
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"dedupe_{worker_id}_{task_id}.bin"
    ) if enable_profiler else nullcontext():
        # TODO (pdames): mitigate risk of running out of memory here in cases of
        #  severe skew of primary key updates in deltas
        src_file_records_obj_refs = [
            cloudpickle.loads(obj_id_pkl) for obj_id_pkl in object_ids
        ]
        logger.info(
            f"[Dedupe task {dedupe_task_index}] Getting delta file envelope "
            f"groups for {len(src_file_records_obj_refs)} object refs..."
        )
        get_pki_time_start = time.time()
        delta_file_envelope_groups_list = ray.get(src_file_records_obj_refs)
        get_pki_time_end = time.time()
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
                s3_bucket=compaction_artifact_s3_bucket,
                round_completion_info=round_completion_info,
                hash_bucket_index=hb_idx,
                df_envelopes_list=dfe_list,
            )
            logger.info(
                f"[Dedupe {dedupe_task_index}] Dedupe round input "
                f"record count: {len(table)}, took {union_time}s"
            )
            # print(
            #     f"[Dedupe {dedupe_task_index}] Dedupe round input "
            #     f"record count: {len(table)}, took {union_time}s"
            # )

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
            # print(
            #     f"[Dedupe task index {dedupe_task_index}] Dropping duplicates for {hb_idx}"
            # )
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
            # print(
            #     f"[Dedupe task index {dedupe_task_index}] Dedupe round output "
            #     f"record count: {len(table)}, took: {drop_time}s"
            # )

            deduped_tables.append((hb_idx, table))
            # append_time = time.time()
            stream_position_col = sc.stream_position_column_np(table)
            file_idx_col = sc.file_index_column_np(table)
            row_idx_col = sc.record_index_column_np(table)
            is_source_col = sc.is_source_column_np(table)

            for row_idx in range(len(table)):
                # construct_start = time.time()
                src_dfl = DeltaFileLocator.of(
                    is_source_col[row_idx],
                    stream_position_col[row_idx],
                    file_idx_col[row_idx],
                )
                # construct_end = time.time()
                # construct += construct_end - construct_start
                # TODO(pdames): merge contiguous record number ranges
                src_file_id_to_row_indices[src_dfl].append(row_idx_col[row_idx])
                # append_row_end = time.time()
                # max_loop = max(max_loop, append_row_end - construct_start)
                # min_loop = min(min_loop, append_row_end - construct_start)

            # append_time_end = time.time()
            # print(f"[Dedupe task index {dedupe_task_index}] construct time: {construct}, min loop: {min_loop}, max loop: {max_loop}")
            # print(
            #     f"[Dedupe task index {dedupe_task_index}] total append time: {append_time_end - append_time}, dflocator total construct time: {construct}, min loop: {min_loop}, max loop: {max_loop}"
            # )

        logger.info(f"Finished all dedupe rounds...")
        # print(f"finished all dedupe rounds...")
        mat_bucket_to_src_file_record_count = defaultdict(dict)
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
            mat_bucket_to_src_file_record_count[mat_bucket][src_dfl] = len(
                src_row_indices
            )

        mat_bucket_to_dd_idx_obj_id: Dict[
            MaterializeBucketIndex, DedupeTaskIndexWithObjectId
        ] = {}
        for mat_bucket, src_file_records in mat_bucket_to_src_file_records.items():
            object_ref = ray.put(src_file_records)
            pickled_object_ref = cloudpickle.dumps(object_ref)
            mat_bucket_to_dd_idx_obj_id[mat_bucket] = (
                dedupe_task_index,
                pickled_object_ref,
            )
            del object_ref
            del pickled_object_ref
        logger.info(
            f"Count of materialize buckets with object refs: "
            f"{len(mat_bucket_to_dd_idx_obj_id)}"
        )
        # Collective Reduce Operation
        # print(f"Collective Reduce Operation Started on {dedupe_task_index}")
        collective_op_start_time = time.time()
        # send partial record counts on this dedupe task to global actor
        # get a subset of the dictionary
        number_of_nodes = len(record_counts_pending_materialize)
        node_data = [{} for _ in range(number_of_nodes)]
        for mat_bucket, mat_record_count in mat_bucket_to_src_file_record_count.items():
            node_id = mat_bucket % number_of_nodes
            node_data[node_id][mat_bucket] = mat_record_count
        # Compare to single actor mode. This version is using number_nodes actors to merge the record counts
        # Each actor is handle 1/number_nodes of total data from all tasks.
        # Assuming total data is 40 GB, now the actor only needs to handle 40/128=300 MB of data assuming number_nodes = 128
        # But the penalty is instead of launching a broadcast once, we need to conduct number_nodes broadcasts
        # The merging is also faster now, as we essentially removed the outer loop, i.e., 'mat_bucket'
        [
            record_counts_pending_materialize[i].add_record_counts.remote(
                dedupe_task_index,
                node_data[i],
            )
            for i in range(number_of_nodes)
        ]

        # reduce all record counts and pull the final record_counts
        record_start = time.time()
        finalized = False
        while not finalized:
            finalized = all(
                ray.get(
                    [
                        record_counts_pending_materialize[i].is_finalized.remote()
                        for i in range(number_of_nodes)
                    ]
                )
            )
            time.sleep(5)
        # Now each node has a complete version of the record_counts, that belongs to a specific mat bucket or buckets
        # e.g., node 0 has all the record counts for mat buckets 0, 4, 8, 12, etc.
        # node 1 has all the record counts for mat buckets 1, 5, 9, 13, etc.
        record_end = time.time()
        # delegate one task to control actor group activies: upload to s3 and download from s3 (or all to all via network)
        # all other tasks should wait for the final record_counts to be local on each node
        if dedupe_task_index == 0:
            print(f"add_record_counts: {(record_end-record_start):.2f} seconds")
            # ask one task to trigger the parallel upload
            upload_start = time.time()
            part_record_length_on_actor = ray.get(
                [
                    record_counts_pending_materialize[i].to_s3.remote(
                        "benchmark-recordcounts", f"record_counts_{i}", "us-east-1"
                    )
                    for i in range(number_of_nodes)
                ]
            )
            upload_end = time.time()
            print(f"upload to s3: {(upload_end-upload_start):.2f} seconds")
            # print(f"part record length on each actor {part_record_length_on_actor}")
            rc_keys = ["record_counts_" + str(i) for i in range(number_of_nodes)]
            record_length_on_actor = ray.get(
                [
                    record_counts_pending_materialize[i].from_s3.remote(
                        "benchmark-recordcounts", rc_keys, "us-east-1"
                    )
                    for i in range(number_of_nodes)
                ]
            )
            download_end = time.time()
            print(f"download from s3: {(download_end-upload_end):.2f} seconds")
            # print(f"final record length on each actor should be same {record_length_on_actor}")
            assert all(
                element == record_length_on_actor[0]
                for element in record_length_on_actor
            ), "Not all elements are the same."
            assert (
                sum(part_record_length_on_actor) == record_length_on_actor[0]
            ), "sum of record counts on all actors not equal to final merged record counts"
            # broadcast the complete signal to all tasks
            print("broadcast complete signal to all tasks")
            ray.get(signalactor.send.remote())

        # sync all tasks by waiting for delegated task completion signal
        ray.get(signalactor.wait.remote())
        # get record_counts ref in node-local actor
        record_count_ref = ray.get(
            record_counts_pending_materialize[
                dedupe_task_index % number_of_nodes
            ].get_record_counts.remote()
        )
        # get record_counts from node-local object_store
        # record_counts = ray.get(record_count_ref)
        collective_op_end_time = time.time()

        write_pki_result: PyArrowWriteResult = write_new_primary_key_index(
            compaction_artifact_s3_bucket,
            new_primary_key_index_version_locator,
            max_records_per_index_file,
            max_records_per_materialized_file,
            num_materialize_buckets,
            dedupe_task_index,
            deduped_tables,
            record_count_ref,
        )
        write_pki_result_end_time = time.time()

        if delete_old_primary_key_index:
            pki.delete_primary_key_index_version(
                compaction_artifact_s3_bucket,
                round_completion_info.primary_key_index_version_locator,
            )
        return DedupeResult(
            mat_bucket_to_dd_idx_obj_id,
            write_pki_result,
            np.int64(total_deduped_records),
            get_pki_time_end - get_pki_time_start,
            collective_op_end_time - collective_op_start_time,
            write_pki_result_end_time - collective_op_end_time,
        )


@ray.remote(num_cpus=1)
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
    record_counts_pending_materialize: List[RecordCountsPendingMaterialize],
    signalactor: SignalActor,
    enable_profiler: bool,
    metrics_config: MetricsConfig,
) -> DedupeResult:
    logger.info(f"[Dedupe task {dedupe_task_index}] Starting dedupe task...")
    dedupe_result, duration = timed_invocation(
        func=_timed_dedupe,
        compaction_artifact_s3_bucket=compaction_artifact_s3_bucket,
        round_completion_info=round_completion_info,
        new_primary_key_index_version_locator=new_primary_key_index_version_locator,
        object_ids=object_ids,
        sort_keys=sort_keys,
        max_records_per_index_file=max_records_per_index_file,
        max_records_per_materialized_file=max_records_per_materialized_file,
        num_materialize_buckets=num_materialize_buckets,
        dedupe_task_index=dedupe_task_index,
        delete_old_primary_key_index=delete_old_primary_key_index,
        record_counts_pending_materialize=record_counts_pending_materialize,
        signalactor=signalactor,
        enable_profiler=enable_profiler,
    )
    if metrics_config:
        emit_timer_metrics(
            metrics_name="dedupe", value=duration, metrics_config=metrics_config
        )

    logger.info(f"[Dedupe task index {dedupe_task_index}] Finished dedupe task...")
    return dedupe_result, duration
