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
from itertools import repeat

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
    RoundCompletionInfo,
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
from deltacat.compute.compactor.utils.primary_key_index import generate_pk_hash_column

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

MaterializeBucketIndex = int
DeltaFileLocatorToRecords = Dict[DeltaFileLocator, np.ndarray]
DedupeTaskIndex, PickledObjectRef = int, str
DedupeTaskIndexWithObjectId = Tuple[DedupeTaskIndex, PickledObjectRef]


def combine_column(table, name):
    return table.column(name).combine_chunks()


def groupify_array(arr):
    # Input: Pyarrow/Numpy array
    # Output:
    #   - 1. Unique values
    #   - 2. Count per unique
    #   - 3. Sort index
    #   - 4. Begin index per unique
    dic, counts = np.unique(arr, return_counts=True)
    sort_idx = np.argsort(arr)
    return dic, counts, sort_idx, [0] + np.cumsum(counts)[:-1].tolist()


def columns_to_array(table, columns):
    columns = [columns] if isinstance(columns, str) else list(set(columns))
    values = [c.to_numpy() for c in table.select(columns).itercolumns()]
    return np.array(list(map(hash, zip(*values))))


def drop_duplicates(table, on=[], keep="first"):
    # Gather columns to arr
    arr = columns_to_array(table, (on if on else table.column_names))

    # Groupify
    dic, counts, sort_idxs, bgn_idxs = groupify_array(arr)

    # Gather idxs
    if keep == "last":
        idxs = (np.array(bgn_idxs) - 1)[1:].tolist() + [len(sort_idxs) - 1]
    elif keep == "first":
        idxs = bgn_idxs
    elif keep == "drop":
        idxs = [i for i, c in zip(bgn_idxs, counts) if c == 1]

    mask = list(repeat(False, len(table)))

    for id in sort_idxs[idxs]:
        mask[id] = True

    return table.filter(pa.array(mask))


def _dedupe_incremental(
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
        hb_tables.append(df_envelope.table)

    hb_table = pa.concat_tables(hb_tables)

    start = time.monotonic()
    # TODO: We do not need to run this when rebasing.
    hb_table = drop_duplicates(hb_table, on=[sc._PK_HASH_COLUMN_NAME], keep="last")
    end = time.monotonic()
    # Rebase:  Dropping duplicates for incremental in 31 took: 88.78605026099999
    #
    logger.info(
        f"Dropping duplicates for incremental in {hash_bucket_index} took: {end - start}"
    )

    logger.info(
        f"Total records in hash bucket {hash_bucket_index} is {hb_table.num_rows}"
    )
    return hb_table


def merge_tables(table, old_table):
    start = time.monotonic()
    mask = pc.invert(
        pc.is_in(old_table[sc._PK_HASH_COLUMN_NAME], table[sc._PK_HASH_COLUMN_NAME])
    )

    result = old_table.filter(mask)
    end = time.monotonic()

    # Merging with old table took: 4.996597067999971. Total records: 12223266 and 12232035 and 11789
    logger.info(
        f"Merging with old table took: {end - start}. Total records: {len(result)} and {len(old_table)} and {len(table)}"
    )

    return pa.concat_tables([result, table])


def download_old_table_and_hash(
    hb_index: int,
    rcf: RoundCompletionInfo,
    read_kwargs_provider,
    primary_keys,
    deltacat_storage: unimplemented_deltacat_storage,
):
    tables = []
    hb_index_to_indices = rcf.hb_id_to_indices
    indices = hb_index_to_indices[str(hb_index)]

    assert (
        indices is not None and len(indices) == 2
    ), "indices should not be none and contains exactly two elements"

    start = time.monotonic()

    # pool = multiprocessing.Pool(indices[1] - indices[0])

    # partial_downloader = partial(
    #     downloader, compacted_delta=rcf.compacted_delta_locator, read_kwargs_provider=read_kwargs_provider)

    # for table in pool.map(partial_downloader, [indices[0] + offset for offset in range(indices[1] - indices[0])]):
    #     tables.append(table)

    for offset in range(indices[1] - indices[0]):
        table = deltacat_storage.download_delta_manifest_entry(
            rcf.compacted_delta_locator,
            entry_index=(indices[0] + offset),
            file_reader_kwargs_provider=read_kwargs_provider,
        )

        tables.append(table)

    end = time.monotonic()

    # Downloaded 4 files for hash bucket: 850 in 64.093580946s
    # There is an opportunity to parallelize this but this is also current behavior.
    logger.info(
        f"Downloaded {indices[1] - indices[0]} files for hash bucket: {hb_index} in {end - start}s"
    )

    result = pa.concat_tables(tables)
    return generate_pk_hash_column(result, primary_keys=primary_keys)


def _timed_dedupe(
    object_ids: List[Any],
    sort_keys: List[SortKey],
    num_materialize_buckets: int,
    read_kwargs_provider: Any,
    dedupe_task_index: int,
    partition: Partition,
    enable_profiler: bool,
    max_records_per_output_file: int,
    compacted_file_content_type: ContentType,
    object_store: Optional[IObjectStore],
    primary_keys: Any,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    round_completion_info: Optional[RoundCompletionInfo] = None,
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

        # TODO, if hash buckets are not touched, reference their files as is.
        for hb_idx, dfe_list in hb_index_to_delta_file_envelopes_list.items():
            logger.info(
                f"{dedupe_task_index}: union primary keys for hb_index: {hb_idx}"
            )
            table, union_time = timed_invocation(
                func=_dedupe_incremental,
                hash_bucket_index=hb_idx,
                df_envelopes_list=dfe_list,
            )
            # # Dedupe round input record count: 8859597, took 92.469715982s
            logger.info(
                f"[Dedupe {dedupe_task_index}] Dedupe round input "
                f"record count: {len(table)}, took {union_time}s"
            )

            # sort by sort keys
            if len(sort_keys):
                # TODO (pdames): convert to O(N) dedupe w/ sort keys
                table = table.take(pc.sort_indices(table, sort_keys=sort_keys))

            # drop duplicates by primary key hash column
            logger.info(
                f"[Dedupe task index {dedupe_task_index}] Dropping duplicates for {hb_idx}"
            )

            if round_completion_info is not None:
                compacted_table = download_old_table_and_hash(
                    hb_idx,
                    round_completion_info,
                    read_kwargs_provider,
                    primary_keys,
                    deltacat_storage,
                )

                hb_table_record_count = len(table) + len(compacted_table)
                table, drop_time = timed_invocation(
                    func=merge_tables, old_table=compacted_table, table=table
                )
                deduped_record_count = hb_table_record_count - len(table)
                total_deduped_records += deduped_record_count

                logger.info(
                    f"[Dedupe task index {dedupe_task_index}] Dedupe round output "
                    f"record count: {len(table)}, took: {drop_time}s"
                )

            table = table.drop(
                [
                    sc._PK_HASH_COLUMN_NAME,
                ]
            )

            materialized_results.append(_materialize(hb_idx, [table]))

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
    primary_keys: Any,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    read_kwargs_provider=None,
    round_completion_info: Optional[RoundCompletionInfo] = None,
    deltacat_storage=unimplemented_deltacat_storage,
) -> DedupeResult:
    logger.info(f"[Dedupe task {dedupe_task_index}] Starting dedupe task...")
    dedupe_result, duration = timed_invocation(
        func=_timed_dedupe,
        object_ids=object_ids,
        sort_keys=sort_keys,
        read_kwargs_provider=read_kwargs_provider,
        num_materialize_buckets=num_materialize_buckets,
        dedupe_task_index=dedupe_task_index,
        enable_profiler=enable_profiler,
        object_store=object_store,
        partition=partition,
        primary_keys=primary_keys,
        max_records_per_output_file=max_records_per_output_file,
        compacted_file_content_type=compacted_file_content_type,
        s3_table_writer_kwargs=s3_table_writer_kwargs,
        round_completion_info=round_completion_info,
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
