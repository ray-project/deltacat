import logging
import importlib
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
import numpy as np
import pyarrow as pa
import ray
import itertools
import time
import pyarrow.compute as pc
from deltacat.utils.pyarrow import MAX_INT_BYTES
import deltacat.compute.compactor_v2.utils.merge as merge_utils
from uuid import uuid4
from deltacat import logs
from typing import Callable, Iterator, List, Optional, Tuple
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.compute.compactor_v2.model.merge_file_group import MergeFileGroup
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.compute.compactor import RoundCompletionInfo, DeltaFileEnvelope
from deltacat.utils.common import ReadKwargsProvider
from contextlib import nullcontext
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics, failure_metric, success_metric
from deltacat.utils.resources import (
    get_current_process_peak_memory_usage_in_bytes,
    ProcessUtilizationOverTimeRange,
)
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    generate_pk_hash_column,
)
from deltacat.storage import (
    Delta,
    DeltaLocator,
    DeltaType,
    Manifest,
    Partition,
    interface as unimplemented_deltacat_storage,
)
from deltacat.compute.compactor_v2.utils.dedupe import drop_duplicates
from deltacat.constants import BYTES_PER_GIBIBYTE
from deltacat.compute.compactor_v2.constants import (
    MERGE_TIME_IN_SECONDS,
    MERGE_SUCCESS_COUNT,
    MERGE_FAILURE_COUNT,
)
from deltacat.exceptions import (
    categorize_errors,
)

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _append_delta_type_column(table: pa.Table, value: np.bool_):
    return table.append_column(
        sc._DELTA_TYPE_COLUMN_FIELD,
        pa.array(np.repeat(value, len(table)), sc._DELTA_TYPE_COLUMN_TYPE),
    )


def _drop_delta_type_rows(table: pa.Table, delta_type: DeltaType) -> pa.Table:
    if sc._DELTA_TYPE_COLUMN_NAME not in table.column_names:
        return table

    delta_type_value = sc.delta_type_to_field(delta_type)

    result = table.filter(
        pc.not_equal(table[sc._DELTA_TYPE_COLUMN_NAME], delta_type_value)
    )

    return result.drop([sc._DELTA_TYPE_COLUMN_NAME])


def _build_incremental_table(
    df_envelopes: List[DeltaFileEnvelope],
) -> pa.Table:

    hb_tables = []
    # sort by delta file stream position now instead of sorting every row later
    is_delete = False
    for df_envelope in df_envelopes:
        assert (
            df_envelope.delta_type != DeltaType.APPEND
        ), "APPEND type deltas are not supported. Kindly use UPSERT or DELETE"
        if df_envelope.delta_type == DeltaType.DELETE:
            is_delete = True

    for df_envelope in df_envelopes:
        table = df_envelope.table
        if is_delete:
            table = _append_delta_type_column(
                table, np.bool_(sc.delta_type_to_field(df_envelope.delta_type))
            )

        hb_tables.append(table)
    result = pa.concat_tables(hb_tables)
    return result


def _merge_tables(
    table: pa.Table,
    primary_keys: List[str],
    can_drop_duplicates: bool,
    compacted_table: Optional[pa.Table] = None,
) -> pa.Table:
    """
    Merges the table with compacted table dropping duplicates where necessary.

    This method ensures the appropriate deltas of types [UPSERT] are correctly
    appended to the table.
    """

    all_tables = []
    incremental_idx = 0

    if compacted_table:
        incremental_idx = 1
        all_tables.append(compacted_table)

    all_tables.append(table)

    if not primary_keys or not can_drop_duplicates:
        logger.info(
            f"Not dropping duplicates for primary keys={primary_keys} "
            f"and can_drop_duplicates={can_drop_duplicates}"
        )
        all_tables[incremental_idx] = _drop_delta_type_rows(
            all_tables[incremental_idx], DeltaType.DELETE
        )
        # we need not drop duplicates
        return pa.concat_tables(all_tables)

    all_tables = generate_pk_hash_column(all_tables, primary_keys=primary_keys)

    result_table_list = []

    incremental_table = drop_duplicates(
        all_tables[incremental_idx], on=sc._PK_HASH_STRING_COLUMN_NAME
    )

    if compacted_table:
        compacted_table = all_tables[0]

        compacted_pk_hash_str = compacted_table[sc._PK_HASH_STRING_COLUMN_NAME]
        incremental_pk_hash_str = incremental_table[sc._PK_HASH_STRING_COLUMN_NAME]

        logger.info(
            f"Size of compacted pk hash={compacted_pk_hash_str.nbytes} "
            f"and incremental pk hash={incremental_pk_hash_str.nbytes}."
        )

        if (
            compacted_table[sc._PK_HASH_STRING_COLUMN_NAME].nbytes >= MAX_INT_BYTES
            or incremental_table[sc._PK_HASH_STRING_COLUMN_NAME].nbytes >= MAX_INT_BYTES
        ):
            logger.info("Casting compacted and incremental pk hash to large_string...")
            # is_in combines the chunks of the chunked array passed which can cause
            # ArrowCapacityError if the total size of string array is over 2GB.
            # Using a large_string would resolve this issue.
            # The cast here should be zero-copy in most cases.
            compacted_pk_hash_str = pc.cast(compacted_pk_hash_str, pa.large_string())
            incremental_pk_hash_str = pc.cast(
                incremental_pk_hash_str, pa.large_string()
            )

        records_to_keep = pc.invert(
            pc.is_in(
                compacted_pk_hash_str,
                incremental_pk_hash_str,
            )
        )

        result_table_list.append(compacted_table.filter(records_to_keep))

    incremental_table = _drop_delta_type_rows(incremental_table, DeltaType.DELETE)
    result_table_list.append(incremental_table)

    final_table = pa.concat_tables(result_table_list)
    final_table = final_table.drop([sc._PK_HASH_STRING_COLUMN_NAME])

    return final_table


def _download_compacted_table(
    hb_index: int,
    rcf: RoundCompletionInfo,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
) -> pa.Table:
    tables = []
    hb_index_to_indices = rcf.hb_index_to_entry_range

    if str(hb_index) not in hb_index_to_indices:
        return None
    indices = hb_index_to_indices[str(hb_index)]
    assert (
        indices is not None and len(indices) == 2
    ), "indices should not be none and contains exactly two elements"
    for offset in range(indices[1] - indices[0]):
        table = deltacat_storage.download_delta_manifest_entry(
            rcf.compacted_delta_locator,
            entry_index=(indices[0] + offset),
            file_reader_kwargs_provider=read_kwargs_provider,
            **deltacat_storage_kwargs,
        )

        tables.append(table)

    return pa.concat_tables(tables)


def _copy_all_manifest_files_from_old_hash_buckets(
    hb_index_copy_by_reference: List[int],
    round_completion_info: RoundCompletionInfo,
    write_to_partition: Partition,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
) -> List[MaterializeResult]:

    compacted_delta_locator = round_completion_info.compacted_delta_locator
    manifest = deltacat_storage.get_delta_manifest(
        compacted_delta_locator, **deltacat_storage_kwargs
    )

    manifest_entry_referenced_list = []
    materialize_result_list = []
    hb_index_to_indices = round_completion_info.hb_index_to_entry_range

    if hb_index_to_indices is None:
        logger.info("Nothing to copy by reference. Skipping...")
        return []

    for hb_index in hb_index_copy_by_reference:
        if str(hb_index) not in hb_index_to_indices:
            continue

        indices = hb_index_to_indices[str(hb_index)]
        for offset in range(indices[1] - indices[0]):
            entry_index = indices[0] + offset
            assert entry_index < len(
                manifest.entries
            ), f"entry index: {entry_index} >= {len(manifest.entries)}"
            manifest_entry = manifest.entries[entry_index]
            manifest_entry_referenced_list.append(manifest_entry)

        manifest = Manifest.of(
            entries=manifest_entry_referenced_list, uuid=str(uuid4())
        )
        delta = Delta.of(
            locator=DeltaLocator.of(write_to_partition.locator),
            delta_type=DeltaType.UPSERT,
            meta=manifest.meta,
            manifest=manifest,
            previous_stream_position=write_to_partition.stream_position,
            properties={},
        )
        referenced_pyarrow_write_result = PyArrowWriteResult.of(
            len(manifest_entry_referenced_list),
            manifest.meta.source_content_length,
            manifest.meta.content_length,
            manifest.meta.record_count,
        )
        materialize_result = MaterializeResult.of(
            delta=delta,
            task_index=hb_index,
            pyarrow_write_result=referenced_pyarrow_write_result,
            referenced_pyarrow_write_result=referenced_pyarrow_write_result,
        )
        materialize_result_list.append(materialize_result)
    return materialize_result_list


def _has_previous_compacted_table(input: MergeInput, hb_idx: int) -> bool:
    """
    Checks if the given hash bucket index has a compacted table available from the previous compaction round.

    Args:
        input (MergeInput): The input for the merge operation.
        hb_idx (int): The hash bucket index to check.

    Returns:
        bool: True if the hash bucket index has a compacted table available, False otherwise.
    """
    return (
        input.round_completion_info
        and input.round_completion_info.hb_index_to_entry_range
        and input.round_completion_info.hb_index_to_entry_range.get(str(hb_idx))
        is not None
    )


def _can_copy_by_reference(
    has_delete: bool, merge_file_group: MergeFileGroup, input: MergeInput
) -> bool:
    """
    Can copy by reference only if there are no deletes to merge in
    and previous compacted stream id matches that of new stream
    """
    copy_by_ref = (
        not has_delete
        and not merge_file_group.dfe_groups
        and input.round_completion_info is not None
    )

    if input.disable_copy_by_reference:
        copy_by_ref = False

    logger.info(f"Copy by reference is {copy_by_ref} for {merge_file_group.hb_index}")

    return copy_by_ref


def _flatten_dfe_list(
    df_envelopes_list: List[List[DeltaFileEnvelope]],
) -> List[DeltaFileEnvelope]:
    """
    Flattens a list of lists of DeltaFileEnvelope objects into a single list of DeltaFileEnvelope objects.

    Args:
        df_envelopes_list (List[List[DeltaFileEnvelope]]): A list of lists of DeltaFileEnvelope objects.

    Returns:
        List[DeltaFileEnvelope]: A flattened list of DeltaFileEnvelope objects.
    """
    if not df_envelopes_list:
        return []
    return [d for dfe_list in df_envelopes_list for d in dfe_list]


def _sort_df_envelopes(
    df_envelopes: List[DeltaFileEnvelope],
    key: Callable = lambda df: (df.stream_position, df.file_index),
) -> List[DeltaFileEnvelope]:
    """
    Sorts a list of DeltaFileEnvelope objects based on a specified key function.

    Args:
        df_envelopes (List[DeltaFileEnvelope]): A list of DeltaFileEnvelope objects.
        key (Callable, optional): A function that takes a DeltaFileEnvelope object and returns a key for sorting.
            Defaults to lambda df: (df.stream_position, df.file_index).

    Returns:
        List[DeltaFileEnvelope]: A sorted list of DeltaFileEnvelope objects.
    """
    if not df_envelopes:
        return []
    return sorted(
        df_envelopes,
        key=key,
        reverse=False,  # ascending
    )


def _group_sequence_by_delta_type(
    df_envelopes: List[DeltaFileEnvelope],
) -> Iterator[Tuple[List, List]]:
    """
    Groups a list of DeltaFileEnvelope objects by their delta_type.

    Args:
        df_envelopes (List[DeltaFileEnvelope]): A list of DeltaFileEnvelope objects.

    Yields:
        Iterator[Tuple[DeltaType, List[DeltaFileEnvelope]]]: A tuple containing the delta_type
        and a list of DeltaFileEnvelope objects that share the same delta_type.
    """
    iter_df_envelopes = iter(df_envelopes)
    for delta_type, delta_type_sequence in itertools.groupby(
        iter_df_envelopes, lambda x: x.delta_type
    ):
        yield delta_type, list(delta_type_sequence)


def _compact_tables(
    input: MergeInput,
    dfe_list: Optional[List[List[DeltaFileEnvelope]]],
    hb_idx: int,
    compacted_table: Optional[pa.Table] = None,
) -> Tuple[pa.Table, int, int, int]:
    """
    Compacts a list of DeltaFileEnvelope objects into a single PyArrow table.

    Args:
        input (MergeInput): The input for the merge operation.
        dfe_list (List[List[DeltaFileEnvelope]]): A list of lists of DeltaFileEnvelope objects.
        hb_idx (int): The hash bucket index for the compaction.

    Returns:
        Tuple[pa.Table, int, int, int]: A tuple containing:
            1. The compacted PyArrow table.
            2. The total number of records in the incremental data.
            3. The total number of deduplicated records.
            4. The total number of deleted records due to DELETE operations.
    """
    df_envelopes: List[DeltaFileEnvelope] = _flatten_dfe_list(dfe_list)
    delete_file_envelopes = input.delete_file_envelopes or []
    reordered_all_dfes: List[DeltaFileEnvelope] = _sort_df_envelopes(
        delete_file_envelopes + df_envelopes
    )
    assert all(
        dfe.delta_type in (DeltaType.UPSERT, DeltaType.DELETE)
        for dfe in reordered_all_dfes
    ), "All reordered delta file envelopes must be of the UPSERT or DELETE"
    table = compacted_table
    aggregated_incremental_len = 0
    aggregated_deduped_records = 0
    aggregated_dropped_records = 0
    for i, (delta_type, delta_type_sequence) in enumerate(
        _group_sequence_by_delta_type(reordered_all_dfes)
    ):
        if delta_type is DeltaType.UPSERT:
            (
                table,
                incremental_len,
                deduped_records,
                merge_time,
            ) = _apply_upserts(input, delta_type_sequence, hb_idx, table)
            logger.info(
                f" [Merge task index {input.merge_task_index}] Merged"
                f" record count: {len(table)}, size={table.nbytes} took: {merge_time}s"
            )
            aggregated_incremental_len += incremental_len
            aggregated_deduped_records += deduped_records
        elif delta_type is DeltaType.DELETE:
            table_size_before_delete = len(table) if table else 0
            (table, dropped_rows), delete_time = timed_invocation(
                func=input.delete_strategy.apply_many_deletes,
                table=table,
                delete_file_envelopes=delta_type_sequence,
            )
            logger.info(
                f" [Merge task index {input.merge_task_index}]"
                + f" Dropped record count: {dropped_rows} from table"
                + f" of record count {table_size_before_delete} took: {delete_time}s"
            )
            aggregated_dropped_records += dropped_rows
    return (
        table,
        aggregated_incremental_len,
        aggregated_deduped_records,
        aggregated_dropped_records,
    )


def _apply_upserts(
    input: MergeInput,
    dfe_list: List[DeltaFileEnvelope],
    hb_idx,
    prev_table=None,
) -> Tuple[pa.Table, int, int, int]:
    assert all(
        dfe.delta_type is DeltaType.UPSERT for dfe in dfe_list
    ), "All incoming delta file envelopes must of the DeltaType.UPSERT"
    logger.info(
        f"[Hash bucket index {hb_idx}] Reading dedupe input for "
        f"{len(dfe_list)} delta file envelope lists..."
    )
    table = _build_incremental_table(dfe_list)
    incremental_len = len(table)
    logger.info(
        f"[Hash bucket index {hb_idx}] Got the incremental table of length {incremental_len}"
    )
    if input.sort_keys:
        # Incremental is sorted and merged, as sorting
        # on non event based sort key does not produce consistent
        # compaction results. E.g., compaction(delta1, delta2, delta3)
        # will not be equal to compaction(compaction(delta1, delta2), delta3).
        table = table.sort_by(input.sort_keys)
    hb_table_record_count = len(table) + (len(prev_table) if prev_table else 0)
    table, merge_time = timed_invocation(
        func=_merge_tables,
        table=table,
        primary_keys=input.primary_keys,
        can_drop_duplicates=input.drop_duplicates,
        compacted_table=prev_table,
    )
    deduped_records = hb_table_record_count - len(table)
    return table, incremental_len, deduped_records, merge_time


def _copy_manifests_from_hash_bucketing(
    input: MergeInput, hb_index_copy_by_reference_ids: List[int]
) -> List[MaterializeResult]:
    materialized_results: List[MaterializeResult] = []

    if input.round_completion_info:
        referenced_materialized_results = (
            _copy_all_manifest_files_from_old_hash_buckets(
                hb_index_copy_by_reference_ids,
                input.round_completion_info,
                input.write_to_partition,
                input.deltacat_storage,
                input.deltacat_storage_kwargs,
            )
        )
        logger.info(
            f"Copying {len(referenced_materialized_results)} manifest files by reference..."
        )
        materialized_results.extend(referenced_materialized_results)

    return materialized_results


@success_metric(name=MERGE_SUCCESS_COUNT)
@failure_metric(name=MERGE_FAILURE_COUNT)
@categorize_errors
def _timed_merge(input: MergeInput) -> MergeResult:
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with (
        memray.Tracker(f"merge_{worker_id}_{task_id}.bin")
        if input.enable_profiler
        else nullcontext()
    ):
        total_input_records, total_deduped_records = 0, 0
        total_dropped_records = 0
        materialized_results: List[MaterializeResult] = []
        merge_file_groups = input.merge_file_groups_provider.create()
        hb_index_copy_by_ref_ids = []

        for merge_file_group in merge_file_groups:
            compacted_table = None
            has_delete = input.delete_file_envelopes is not None
            if has_delete:
                assert (
                    input.delete_strategy is not None
                ), "Merge input missing delete_strategy"
            if _can_copy_by_reference(
                has_delete=has_delete, merge_file_group=merge_file_group, input=input
            ):
                hb_index_copy_by_ref_ids.append(merge_file_group.hb_index)
                continue

            if _has_previous_compacted_table(input, merge_file_group.hb_index):
                compacted_table = _download_compacted_table(
                    hb_index=merge_file_group.hb_index,
                    rcf=input.round_completion_info,
                    read_kwargs_provider=input.read_kwargs_provider,
                    deltacat_storage=input.deltacat_storage,
                    deltacat_storage_kwargs=input.deltacat_storage_kwargs,
                )
            if not merge_file_group.dfe_groups and compacted_table is None:
                logger.warning(
                    f" [Hash bucket index {merge_file_group.hb_index}]"
                    + f" No new deltas and no compacted table found. Skipping compaction for {merge_file_group.hb_index}"
                )
                continue
            table, input_records, deduped_records, dropped_records = _compact_tables(
                input,
                merge_file_group.dfe_groups,
                merge_file_group.hb_index,
                compacted_table,
            )
            total_input_records += input_records
            total_deduped_records += deduped_records
            total_dropped_records += dropped_records
            materialized_results.append(
                merge_utils.materialize(input, merge_file_group.hb_index, [table])
            )

        if hb_index_copy_by_ref_ids:
            materialized_results.extend(
                _copy_manifests_from_hash_bucketing(input, hb_index_copy_by_ref_ids)
            )

        logger.info(
            f"[Hash group index: {input.merge_file_groups_provider.hash_group_index}]"
            f" Total number of materialized results produced: {len(materialized_results)} "
        )

        peak_memory_usage_bytes = get_current_process_peak_memory_usage_in_bytes()
        logger.info(
            f"Peak memory usage in bytes after merge: {peak_memory_usage_bytes}"
        )

        return MergeResult(
            materialized_results,
            np.int64(total_input_records),
            np.int64(total_deduped_records),
            np.int64(total_dropped_records),
            np.double(peak_memory_usage_bytes),
            np.double(0.0),
            np.double(time.time()),
        )


@ray.remote
def merge(input: MergeInput) -> MergeResult:
    with ProcessUtilizationOverTimeRange() as process_util:
        logger.info(f"Starting merge task {input.merge_task_index}...")

        # Log node peak memory utilization every 10 seconds
        def log_peak_memory():
            logger.debug(
                f"Process peak memory utilization so far: {process_util.max_memory} bytes "
                f"({process_util.max_memory/BYTES_PER_GIBIBYTE} GB)"
            )

        if input.memory_logs_enabled:
            process_util.schedule_callback(log_peak_memory, 10)

        merge_result, duration = timed_invocation(func=_timed_merge, input=input)

        emit_metrics_time = 0.0
        if input.metrics_config:
            emit_result, latency = timed_invocation(
                func=emit_timer_metrics,
                metrics_name=MERGE_TIME_IN_SECONDS,
                value=duration,
                metrics_config=input.metrics_config,
            )
            emit_metrics_time = latency

        logger.info(f"Finished merge task {input.merge_task_index}...")
        return MergeResult(
            merge_result[0],
            merge_result[1],
            merge_result[2],
            merge_result[3],
            merge_result[4],
            np.double(emit_metrics_time),
            merge_result[6],
        )
