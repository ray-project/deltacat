import logging
import importlib
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
import numpy as np
import pyarrow as pa
import ray
from typing import Any, Callable
import time
from deltacat.io.object_store import IObjectStore
import pyarrow.compute as pc
import deltacat.compute.compactor_v2.utils.merge as merge_utils
from uuid import uuid4
from deltacat import logs
from typing import Iterator, List, Optional, Tuple
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
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
from deltacat.utils.metrics import emit_timer_metrics
from deltacat.utils.resources import (
    get_current_process_peak_memory_usage_in_bytes,
    ProcessUtilizationOverTimeRange,
)
from typing import Dict
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


if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def searchsorted_by_attr(
    attribute: str, obj_arr, values_to_insert, side="right", post_processing=None
):
    return np.searchsorted(
        [getattr(input, attribute) for input in obj_arr], values_to_insert, side
    )


def _does_hash_bucket_idx_have_compacted_table(input: MergeInput, hb_idx: int) -> bool:
    return (
        input.round_completion_info
        and input.round_completion_info.hb_index_to_entry_range
        and input.round_completion_info.hb_index_to_entry_range.get(str(hb_idx))
        is not None
    )


def _get_all_deletes(
    stream_positions_to_delete_obj_ref: Dict[int, Any],
    object_store: IObjectStore = None,
) -> Iterator[pa.Table]:
    for _, obj_ref in stream_positions_to_delete_obj_ref.items():
        yield object_store.get(obj_ref)


def _get_all_deletes2(
    deletes_to_apply_by_stream_positions_list: List[Tuple[int, Any, List[str]]],
    object_store: IObjectStore = None,
) -> Iterator[Tuple[pa.Table, str]]:
    for _, obj_ref, delete_column in deletes_to_apply_by_stream_positions_list:
        yield object_store.get(obj_ref), delete_column


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


def _flatten_df_envelopes_list(
    df_envelopes_list: List[List[DeltaFileEnvelope]],
) -> List[DeltaFileEnvelope]:
    return [d for dfe_list in df_envelopes_list for d in dfe_list]


def _sort_df_envelopes_list(
    df_envelopes: List[DeltaFileEnvelope],
    key: Callable = lambda df: (df.stream_position, df.file_index),
) -> List[DeltaFileEnvelope]:
    return sorted(
        df_envelopes,
        key=key,
        reverse=False,  # ascending
    )


def _build_incremental_table(
    df_envelopes: List[DeltaFileEnvelope],
    hb_idx: int,
    deletes_to_apply_by_stream_positions: Optional[Dict[int, str]] = None,
    object_store: IObjectStore = None,
) -> pa.Table:
    hb_tables = []
    for df_envelope in df_envelopes:
        table = df_envelope.table
        assert (
            df_envelope.delta_type != DeltaType.APPEND
        ), "APPEND type deltas are not supported. Kindly use UPSERT"
        assert (
            df_envelope.delta_type != DeltaType.DELETE
        ), "DELETE type deltas should not be present when building the incremental table"
        hb_tables.append(table)
    incremental_res: pa.Table = pa.concat_tables(hb_tables)
    return incremental_res


def _build_incremental_table(
    df_envelopes: List[DeltaFileEnvelope],
    hb_idx: int,
    deletes_to_apply_by_stream_positions: Optional[Dict[int, str]] = None,
    object_store: IObjectStore = None,
) -> pa.Table:
    hb_tables: List[Any] = []
    # sort by delta file stream position now instead of sorting every row later
    for df_envelope in df_envelopes:
        table = df_envelope.table
        assert (
            df_envelope.delta_type != DeltaType.APPEND
        ), "APPEND type deltas are not supported. Kindly use UPSERT"
        assert (
            df_envelope.delta_type != DeltaType.DELETE
        ), "DELETE type deltas should not be present when building the incremental table"
        if df_envelope.delta_type is DeltaType.UPSERT:
            if deletes_to_apply_by_stream_positions:
                # The input delta files contain deletes to apply immediately after this upsert based on stream position
                # before appending it to hb_tables
                try:
                    obj_ref = deletes_to_apply_by_stream_positions[
                        df_envelope.stream_position
                    ]
                except KeyError:
                    # the input delta files contain deletes but they are none after this stream position. Append to hb_tables normally
                    hb_tables.append(table)
                    continue
                deletes_to_apply = object_store.get(obj_ref)
                table, number_of_rows_dropped = drop_rows_affected_by_deletes(
                    table, deletes_to_apply
                )
                logger.info(
                    f"[Hash bucket index {hb_idx}]. Deletes found to apply to df_envelope"
                    f" {df_envelope.delta_type} table at stream position: {df_envelope.stream_position}"
                    f" . Dropped {number_of_rows_dropped} row(s) from incremental table."
                )
            hb_tables.append(table)
    incremental_res: pa.Table = pa.concat_tables(hb_tables)
    return incremental_res


def _get_delete_column_names(
    table: pa.Table, column_names: Optional[List[str]] = None
) -> List[str]:
    if table is None:
        return []
    if column_names:
        return column_names
    delete_column_names = [name for name in table.column_names]
    return delete_column_names


def drop_rows_affected_by_deletes(
    table: Optional[pa.Table],
    deletes_to_apply: pa.Table,
    delete_column_names: List[str] = None,
    equality_predicate_operation: Optional[Callable] = pa.compute.and_,
) -> Tuple[Any, int]:
    if not table:
        return table
    delete_column_names = _get_delete_column_names(
        deletes_to_apply, delete_column_names
    )
    drop_masks = [
        pc.is_in(
            table[delete_column_name], value_set=deletes_to_apply[delete_column_name]
        )
        for delete_column_name in delete_column_names
    ]
    for i, droppable in enumerate(drop_masks[: len(drop_masks) - 1]):
        curr_drop_mask = drop_masks[i]
        next_drop_mask = drop_masks[i + 1]
        # logical OR the mask arrays
        result_mask = equality_predicate_operation(curr_drop_mask, next_drop_mask)
        drop_masks[i + 1] = result_mask
    mask_with_all_droppable = drop_masks[-1].combine_chunks()
    number_of_rows_dropped: int = mask_with_all_droppable.true_count
    table = table.filter(pc.invert(mask_with_all_droppable))
    return table, number_of_rows_dropped


def _merge_tables(
    table: pa.Table,
    primary_keys: List[str],
    can_drop_duplicates: bool,
    compacted_table: Optional[pa.Table] = None,
) -> pa.Table:
    """
    Merges the table with compacted table dropping duplicates where necessary.

    This method ensures the appropriate deltas of types DELETE/UPSERT are correctly
    appended to the table.
    """
    # If all rows are dropped from both the table and compacted_table return None
    if not (table or compacted_table):
        return None

    # if all rows are dropped from the table just return the compacted table
    if not table and compacted_table:
        return compacted_table

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
        # we need not drop duplicates
        return pa.concat_tables(all_tables)

    all_tables = generate_pk_hash_column(all_tables, primary_keys=primary_keys)

    result_table_list = []

    incremental_table = drop_duplicates(
        all_tables[incremental_idx], on=sc._PK_HASH_STRING_COLUMN_NAME
    )

    if compacted_table:
        compacted_table = all_tables[0]

        records_to_keep = pc.invert(
            pc.is_in(
                compacted_table[sc._PK_HASH_STRING_COLUMN_NAME],
                incremental_table[sc._PK_HASH_STRING_COLUMN_NAME],
            )
        )

        result_table_list.append(compacted_table.filter(records_to_keep))

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
    compacted_table = pa.concat_tables(tables)
    return compacted_table


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
        logger.info(f"Nothing to copy by reference. Skipping...")
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


def apply_deletes(
    table: pa.Table,
    deletes_to_apply,
    delete_column_names: List[str],
    equality_predicate_operation: Optional[Callable] = pa.compute.and_,
) -> Tuple[Any, int]:
    # next = None
    for i, delete_column_name in enumerate(delete_column_names):
        boolean_mask = pc.is_in(
            table[delete_column_name], value_set=deletes_to_apply[delete_column_name]
        )
        logger.info(f"pdebug: {len(boolean_mask)=}")
        table = table.filter(pc.invert(boolean_mask))
        return table, len(boolean_mask)

    # drop_masks = [
    #     pc.is_in(
    #         table[delete_column_name], value_set=deletes_to_apply[delete_column_name]
    #     )
    #     for delete_column_name in delete_column_names
    # ]
    # for i, droppable in enumerate(drop_masks[: len(drop_masks) - 1]):
    #     curr_drop_mask = drop_masks[i]
    #     next_drop_mask = drop_masks[i + 1]
    #     # logical OR the mask arrays
    #     result_mask = equality_predicate_operation(curr_drop_mask, next_drop_mask)
    #     drop_masks[i + 1] = result_mask
    # mask_with_all_droppable = drop_masks[-1].combine_chunks()
    # number_of_rows_dropped: int = mask_with_all_droppable.true_count
    # table = table.filter(pc.invert(mask_with_all_droppable))
    # return table, number_of_rows_dropped


def _compact_tables(
    input: MergeInput, dfe_list: List[List[DeltaFileEnvelope]], hb_idx: int
) -> Tuple[pa.Table, int, int]:
    # NOTE: If there are no incremental deltas and a compacted table corresponds to the hash
    # bucket then just return the compacted table
    if dfe_list is None and _does_hash_bucket_idx_have_compacted_table(input, hb_idx):
        compacted_table = _download_compacted_table(
            hb_index=hb_idx,
            rcf=input.round_completion_info,
            read_kwargs_provider=input.read_kwargs_provider,
            deltacat_storage=input.deltacat_storage,
            deltacat_storage_kwargs=input.deltacat_storage_kwargs,
        )
        for delete_table, delete_column_name in _get_all_deletes2(
            input.deletes_to_apply_by_stream_positions_list, input.object_store
        ):
            compacted_table, number_of_rows_dropped = apply_deletes(
                compacted_table, delete_table, delete_column_name
            )
        return compacted_table, 0, 0
    logger.info(f"pdebug: {input=}, {dfe_list=}, {hb_idx=}")
    df_envelopes: List[DeltaFileEnvelope] = _sort_df_envelopes_list(
        _flatten_df_envelopes_list(dfe_list)
    )
    deletes_to_apply_by_stream_positions_list = (
        input.deletes_to_apply_by_stream_positions_list
    )
    logger.info(f"pdebug: {deletes_to_apply_by_stream_positions_list=}")
    delete_stream_positions = (
        [composite[0] for composite in deletes_to_apply_by_stream_positions_list]
        if deletes_to_apply_by_stream_positions_list
        else []
    )
    delete_indices: List[int] = searchsorted_by_attr(
        "stream_position", df_envelopes, delete_stream_positions
    )
    upsert_sequence_spos_to_delete_index = {}
    for delete_stream_position_index, delete_pos_in_upsert in enumerate(delete_indices):
        if delete_pos_in_upsert == 0:
            continue
        upsert_stream_pos = df_envelopes[delete_pos_in_upsert - 1].stream_position
        upsert_sequence_spos_to_delete_index[
            upsert_stream_pos
        ] = delete_stream_position_index
    split_up_df_envelopes: nd.array = np.split(df_envelopes, delete_indices)
    contiguous_upsert_seq_list: List[List[DeltaFileEnvelope]] = []
    for delete_stream_position_index, contiguous_upsert_sequence in enumerate(
        split_up_df_envelopes
    ):
        contiguous_upsert_sequence: List[
            DeltaFileEnvelope
        ] = contiguous_upsert_sequence.tolist()
        if contiguous_upsert_sequence:
            contiguous_upsert_seq_list.append(contiguous_upsert_sequence)
    prev_table = None
    total_incremental_len = 0
    total_deduped_records = 0
    for i, df_envelopes in enumerate(contiguous_upsert_seq_list):
        table = _build_incremental_table(
            df_envelopes,
            hb_idx,
            input.deletes_to_apply_by_stream_positions,
            input.object_store,
        )
        partial_incremental_len = len(table)
        logger.info(
            f"[Hash bucket index {hb_idx}] Got the incremental table of length {partial_incremental_len}"
        )
        if input.sort_keys:
            # Incremental is sorted and merged, as sorting
            # on non event based sort key does not produce consistent
            # compaction results. E.g., compaction(delta1, delta2, delta3)
            # will not be equal to compaction(compaction(delta1, delta2), delta3).
            table = table.sort_by(input.sort_keys)
        compacted_table = None
        if _does_hash_bucket_idx_have_compacted_table(input, hb_idx):
            compacted_table = _download_compacted_table(
                hb_index=hb_idx,
                rcf=input.round_completion_info,
                read_kwargs_provider=input.read_kwargs_provider,
                deltacat_storage=input.deltacat_storage,
                deltacat_storage_kwargs=input.deltacat_storage_kwargs,
            )
        partial_hb_table_record_count = len(table) + (
            len(prev_table) if prev_table else 0
        )
        table, merge_time = timed_invocation(
            func=_merge_tables,
            table=table,
            primary_keys=input.primary_keys,
            can_drop_duplicates=input.drop_duplicates,
            compacted_table=compacted_table,
        )
        if prev_table:
            table, merge_time = timed_invocation(
                func=_merge_tables,
                table=table,
                primary_keys=input.primary_keys,
                can_drop_duplicates=input.drop_duplicates,
                compacted_table=prev_table,
            )
        partial_deduped_records = partial_hb_table_record_count - len(table)
        if upsert_sequence_spos_to_delete_index:
            spos = df_envelopes[-1].stream_positionAccentu
            delete_index = upsert_sequence_spos_to_delete_index[spos]
            (
                _,
                delete_obj_ref,
                delete_column_names,
            ) = deletes_to_apply_by_stream_positions_list[delete_index]
            deletes_to_apply = input.object_store.get(delete_obj_ref)
            table, number_of_rows_dropped = apply_deletes(
                table, deletes_to_apply, delete_column_names
            )
        prev_table = table
        total_incremental_len += partial_incremental_len
        total_deduped_records += partial_deduped_records
    logger.info(
        f"[Merge task index {input.merge_task_index}] Merged "
        f"record count: {len(table)}, size={table.nbytes if len(table) > 0 else 0} took: {merge_time}s"
    )
    return table, total_incremental_len, total_deduped_records


def drop_rows_affected_by_deletes2(
    table: Optional[pa.Table],
    deletes_to_apply: pa.Table,
    delete_column_names: List[str] = None,
    equality_predicate_operation: Optional[Callable] = pa.compute.and_,
) -> Tuple[Any, int]:
    if not table:
        return table
    delete_column_names = _get_delete_column_names(
        deletes_to_apply, delete_column_names
    )
    drop_masks = [
        pc.is_in(
            table[delete_column_name], value_set=deletes_to_apply[delete_column_name]
        )
        for delete_column_name in delete_column_names
    ]
    for i, droppable in enumerate(drop_masks[: len(drop_masks) - 1]):
        curr_drop_mask = drop_masks[i]
        next_drop_mask = drop_masks[i + 1]
        # logical OR the mask arrays
        result_mask = equality_predicate_operation(curr_drop_mask, next_drop_mask)
        drop_masks[i + 1] = result_mask
    mask_with_all_droppable = drop_masks[-1].combine_chunks()
    number_of_rows_dropped: int = mask_with_all_droppable.true_count
    table = table.filter(pc.invert(mask_with_all_droppable))
    return table, number_of_rows_dropped


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


def _timed_merge(input: MergeInput) -> MergeResult:
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"merge_{worker_id}_{task_id}.bin"
    ) if input.enable_profiler else nullcontext():
        total_input_records, total_deduped_records = 0, 0
        materialized_results: List[MaterializeResult] = []
        merge_file_groups = input.merge_file_groups_provider.create()
        hb_index_copy_by_ref_ids = []
        for merge_file_group in merge_file_groups:
            # copy by reference only if deletes are not present
            can_copy_by_ref = (
                not merge_file_group.dfe_groups
                and not input.deletes_to_apply_by_stream_positions_list
            )
            if can_copy_by_ref:
                hb_index_copy_by_ref_ids.append(merge_file_group.hb_index)
                continue
            no_compacted_table_and_no_deltas = (
                not merge_file_group.dfe_groups
                and not _does_hash_bucket_idx_have_compacted_table(
                    input, merge_file_group.hb_index
                )
            )
            if no_compacted_table_and_no_deltas:
                continue
            table, input_records, deduped_records = _compact_tables(
                input, merge_file_group.dfe_groups, merge_file_group.hb_index
            )
            if table is None:
                continue
            total_input_records += input_records
            total_deduped_records += deduped_records
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

        process_util.schedule_callback(log_peak_memory, 10)

        merge_result, duration = timed_invocation(func=_timed_merge, input=input)

        emit_metrics_time = 0.0
        if input.metrics_config:
            emit_result, latency = timed_invocation(
                func=emit_timer_metrics,
                metrics_name="merge",
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
            np.double(emit_metrics_time),
            merge_result[4],
        )
