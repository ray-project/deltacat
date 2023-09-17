import logging
import importlib
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
import numpy as np
import pyarrow as pa
import ray
import time
import pyarrow.compute as pc
from uuid import uuid4
from collections import defaultdict
from deltacat import logs
from typing import List, Optional
from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES
from deltacat.compute.compactor_v2.model.merge_result import MergeResult
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.compute.compactor import (
    RoundCompletionInfo,
    DeltaFileEnvelope,
)
from deltacat.utils.common import ReadKwargsProvider

from contextlib import nullcontext
from deltacat.types.tables import TABLE_CLASS_TO_SIZE_FUNC
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.compute.compactor.utils import system_columns as sc

from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics
from deltacat.utils.resources import get_current_node_peak_memory_usage_in_bytes
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    generate_pk_hash_column,
    hash_group_index_to_hash_bucket_indices,
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
    hash_bucket_index: int,
    df_envelopes_list: List[List[DeltaFileEnvelope]],
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

    This method ensures the appropriate deltas of types DELETE/UPSERT are correctly
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

        records_to_keep = pc.invert(
            pc.is_in(
                compacted_table[sc._PK_HASH_STRING_COLUMN_NAME],
                incremental_table[sc._PK_HASH_STRING_COLUMN_NAME],
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


def _timed_merge(input: MergeInput) -> MergeResult:
    def _materialize(
        hash_bucket_index,
        compacted_tables: List[pa.Table],
    ) -> MaterializeResult:
        compacted_table = pa.concat_tables(compacted_tables)
        if input.compacted_file_content_type in DELIMITED_TEXT_CONTENT_TYPES:
            # TODO (rkenmi): Investigate if we still need to convert this table to pandas DataFrame
            # TODO (pdames): compare performance to pandas-native materialize path
            df = compacted_table.to_pandas(split_blocks=True, self_destruct=True)
            compacted_table = df
        delta, stage_delta_time = timed_invocation(
            input.deltacat_storage.stage_delta,
            compacted_table,
            input.write_to_partition,
            max_records_per_entry=input.max_records_per_output_file,
            content_type=input.compacted_file_content_type,
            s3_table_writer_kwargs=input.s3_table_writer_kwargs,
            **input.deltacat_storage_kwargs,
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
        assert manifest_records == len(compacted_table), (
            f"Unexpected Error: Materialized delta manifest record count "
            f"({manifest_records}) does not equal compacted table record count "
            f"({len(compacted_table)})"
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
        f"merge_{worker_id}_{task_id}.bin"
    ) if input.enable_profiler else nullcontext():
        # In V2, we need to mitigate risk of running out of memory here in cases of
        #  severe skew of primary key updates in deltas. By severe skew, we mean
        #  one hash bucket require more memory than a worker instance have.
        logger.info(
            f"[Merge task {input.merge_task_index}] Getting delta file envelope "
            f"groups for {len(input.dfe_groups_refs)} object refs..."
        )

        delta_file_envelope_groups_list = input.object_store.get_many(
            input.dfe_groups_refs
        )
        hb_index_to_delta_file_envelopes_list = defaultdict(list)
        for delta_file_envelope_groups in delta_file_envelope_groups_list:
            assert input.hash_bucket_count == len(delta_file_envelope_groups), (
                f"The hash bucket count must match the dfe size as {input.hash_bucket_count}"
                f" != {len(delta_file_envelope_groups)}"
            )

            for hb_idx, dfes in enumerate(delta_file_envelope_groups):
                if dfes:
                    hb_index_to_delta_file_envelopes_list[hb_idx].append(dfes)

        valid_hb_indices_iterable = hash_group_index_to_hash_bucket_indices(
            input.hash_group_index, input.hash_bucket_count, input.num_hash_groups
        )

        total_deduped_records = 0
        total_dfes_found = 0

        materialized_results: List[MaterializeResult] = []
        hb_index_copy_by_reference = []
        for hb_idx in valid_hb_indices_iterable:
            dfe_list = hb_index_to_delta_file_envelopes_list.get(hb_idx)

            if dfe_list:
                total_dfes_found += 1
                table = _build_incremental_table(hb_idx, dfe_list)

                incremental_len = len(table)
                logger.info(
                    f"Got the incremental table of length {incremental_len} for hash bucket {hb_idx}"
                )

                if input.sort_keys:
                    # Incremental is sorted and merged, as sorting
                    # on non event based sort key does not produce consistent
                    # compaction results. E.g., compaction(delta1, delta2, delta3)
                    # will not be equal to compaction(compaction(delta1, delta2), delta3).
                    table = table.sort_by(input.sort_keys)

                compacted_table = None
                if (
                    input.round_completion_info
                    and input.round_completion_info.hb_index_to_entry_range
                    and input.round_completion_info.hb_index_to_entry_range.get(
                        str(hb_idx)
                    )
                    is not None
                ):

                    compacted_table = _download_compacted_table(
                        hb_index=hb_idx,
                        rcf=input.round_completion_info,
                        read_kwargs_provider=input.read_kwargs_provider,
                        deltacat_storage=input.deltacat_storage,
                        deltacat_storage_kwargs=input.deltacat_storage_kwargs,
                    )

                hb_table_record_count = len(table) + (
                    len(compacted_table) if compacted_table else 0
                )

                table, merge_time = timed_invocation(
                    func=_merge_tables,
                    table=table,
                    primary_keys=input.primary_keys,
                    can_drop_duplicates=input.drop_duplicates,
                    compacted_table=compacted_table,
                )
                total_deduped_records += hb_table_record_count - len(table)

                logger.info(
                    f"[Merge task index {input.merge_task_index}] Merged "
                    f"record count: {len(table)}, size={table.nbytes} took: {merge_time}s"
                )

                materialized_results.append(_materialize(hb_idx, [table]))
            else:
                hb_index_copy_by_reference.append(hb_idx)

        if input.round_completion_info and hb_index_copy_by_reference:
            referenced_materialized_results = (
                _copy_all_manifest_files_from_old_hash_buckets(
                    hb_index_copy_by_reference,
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

        logger.info(
            "Total number of materialized results produced for "
            f"hash group index: {input.hash_group_index} is {len(materialized_results)}"
        )

        assert total_dfes_found == len(hb_index_to_delta_file_envelopes_list), (
            "The total dfe list does not match the input dfes from hash bucket as "
            f"{total_dfes_found} != {len(hb_index_to_delta_file_envelopes_list)}"
        )

        peak_memory_usage_bytes = get_current_node_peak_memory_usage_in_bytes()

        return MergeResult(
            materialized_results,
            np.int64(total_deduped_records),
            np.double(peak_memory_usage_bytes),
            np.double(0.0),
            np.double(time.time()),
        )


@ray.remote
def merge(input: MergeInput) -> MergeResult:

    logger.info(f"Starting merge task...")
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

    logger.info(f"Finished merge task...")
    return MergeResult(
        merge_result[0],
        merge_result[1],
        merge_result[2],
        np.double(emit_metrics_time),
        merge_result[4],
    )
