import importlib
import logging
import time
from uuid import uuid4
from collections import defaultdict
from contextlib import nullcontext
from itertools import chain, repeat
from typing import List, Optional, Tuple, Dict, Any
import pyarrow as pa
import numpy as np
import ray
from deltacat import logs
from deltacat.compute.compactor import (
    MaterializeResult,
    PyArrowWriteResult,
    RoundCompletionInfo,
)
from deltacat.compute.compactor.steps.dedupe import (
    DedupeTaskIndexWithObjectId,
    DeltaFileLocatorToRecords,
)
from deltacat.storage import (
    Delta,
    DeltaLocator,
    DeltaType,
    Partition,
    PartitionLocator,
    Manifest,
    ManifestEntry,
)
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.utils.common import ReadKwargsProvider
from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES, ContentType
from deltacat.types.tables import TABLE_CLASS_TO_SIZE_FUNC
from deltacat.utils.performance import timed_invocation
from deltacat.io.object_store import IObjectStore
from deltacat.utils.pyarrow import (
    ReadKwargsProviderPyArrowCsvPureUtf8,
    ReadKwargsProviderPyArrowSchemaOverride,
    RecordBatchTables,
)
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.metrics import emit_timer_metrics, MetricsConfig
from deltacat.utils.resources import get_current_process_peak_memory_usage_in_bytes

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def materialize(
    source_partition_locator: PartitionLocator,
    round_completion_info: Optional[RoundCompletionInfo],
    partition: Partition,
    mat_bucket_index: int,
    dedupe_task_idx_and_obj_id_tuples: List[DedupeTaskIndexWithObjectId],
    max_records_per_output_file: int,
    compacted_file_content_type: ContentType,
    enable_manifest_entry_copy_by_reference: bool,
    enable_profiler: bool,
    metrics_config: MetricsConfig,
    schema: Optional[pa.Schema] = None,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    object_store: Optional[IObjectStore] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
):
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}

    def _stage_delta_from_manifest_entry_reference_list(
        manifest_entry_list_reference: List[ManifestEntry],
        partition: Partition,
        delta_type: DeltaType = DeltaType.UPSERT,
    ) -> Delta:
        assert (
            delta_type == DeltaType.UPSERT
        ), "Stage delta with existing manifest entries only supports UPSERT delta type!"
        manifest = Manifest.of(entries=manifest_entry_list_reference, uuid=str(uuid4()))
        delta = Delta.of(
            locator=DeltaLocator.of(partition.locator),
            delta_type=delta_type,
            meta=manifest.meta,
            manifest=manifest,
            previous_stream_position=partition.stream_position,
            properties={},
        )
        return delta

    # TODO (rkenmi): Add docstrings for the steps in the compaction workflow
    #  https://github.com/ray-project/deltacat/issues/79
    def _materialize(compacted_tables: List[pa.Table]) -> MaterializeResult:
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
            **deltacat_storage_kwargs,
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
            task_index=mat_bucket_index,
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

    logger.info(
        f"Starting materialize task with"
        f" materialize bucket index: {mat_bucket_index}..."
    )
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"dedupe_{worker_id}_{task_id}.bin"
    ) if enable_profiler else nullcontext():
        start = time.time()
        logger.info(f"Resolved materialize task obj refs...")
        dedupe_task_indices, obj_refs = zip(*dedupe_task_idx_and_obj_id_tuples)
        # this depends on `ray.get` result order matching input order, as per the
        # contract established in: https://github.com/ray-project/ray/pull/16763
        src_file_records_list = object_store.get_many(list(obj_refs))
        all_src_file_records = defaultdict(list)
        for i, src_file_records in enumerate(src_file_records_list):
            dedupe_task_idx = dedupe_task_indices[i]
            for src_dfl, record_numbers in src_file_records.items():
                all_src_file_records[src_dfl].append(
                    (record_numbers, repeat(dedupe_task_idx, len(record_numbers)))
                )
        manifest_cache = {}
        materialized_results: List[MaterializeResult] = []
        record_batch_tables = RecordBatchTables(max_records_per_output_file)
        count_of_src_dfl = 0
        manifest_entry_list_reference = []
        referenced_pyarrow_write_results = []
        for src_dfl in sorted(all_src_file_records.keys()):
            record_numbers_dd_task_idx_tpl_list: List[
                Tuple[DeltaFileLocatorToRecords, repeat]
            ] = all_src_file_records[src_dfl]
            record_numbers_tpl, dedupe_task_idx_iter_tpl = zip(
                *record_numbers_dd_task_idx_tpl_list
            )
            is_src_partition_file_np = src_dfl.is_source_delta
            src_stream_position_np = src_dfl.stream_position
            src_file_idx_np = src_dfl.file_index
            src_file_record_count = src_dfl.file_record_count.item()
            count_of_src_dfl += 1
            src_file_partition_locator = (
                source_partition_locator
                if is_src_partition_file_np
                else round_completion_info.compacted_delta_locator.partition_locator
            )
            delta_locator = DeltaLocator.of(
                src_file_partition_locator,
                src_stream_position_np.item(),
            )
            dl_digest = delta_locator.digest()
            manifest = manifest_cache.setdefault(
                dl_digest,
                deltacat_storage.get_delta_manifest(
                    delta_locator, **deltacat_storage_kwargs
                ),
            )

            if read_kwargs_provider is None:
                # for delimited text output, disable type inference to prevent
                # unintentional type-casting side-effects and improve performance
                if compacted_file_content_type in DELIMITED_TEXT_CONTENT_TYPES:
                    read_kwargs_provider = ReadKwargsProviderPyArrowCsvPureUtf8()
                # enforce a consistent schema if provided, when reading files into PyArrow tables
                elif schema is not None:
                    read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
                        schema=schema
                    )
            record_numbers = chain.from_iterable(record_numbers_tpl)
            record_numbers_length = 0
            mask_pylist = list(repeat(False, src_file_record_count))
            for record_number in record_numbers:
                record_numbers_length += 1
                mask_pylist[record_number] = True
            if (
                round_completion_info
                and enable_manifest_entry_copy_by_reference
                and record_numbers_length == src_file_record_count
                and src_file_partition_locator
                == round_completion_info.compacted_delta_locator.partition_locator
            ):
                logger.debug(
                    f"Untouched manifest file found, "
                    f"record numbers length: {record_numbers_length} "
                    f"same as downloaded table length: {src_file_record_count}"
                )
                untouched_src_manifest_entry = manifest.entries[src_file_idx_np.item()]
                manifest_entry_list_reference.append(untouched_src_manifest_entry)
                referenced_pyarrow_write_result = PyArrowWriteResult.of(
                    1,
                    untouched_src_manifest_entry.meta.source_content_length,
                    untouched_src_manifest_entry.meta.content_length,
                    src_file_record_count,
                )
                referenced_pyarrow_write_results.append(referenced_pyarrow_write_result)
            else:
                pa_table, download_delta_manifest_entry_time = timed_invocation(
                    deltacat_storage.download_delta_manifest_entry,
                    Delta.of(delta_locator, None, None, None, manifest),
                    src_file_idx_np.item(),
                    file_reader_kwargs_provider=read_kwargs_provider,
                    **deltacat_storage_kwargs,
                )
                logger.debug(
                    f"Time taken for materialize task"
                    f" to download delta locator {delta_locator} with entry ID {src_file_idx_np.item()}"
                    f" is: {download_delta_manifest_entry_time}s"
                )
                mask = pa.array(mask_pylist)
                pa_table = pa_table.filter(mask)
                record_batch_tables.append(pa_table)
                if record_batch_tables.has_batches():
                    batched_tables = record_batch_tables.evict()
                    materialized_results.append(_materialize(batched_tables))

        if record_batch_tables.has_remaining():
            materialized_results.append(_materialize(record_batch_tables.remaining))

        logger.info(f"Got {count_of_src_dfl} source delta files during materialize")
        referenced_manifest_delta = (
            _stage_delta_from_manifest_entry_reference_list(
                manifest_entry_list_reference, partition
            )
            if manifest_entry_list_reference
            else None
        )

        merged_materialized_delta = [mr.delta for mr in materialized_results]
        merged_materialized_delta.append(referenced_manifest_delta)
        merged_delta = Delta.merge_deltas(
            [d for d in merged_materialized_delta if d is not None]
        )

        write_results_union = [*referenced_pyarrow_write_results]
        if materialized_results:
            for mr in materialized_results:
                write_results_union.append(mr.pyarrow_write_result)
        write_result = PyArrowWriteResult.union(write_results_union)
        referenced_write_result = PyArrowWriteResult.union(
            referenced_pyarrow_write_results
        )

        if referenced_manifest_delta:
            logger.info(
                f"Got delta with {len(referenced_manifest_delta.manifest.entries)} referenced manifest entries"
            )
            assert referenced_write_result.files == len(
                referenced_manifest_delta.manifest.entries
            ), "The files referenced must match with the entries in the delta"

        assert write_result.files == len(
            merged_delta.manifest.entries
        ), "The total number of files written by materialize must match manifest entries"

        logger.debug(
            f"{write_result.files} files written"
            f" with records: {write_result.records}"
        )

        logger.info(f"Finished materialize task...")
        end = time.time()
        duration = end - start

        emit_metrics_time = 0.0
        if metrics_config:
            emit_result, latency = timed_invocation(
                func=emit_timer_metrics,
                metrics_name="materialize",
                value=duration,
                metrics_config=metrics_config,
            )
            emit_metrics_time = latency
        logger.info(f"Materialize task ended in {end - start}s")

        peak_memory_usage_bytes = get_current_process_peak_memory_usage_in_bytes()

        # Merge all new deltas into one for this materialize bucket index
        merged_materialize_result = MaterializeResult.of(
            merged_delta,
            mat_bucket_index,
            write_result,
            referenced_write_result,
            np.double(peak_memory_usage_bytes),
            np.double(emit_metrics_time),
            np.double(time.time()),
        )

        return merged_materialize_result
