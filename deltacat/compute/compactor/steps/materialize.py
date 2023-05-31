import importlib
import logging
import time
from collections import defaultdict
from contextlib import nullcontext
from itertools import chain, repeat
from typing import List, Optional, Tuple, Dict, Any
import pyarrow as pa
import ray
from ray import cloudpickle
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
from deltacat.storage import Delta, DeltaLocator, Partition, PartitionLocator
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.utils.common import ReadKwargsProvider
from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES, ContentType
from deltacat.types.tables import TABLE_CLASS_TO_SIZE_FUNC
from deltacat.utils.performance import timed_invocation
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
    enable_profiler: bool,
    metrics_config: MetricsConfig,
    schema: Optional[pa.Schema] = None,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
) -> MaterializeResult:
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
            delta,
            mat_bucket_index,
            # TODO (pdames): Generalize WriteResult to contain in-memory-table-type
            #  and in-memory-table-bytes instead of tight coupling to paBytes
            PyArrowWriteResult.of(
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
        dedupe_task_idx_and_obj_ref_tuples = [
            (
                t1,
                cloudpickle.loads(t2),
            )
            for t1, t2 in dedupe_task_idx_and_obj_id_tuples
        ]
        logger.info(f"Resolved materialize task obj refs...")
        dedupe_task_indices, obj_refs = zip(*dedupe_task_idx_and_obj_ref_tuples)
        # this depends on `ray.get` result order matching input order, as per the
        # contract established in: https://github.com/ray-project/ray/pull/16763
        src_file_records_list = ray.get(list(obj_refs))
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
        total_download_time = 0
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
                deltacat_storage.get_delta_manifest(delta_locator),
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
            pa_table, download_delta_manifest_entry_time = timed_invocation(
                deltacat_storage.download_delta_manifest_entry,
                Delta.of(delta_locator, None, None, None, manifest),
                src_file_idx_np.item(),
                file_reader_kwargs_provider=read_kwargs_provider,
            )
            total_download_time += download_delta_manifest_entry_time
            logger.debug(
                f"Time taken for materialize task"
                f" to download delta locator {delta_locator} with entry ID {src_file_idx_np.item()}"
                f" is: {download_delta_manifest_entry_time}s"
            )
            mask_pylist = list(repeat(False, len(pa_table)))
            record_numbers = chain.from_iterable(record_numbers_tpl)
            # TODO(raghumdani): reference the same file URIs while writing the files
            # instead of copying the data over and creating new files.
            for record_number in record_numbers:
                mask_pylist[record_number] = True
            mask = pa.array(mask_pylist)
            pa_table = pa_table.filter(mask)
            record_batch_tables.append(pa_table)
            if record_batch_tables.has_batches():
                batched_tables = record_batch_tables.evict()
                materialized_results.append(_materialize(batched_tables))

        if record_batch_tables.has_remaining():
            materialized_results.append(_materialize(record_batch_tables.remaining))

        merged_delta = Delta.merge_deltas([mr.delta for mr in materialized_results])
        assert (
            materialized_results and len(materialized_results) > 0
        ), f"Expected at least one materialized result in materialize step."

        write_results = [mr.pyarrow_write_result for mr in materialized_results]
        logger.debug(
            f"{len(write_results)} files written"
            f" with records: {[wr.records for wr in write_results]}"
        )
        # Merge all new deltas into one for this materialize bucket index
        merged_materialize_result = MaterializeResult.of(
            merged_delta,
            materialized_results[0].task_index,
            PyArrowWriteResult.union(
                [mr.pyarrow_write_result for mr in materialized_results]
            ),
        )

        logger.info(f"Finished materialize task...")
        end = time.time()
        duration = end - start
        if metrics_config:
            emit_timer_metrics(
                metrics_name="materialize",
                value=duration,
                metrics_config=metrics_config,
            )
        logger.info(f"Materialize task ended in {end - start}s")
        return (
            merged_materialize_result,
            duration,
            len(write_results),
            total_download_time,
        )
