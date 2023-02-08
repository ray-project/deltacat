import logging
import ray
import pyarrow as pa

from collections import defaultdict
from itertools import chain, repeat
from typing import List, Optional, Tuple

import pyarrow as pa
import ray
from pyarrow import compute as pc
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
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.storage import Delta, DeltaLocator, Partition, PartitionLocator
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES, ContentType
from deltacat.types.tables import TABLE_CLASS_TO_SIZE_FUNC
from deltacat.utils.pyarrow import (
    ReadKwargsProviderPyArrowCsvPureUtf8,
    ReadKwargsProviderPyArrowSchemaOverride, slice_table_generator,
)

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
        schema: Optional[pa.Schema] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> MaterializeResult:
    def _materialize(
            compacted_tables: List[pa.Table]) -> MaterializeResult:
        compacted_tables_size = sum([TABLE_CLASS_TO_SIZE_FUNC[type(tbl)](tbl)
                                     for tbl in compacted_tables])
        logger.debug(f"Concatenating {len(compacted_tables)} compacted tables"
                     f" with total size: {compacted_tables_size} bytes")
        compacted_table = pa.concat_tables(compacted_tables)

        if compacted_file_content_type in DELIMITED_TEXT_CONTENT_TYPES:
            # TODO (ricmiyam): Investigate if we still need to convert this table to pandas DataFrame
            # TODO (pdames): compare performance to pandas-native materialize path
            df = compacted_table.to_pandas(
                split_blocks=True,
                self_destruct=True,
                zero_copy_only=True
            )
            compacted_table = df
        delta = deltacat_storage.stage_delta(
            compacted_table,
            partition,
            max_records_per_entry=max_records_per_output_file,
            content_type=compacted_file_content_type,
        )
        manifest = delta.manifest
        manifest_records = manifest.meta.record_count
        assert (manifest_records == len(compacted_table),
                f"Unexpected Error: Materialized delta manifest record count "
                f"({manifest_records}) does not equal compacted table record count "
                f"({len(compacted_table)})")
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

    logger.info(f"Starting materialize task...")
    dedupe_task_idx_and_obj_ref_tuples = [
        (
            t1,
            cloudpickle.loads(t2),
        ) for t1, t2 in dedupe_task_idx_and_obj_id_tuples
    ]
    logger.info(f"Resolved materialize task obj refs...")
    dedupe_task_indices, obj_refs = zip(
        *dedupe_task_idx_and_obj_ref_tuples
    )
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
    remainder_tables = []
    materialized_results: List[MaterializeResult] = []
    prev_remainder_record_count = 0
    for src_dfl in sorted(all_src_file_records.keys()):
        record_numbers_dd_task_idx_tpl_list: List[Tuple[DeltaFileLocatorToRecords, repeat]] = \
            all_src_file_records[src_dfl]
        record_numbers_tpl, dedupe_task_idx_iter_tpl = zip(
            *record_numbers_dd_task_idx_tpl_list
        )
        is_src_partition_file_np = src_dfl.is_source_delta
        src_stream_position_np = src_dfl.stream_position
        src_file_idx_np = src_dfl.file_index
        src_file_partition_locator = source_partition_locator \
            if is_src_partition_file_np \
            else round_completion_info.compacted_delta_locator.partition_locator
        delta_locator = DeltaLocator.of(
            src_file_partition_locator,
            src_stream_position_np.item(),
        )
        dl_digest = delta_locator.digest()

        manifest = manifest_cache.setdefault(
            dl_digest,
            deltacat_storage.get_delta_manifest(delta_locator),
        )

        read_kwargs_provider = None
        # for delimited text output, disable type inference to prevent
        # unintentional type-casting side-effects and improve performance
        if compacted_file_content_type in DELIMITED_TEXT_CONTENT_TYPES:
            read_kwargs_provider = ReadKwargsProviderPyArrowCsvPureUtf8()
        # enforce a consistent schema if provided, when reading files into PyArrow tables
        elif schema is not None:
            read_kwargs_provider = ReadKwargsProviderPyArrowSchemaOverride(
                schema=schema)
        pa_table = deltacat_storage.download_delta_manifest_entry(
            Delta.of(delta_locator, None, None, None, manifest),
            src_file_idx_np.item(),
            file_reader_kwargs_provider=read_kwargs_provider,
        )
        mask_pylist = list(repeat(False, len(pa_table)))
        record_numbers = chain.from_iterable(record_numbers_tpl)
        # TODO(raghumdani): reference the same file URIs while writing the files
        # instead of copying the data over and creating new files. 
        for record_number in record_numbers:
            mask_pylist[record_number] = True
        mask = pa.array(mask_pylist)
        pa_table = pa_table.filter(mask)

        if remainder_tables:
            assert prev_remainder_record_count < max_records_per_output_file, \
                f"Total records in previous remainder should not exceed {max_records_per_output_file}."

            # Skip materializing tables until we have reached 'max_records_per_output_file'
            if prev_remainder_record_count + len(pa_table) < max_records_per_output_file:
                remainder_tables.append(pa_table)
                prev_remainder_record_count += len(pa_table)
                continue

            # 'max_records_per_output_file' will be exceeded with the
            #  current file's records + previous remainder records.
            records_to_fit = new_file_record_offset = max_records_per_output_file - prev_remainder_record_count
            fitted_table = pa_table.slice(length=records_to_fit)
            remainder_tables.append(fitted_table)
            remainder_records = sum([len(tbl) for tbl in remainder_tables])
            assert max_records_per_output_file == remainder_records, \
                f"Expected previous remainder records + partial records of current table" \
                f" to be fitted to {max_records_per_output_file}, but got {remainder_records} instead."
            materialized_results.append(_materialize(remainder_tables))
            # Free up written tables in memory
            remainder_tables.clear()
            prev_remainder_record_count = 0

            # Update the starting record offset of the current file
            if new_file_record_offset < len(pa_table):
                pa_table = pa_table.slice(offset=new_file_record_offset)
                assert len(pa_table), f"Expected non-empty table after slicing by {max_records_per_output_file}"

        # Slice the table into 'max_records_per_output_file' chunks.
        for sliced_table in slice_table_generator(pa_table, max_records_per_output_file):
            if len(sliced_table) == max_records_per_output_file:
                materialized_results.append(_materialize([sliced_table]))
            else:
                remainder_tables.append(sliced_table)
                prev_remainder_record_count += len(sliced_table)

    if remainder_tables:
        materialized_results.append(_materialize(remainder_tables))
        # Free up written tables in memory
        remainder_tables.clear()

    merged_delta = Delta.merge_deltas([mr.delta for mr in materialized_results])
    assert materialized_results and len(materialized_results) > 0, \
        f"Expected at least one materialized result in materialize step."

    write_results = [mr.pyarrow_write_result for mr in materialized_results]
    logger.debug(f"{len(write_results)} files written"
                 f" with records: {[wr.records for wr in write_results]}")
    # Merge all new deltas into one for this materialize bucket index
    merged_materialize_result = MaterializeResult.of(merged_delta,
                                                     materialized_results[0].task_index,
                                                     PyArrowWriteResult.union([mr.pyarrow_write_result
                                                                               for mr in materialized_results]))

    logger.info(f"Finished materialize task...")
    return merged_materialize_result
