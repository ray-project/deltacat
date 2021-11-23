import logging
import ray
import pyarrow as pa

from collections import defaultdict
from itertools import chain, repeat

from pyarrow import compute as pc

from ray import cloudpickle

from deltacat import logs
from deltacat.storage import Delta, DeltaLocator, Partition, PartitionLocator, \
    interface as unimplemented_deltacat_storage
from deltacat.compute.compactor import MaterializeResult, PyArrowWriteResult
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.types.media import ContentType

from typing import Any, List, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def materialize(
        source_partition_locator: PartitionLocator,
        partition: Partition,
        mat_bucket_index: int,
        dedupe_task_idx_and_obj_id_tuples: List[Tuple[int, Any]],
        max_records_per_output_file: int,
        compacted_file_content_type: ContentType,
        deltacat_storage=unimplemented_deltacat_storage) -> MaterializeResult:

    logger.info(f"Starting materialize task...")
    dest_partition_locator = partition.locator
    dedupe_task_idx_and_obj_ref_tuples = [
        (
            t[0],
            cloudpickle.loads(t[1])
        ) for t in dedupe_task_idx_and_obj_id_tuples
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
    compacted_tables = []
    for src_dfl in sorted(all_src_file_records.keys()):
        record_numbers_dd_task_idx_tpl_list = all_src_file_records[src_dfl]
        record_numbers_tpl, dedupe_task_idx_iter_tpl = zip(
            *record_numbers_dd_task_idx_tpl_list
        )
        is_src_partition_file_np = src_dfl.is_source_delta
        src_stream_position_np = src_dfl.stream_position
        src_file_idx_np = src_dfl.file_index
        src_file_partition_locator = source_partition_locator \
            if is_src_partition_file_np \
            else dest_partition_locator
        delta_locator = DeltaLocator.of(
            src_file_partition_locator,
            src_stream_position_np.item(),
        )
        dl_digest = delta_locator.digest()
        manifest = manifest_cache.setdefault(
            dl_digest,
            deltacat_storage.get_delta_manifest(delta_locator),
        )
        pa_table = deltacat_storage.download_delta_manifest_entry(
            Delta.of(delta_locator, None, None, None, manifest),
            src_file_idx_np.item(),
        )
        mask_pylist = list(repeat(False, len(pa_table)))
        record_numbers = chain.from_iterable(record_numbers_tpl)
        for record_number in record_numbers:
            mask_pylist[record_number] = True
        mask = pa.array(mask_pylist)
        compacted_table = pa_table.filter(mask)

        # appending, sorting, taking, and dropping has 2-3X latency of a
        # single filter on average, and thus provides better average
        # performance than repeatedly filtering the table in dedupe task index
        # order
        dedupe_task_indices = chain.from_iterable(dedupe_task_idx_iter_tpl)
        compacted_table = sc.append_dedupe_task_idx_col(
            compacted_table,
            dedupe_task_indices,
        )
        pa_sort_keys = [(sc._DEDUPE_TASK_IDX_COLUMN_NAME, "ascending")]
        compacted_table = compacted_table.take(
            pc.sort_indices(compacted_table, sort_keys=pa_sort_keys),
        )
        compacted_table = compacted_table.drop(
            [sc._DEDUPE_TASK_IDX_COLUMN_NAME]
        )
        compacted_tables.append(compacted_table)

    # TODO (pdames): save memory by writing parquet files eagerly whenever
    #  len(compacted_table) >= max_records_per_output_file
    compacted_table = pa.concat_tables(compacted_tables)
    delta = deltacat_storage.stage_delta(
        compacted_table,
        partition,
        max_records_per_entry=max_records_per_output_file,
        content_type=compacted_file_content_type,
    )
    manifest = delta.manifest
    manifest_records = manifest.meta.record_count
    assert(manifest_records == len(compacted_table),
           f"Unexpected Error: Materialized delta manifest record count "
           f"({manifest_records}) does not equal compacted table record count "
           f"({len(compacted_table)})")
    materialize_result = MaterializeResult.of(
        delta,
        mat_bucket_index,
        PyArrowWriteResult.of(
            len(manifest.entries),
            compacted_table.nbytes,
            manifest.meta.content_length,
            len(compacted_table),
        ),
    )
    logger.info(f"Materialize result: {materialize_result}")
    logger.info(f"Finished materialize task...")
    return materialize_result
