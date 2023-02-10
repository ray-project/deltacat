import logging
import ray
import pyarrow as pa

from collections import defaultdict
from itertools import chain, repeat
from typing import List, Optional, Tuple
import numpy as np

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


@ray.remote(num_returns=2)
def materialize(
        source_partition_locator: PartitionLocator,
        round_completion_info: Optional[RoundCompletionInfo],
        partition: Partition,
        mat_bucket_index: int,
        src_dfl_to_hb_idx_record_indices: List[DedupeTaskIndexWithObjectId],
        max_records_per_output_file: int,
        compacted_file_content_type: ContentType,
        num_hash_groups: int,
        schema: Optional[pa.Schema] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> MaterializeResult:

    def _materialize(
            compacted_tables: List[pa.Table],
            compacted_tables_record_count: int) -> MaterializeResult:
        compacted_tables_size = sum([TABLE_CLASS_TO_SIZE_FUNC[type(tbl)](tbl)
                                     for tbl in compacted_tables])
        logger.debug(f"Uploading {len(compacted_tables)} compacted tables "
                     f"with size: {compacted_tables_size} bytes "
                     f"and record count: {compacted_tables_record_count}")
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

        # TODO(raghumdani): reference the same file URIs while writing the files
        # instead of copying the data over and creating new files. 
        delta = deltacat_storage.stage_delta(
            compacted_table,
            partition,
            max_records_per_entry=max_records_per_output_file,
            content_type=compacted_file_content_type,
        )
        manifest = delta.manifest
        manifest_records = manifest.meta.record_count

        # FIXME: what if single table writes multiple files?
        assert len(delta.manifest.entries) == 1, "This method should have written only one entry"
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
    # this depends on `ray.get` result order matching input order, as per the
    # contract established in: https://github.com/ray-project/ray/pull/16763
    src_file_records_list = ray.get([cloudpickle.loads(t) for t in src_dfl_to_hb_idx_record_indices])
    logger.info(f"Resolved materialize task obj refs...")

    all_src_file_records = defaultdict(list)
    for src_file_records in src_file_records_list:
        for src_dfl, record_numbers_hb_idx_table in src_file_records.items():
            all_src_file_records[src_dfl].append(record_numbers_hb_idx_table)

    manifest_cache = {}
    compacted_tables = []
    dest_file_idx = 0
    pk_hash_tables = []
    result_pk_hash_tables = []
    materialized_results: List[MaterializeResult] = []
    total_record_count = 0

    logger.info(f"[Materialize task index: {mat_bucket_index}] Found {len(src_dfl)} source delta file locators...")

    for src_dfl in all_src_file_records.keys():
        record_numbers_pk_tables: List[DeltaFileLocatorToRecords] = \
            all_src_file_records[src_dfl]

        logger.info(f"[Materialize task index: {mat_bucket_index}] Concatenating {len(record_numbers_pk_tables)} tables for {src_dfl}...")

        record_numbers_pk_table = pa.concat_tables(record_numbers_pk_tables)

        logger.info(f"[Materialize task index: {mat_bucket_index}] Concatenated total of {len(record_numbers_pk_tables)} tables"
                    f" with {len(record_numbers_pk_table)} records.")

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
        logger.info(f"Taking the record indices from {len(pa_table)} represented by ordered record idx column...")
        
        mask_pylist = list(repeat(False, len(pa_table)))
        ordered_record_idx_np = sc.record_index_column_np(record_numbers_pk_table)
        for record_idx_id in range(len(ordered_record_idx_np)):
            mask_pylist[ordered_record_idx_np[record_idx_id]] = True
        mask = pa.array(mask_pylist)
        pa_table = pa_table.filter(mask)
        record_count = len(pa_table)

        assert record_count == len(record_numbers_pk_table), f"Selected records must match the record numbers for {delta_locator}"

        logger.info(f"[Mat bucket index {mat_bucket_index}] Took total records in pa_table: {record_count}")

        if compacted_tables and total_record_count + record_count > max_records_per_output_file:
            materialized_results.append(_materialize(compacted_tables, total_record_count))
            logger.info(f"[Mat bucket index {mat_bucket_index}] preparing the pk_table...")
            pk_table = pa.concat_tables(pk_hash_tables)
            pk_table = pk_table.drop([sc._ORDERED_RECORD_IDX_COLUMN_NAME])
            pk_table = sc.append_record_idx_col(pk_table, range(0, total_record_count))
            pk_table = sc.append_file_idx_column(pk_table, repeat(dest_file_idx, total_record_count))

            pk_table = sc.append_mat_bucket_idx_column(pk_table, repeat(mat_bucket_index, total_record_count))
            # By this time table would have hash, record_index, hash_bucket_idx, mat_bucket_index, file_index columns
            logger.info(f"[Mat bucket index {mat_bucket_index}] Pk table with {len(pk_table)} prepared.")

            result_pk_hash_tables.append(pk_table)

            # Free up written tables in memory
            compacted_tables.clear()
            pk_hash_tables.clear()
            total_record_count = 0
            dest_file_idx += 1

        total_record_count += record_count
        compacted_tables.append(pa_table)
        pk_hash_tables.append(record_numbers_pk_table)

    if compacted_tables:
        logger.info(f"[Mat bucket index {mat_bucket_index}] preparing the pk_table...")

        materialized_results.append(_materialize(compacted_tables, total_record_count))
        pk_table = pa.concat_tables(pk_hash_tables)
        pk_table = pk_table.drop([sc._ORDERED_RECORD_IDX_COLUMN_NAME])
        pk_table = sc.append_record_idx_col(pk_table, range(0, total_record_count))
        pk_table = sc.append_file_idx_column(pk_table, repeat(dest_file_idx, total_record_count))
        pk_table = sc.append_mat_bucket_idx_column(pk_table, repeat(mat_bucket_index, total_record_count))
        result_pk_hash_tables.append(pk_table)

        logger.info(f"[Mat bucket index {mat_bucket_index}] Pk table with {len(pk_table)} prepared.")
        # Free up written tables in memory
        compacted_tables.clear()
        pk_hash_tables.clear()
        total_record_count = 0
        dest_file_idx += 1

    merged_delta = Delta.merge_deltas([mr.delta for mr in materialized_results])
    assert materialized_results and len(materialized_results) > 0, \
        f"Expected at least one materialized result in materialize step."

    assert len(merged_delta.manifest.entries) == dest_file_idx, "total files should match"


    # Merge all new deltas into one for this materialize bucket index
    merged_materialize_result = MaterializeResult.of(merged_delta,
                                                     materialized_results[0].task_index,
                                                     PyArrowWriteResult.union([mr.pyarrow_write_result
                                                                               for mr in materialized_results]))

    logger.info(f"[Mat bucket index {mat_bucket_index}] Finished materialize task...")

    # TODO(raghumdani): Optimize this grouping
    hb_pk_table = pa.concat_tables(result_pk_hash_tables)
    logger.info(f"[Mat bucket index {mat_bucket_index}] Concatenated {len(result_pk_hash_tables)} with total records {len(hb_pk_table)}.")

    assert merged_materialize_result.pyarrow_write_result.records == len(hb_pk_table), \
            "total records written during this task should match"

    logger.info(f"[Mat bucket index {mat_bucket_index}] Converting to pandas {len(hb_pk_table)}.")

    hb_pk_table = hb_pk_table.sort_by(sc._HASH_BUCKET_IDX_COLUMN_NAME)

    hb_pk_grouped_by = hb_pk_table.group_by(sc._HASH_BUCKET_IDX_COLUMN_NAME) \
                                    .aggregate([(sc._HASH_BUCKET_IDX_COLUMN_NAME, "count")])

    # hb_group_to_pk_hash_tables_ref = defaultdict(list)
    # for hb_idx, pk_table_pandas in hb_pk_grouped_by:
    #     hb_group = hb_idx % num_hash_groups
    #     pyarrow_table = pa.table(pk_table_pandas)
    #     object_ref = ray.put((hb_idx, pyarrow_table))
    #     pickled_object_ref = cloudpickle.dumps(object_ref)
    #     hb_group_to_pk_hash_tables_ref[hb_group].append(pickled_object_ref)

    #     # free memory
    #     del object_ref
    #     del pk_table_pandas
    #     del pickled_object_ref

    

    hb_group_to_pk_hash_tables_ref = defaultdict(list)
    group_count_array = hb_pk_grouped_by[f"{sc._HASH_BUCKET_IDX_COLUMN_NAME}_count"]
    hb_group_array = hb_pk_grouped_by[sc._HASH_BUCKET_IDX_COLUMN_NAME]

    prev_count = 0
    for i, group_count in enumerate(group_count_array):
        hb_idx = hb_group_array[i].as_py()
        pyarrow_table = hb_pk_table.slice(offset=prev_count, length=group_count.as_py())
        pyarrow_table = pyarrow_table.drop([sc._HASH_BUCKET_IDX_COLUMN_NAME])
        hb_group = hb_idx % num_hash_groups
        object_ref = ray.put((hb_idx, pyarrow_table))
        pickled_object_ref = cloudpickle.dumps(object_ref)
        hb_group_to_pk_hash_tables_ref[hb_group].append(pickled_object_ref)

        # free memory
        del object_ref
        del pickled_object_ref
        prev_count = group_count.as_py()

    logger.info(f"[Mat bucket index {mat_bucket_index}] Finished to pandas {len(hb_pk_table)}.")

    return merged_materialize_result, \
        hb_group_to_pk_hash_tables_ref