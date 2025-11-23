import pyarrow as pa
import pyarrow.compute as pc
import time
import ray
import deltacat.compute.converter.utils.iceberg_columns as sc
from deltacat.compute.converter.utils.io import (
    download_data_table_and_append_iceberg_columns,
    write_sliced_table,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    sort_data_files_maintaining_order,
)
from deltacat.utils.resources import get_current_process_peak_memory_usage_in_bytes
import logging
from deltacat import logs
from typing import List, Dict, Tuple, Optional, Any
from pyiceberg.manifest import DataFile

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def write_position_delete_files(
    position_delete_table: pa.Table,
    iceberg_table_warehouse_prefix_with_partition: str,
    filesystem,
) -> List[str]:
    """
    Write position delete table to files and return file paths.

    Args:
        position_delete_table: PyArrow table containing position delete records
        iceberg_table_warehouse_prefix_with_partition: Base path for writing files
        filesystem: Filesystem implementation

    Returns:
        List of position delete file paths
    """
    if len(position_delete_table) == 0:
        logger.info("No position delete records to write, returning empty file list")
        return []

    # Sort by file_path before writing to ensure deterministic output
    logger.info(
        f"Sorting {len(position_delete_table)} position delete records by file_path"
    )
    position_delete_table_sorted = position_delete_table.sort_by(
        [(sc._FILE_PATH_COLUMN_NAME, "ascending")]
    )

    # Write position delete table to files using write_sliced_table
    logger.info(
        f"Writing {len(position_delete_table_sorted)} position delete records to files"
    )

    memory_used_before_write = get_current_process_peak_memory_usage_in_bytes()
    start_write = time.perf_counter()

    position_delete_files = write_sliced_table(
        table=position_delete_table_sorted,
        base_path=iceberg_table_warehouse_prefix_with_partition,
        table_writer_kwargs={},
        filesystem=filesystem,
    )

    end_write = time.perf_counter()
    memory_used_after_write = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"DEBUG_PERFORMANCE_write_sliced_table: time={end_write - start_write:.4f}s"
    )
    logger.info(
        f"DEBUG_MEMORY_write_sliced_table: before={memory_used_before_write}, "
        f"after={memory_used_after_write}, used={memory_used_after_write - memory_used_before_write}"
    )

    logger.info(
        f"Successfully wrote {len(position_delete_files)} position delete files"
    )

    return position_delete_files


def dedupe_data_files(
    data_file_to_dedupe: List[Tuple[int, DataFile]],
    identifier_columns: List[str],
    remaining_data_table_after_convert: Optional[pa.Table],
    merge_sort_column: str,
    s3_client_kwargs: Optional[Dict[str, Any]],
    iceberg_table_warehouse_prefix_with_partition: str,
    filesystem,
) -> Tuple[List[str], int, int]:
    data_file_table = []
    if remaining_data_table_after_convert:
        data_file_table.append(remaining_data_table_after_convert)

    data_file_to_dedupe = sort_data_files_maintaining_order(
        data_files=data_file_to_dedupe
    )
    downloaded_data_file_record_count = 0
    for file_tuple in data_file_to_dedupe:
        data_file = file_tuple[1]
        data_file_to_dedupe_table = download_data_table_and_append_iceberg_columns(
            file=data_file,
            columns_to_download=identifier_columns,
            additional_columns_to_append=[
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
            s3_client_kwargs=s3_client_kwargs,
        )
        logger.info(
            f"Length of downloaded data file table: {len(data_file_to_dedupe_table)}"
        )
        downloaded_data_file_record_count += len(data_file_to_dedupe_table)
        data_file_table.append(data_file_to_dedupe_table)

    # Monitor performance and memory for concat_tables operation
    memory_used_before_concat = get_current_process_peak_memory_usage_in_bytes()
    start_concat = time.perf_counter()
    final_data_to_dedupe = pa.concat_tables(data_file_table)
    end_concat = time.perf_counter()
    memory_used_after_concat = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"DEBUG_PERFORMANCE_concat_tables: time={end_concat - start_concat:.4f}s"
    )
    logger.info(
        f"DEBUG_MEMORY_concat_tables: before={memory_used_before_concat}, "
        f"after={memory_used_after_concat}, used={memory_used_after_concat - memory_used_before_concat}"
    )

    dedupe_input_record_count = downloaded_data_file_record_count
    if remaining_data_table_after_convert:
        dedupe_input_record_count += len(remaining_data_table_after_convert)
    assert len(final_data_to_dedupe) == dedupe_input_record_count, (
        f"Mismatch record count while performing table concat, Got {len(final_data_to_dedupe)} in final table, "
        f"while input table length is: {dedupe_input_record_count}"
    )

    logger.info(f"Length of pyarrow table to dedupe:{len(final_data_to_dedupe)}")

    record_idx_iterator = iter(range(len(final_data_to_dedupe)))

    # Monitor performance and memory for append_global_record_idx_column operation
    memory_used_before_append = get_current_process_peak_memory_usage_in_bytes()
    start_append = time.perf_counter()
    final_data_to_dedupe = sc.append_global_record_idx_column(
        final_data_to_dedupe, record_idx_iterator
    )
    end_append = time.perf_counter()
    memory_used_after_append = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"DEBUG_PERFORMANCE_append_global_record_idx: time={end_append - start_append:.4f}s"
    )
    logger.info(
        f"DEBUG_MEMORY_append_global_record_idx: before={memory_used_before_append}, "
        f"after={memory_used_after_append}, used={memory_used_after_append - memory_used_before_append}"
    )

    # Monitor performance and memory for group_by/aggregate operation
    memory_used_before_groupby = get_current_process_peak_memory_usage_in_bytes()
    start_groupby = time.perf_counter()
    final_data_table_indices = final_data_to_dedupe.group_by(
        sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, use_threads=False
    ).aggregate([(sc._GLOBAL_RECORD_IDX_COLUMN_NAME, "max")])
    end_groupby = time.perf_counter()
    memory_used_after_groupby = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"DEBUG_PERFORMANCE_group_by_aggregate: time={end_groupby - start_groupby:.4f}s"
    )
    logger.info(
        f"DEBUG_MEMORY_group_by_aggregate: before={memory_used_before_groupby}, "
        f"after={memory_used_after_groupby}, used={memory_used_after_groupby - memory_used_before_groupby}"
    )

    pos_delete_indices = pc.invert(
        pc.is_in(
            final_data_to_dedupe[sc._GLOBAL_RECORD_IDX_COLUMN_NAME],
            value_set=final_data_table_indices[
                f"{sc._GLOBAL_RECORD_IDX_COLUMN_NAME}_max"
            ],
        )
    )

    # Monitor performance and memory for filter operation
    memory_used_before_filter = get_current_process_peak_memory_usage_in_bytes()
    start_filter = time.perf_counter()
    final_data_table_to_delete = final_data_to_dedupe.filter(pos_delete_indices)
    end_filter = time.perf_counter()
    memory_used_after_filter = get_current_process_peak_memory_usage_in_bytes()
    logger.info(f"DEBUG_PERFORMANCE_filter: time={end_filter - start_filter:.4f}s")
    logger.info(
        f"DEBUG_MEMORY_filter: before={memory_used_before_filter}, "
        f"after={memory_used_after_filter}, used={memory_used_after_filter - memory_used_before_filter}"
    )

    final_data_table_to_delete = final_data_table_to_delete.drop(
        [sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, sc._GLOBAL_RECORD_IDX_COLUMN_NAME]
    )

    logger.info(
        f"Deduped {len(final_data_table_to_delete)} Records based off identifier columns."
    )

    # Write position delete files and return file paths instead of table
    position_delete_files = write_position_delete_files(
        position_delete_table=final_data_table_to_delete,
        iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
        filesystem=filesystem,
    )

    return (
        position_delete_files,
        len(final_data_to_dedupe),
        int(final_data_to_dedupe.nbytes),
    )


@ray.remote
def dedupe_sub_hash_bucket(
    sub_hash_bucket_num_to_object_ref: Dict[int, List[str]],
    sub_bucket_index_number: int,
    iceberg_table_warehouse_prefix_with_partition: str,
    filesystem,
    parent_convert_task_index: int = None,
    hash_bucket_number: int = None,
) -> Tuple[List[str], int, int]:
    """
    Deduplicate records within a specific hash bucket using object references.

    Args:
        sub_hash_bucket_num_to_object_ref: Dict mapping bucket_number -> list of object_ref_ids
        sub_bucket_index_number: The specific bucket number to process
        parent_convert_task_index: Parent convert task index for logging context
        hash_bucket_number: Hash bucket number for logging context

    Returns:
        Tuple of (position_delete_files, position_delete_record_count, total_memory_size)
    """
    # Create logging prefix with parent context
    log_prefix = (
        f"[Convert task {parent_convert_task_index}][Hash bucket {hash_bucket_number}]"
        if parent_convert_task_index is not None
        else f"[Hash bucket {sub_bucket_index_number}]"
    )

    logger.info(
        f"{log_prefix}: Processing hash bucket {sub_bucket_index_number} for deduplication"
    )

    # Get object references for this specific bucket
    if sub_bucket_index_number not in sub_hash_bucket_num_to_object_ref:
        logger.info(
            f"{log_prefix}: Hash bucket {sub_bucket_index_number} is empty, no records to dedupe"
        )
        # Return empty file list
        return [], 0, 0

    object_ref_ids = sub_hash_bucket_num_to_object_ref[sub_bucket_index_number]
    logger.info(
        f"{log_prefix}: Hash bucket {sub_bucket_index_number} has {len(object_ref_ids)} object references"
    )

    # Convert object reference ID strings back to ObjectRef objects
    from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore

    object_store = RayPlasmaObjectStore()

    # Retrieve tables from Ray object store using ray.get()
    memory_used_before_get = get_current_process_peak_memory_usage_in_bytes()
    start_get = time.perf_counter()

    # Convert string IDs to ObjectRef objects, then get the actual data
    bucket_tables = []
    for object_ref_id in object_ref_ids:
        table = object_store.get(object_ref_id)
        bucket_tables.append(table)

    end_get = time.perf_counter()
    memory_used_after_get = get_current_process_peak_memory_usage_in_bytes()
    logger.info(f"DEBUG_PERFORMANCE_ray_get: time={end_get - start_get:.4f}s")
    logger.info(
        f"DEBUG_MEMORY_ray_get: before={memory_used_before_get}, "
        f"after={memory_used_after_get}, used={memory_used_after_get - memory_used_before_get}"
    )

    # Filter out None or empty tables
    valid_tables = [
        table for table in bucket_tables if table is not None and len(table) > 0
    ]

    if not valid_tables:
        logger.info(
            f"{log_prefix}: Hash bucket {sub_bucket_index_number} has no valid records after ray.get()"
        )
        # Return empty file list
        return [], 0, 0

    # Monitor performance and memory for concat_tables operation
    memory_used_before_concat = get_current_process_peak_memory_usage_in_bytes()
    start_concat = time.perf_counter()
    final_data_to_dedupe = pa.concat_tables(valid_tables)
    end_concat = time.perf_counter()
    memory_used_after_concat = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"{log_prefix}: DEBUG_PERFORMANCE_concat_tables: time={end_concat - start_concat:.4f}s"
    )
    logger.info(
        f"{log_prefix}: DEBUG_MEMORY_concat_tables: before={memory_used_before_concat}, "
        f"after={memory_used_after_concat}, used={memory_used_after_concat - memory_used_before_concat}"
    )

    logger.info(
        f"{log_prefix}: Length of pyarrow table to dedupe: {len(final_data_to_dedupe)}"
    )

    record_idx_iterator = iter(range(len(final_data_to_dedupe)))

    # Monitor performance and memory for append_global_record_idx_column operation
    memory_used_before_append = get_current_process_peak_memory_usage_in_bytes()
    start_append = time.perf_counter()
    final_data_to_dedupe = sc.append_global_record_idx_column(
        final_data_to_dedupe, record_idx_iterator
    )
    end_append = time.perf_counter()
    memory_used_after_append = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"DEBUG_PERFORMANCE_append_global_record_idx: time={end_append - start_append:.4f}s"
    )
    logger.info(
        f"DEBUG_MEMORY_append_global_record_idx: before={memory_used_before_append}, "
        f"after={memory_used_after_append}, used={memory_used_after_append - memory_used_before_append}"
    )

    # Monitor performance and memory for group_by/aggregate operation
    memory_used_before_groupby = get_current_process_peak_memory_usage_in_bytes()
    start_groupby = time.perf_counter()
    final_data_table_indices = final_data_to_dedupe.group_by(
        sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME, use_threads=False
    ).aggregate([(sc._GLOBAL_RECORD_IDX_COLUMN_NAME, "max")])
    end_groupby = time.perf_counter()
    memory_used_after_groupby = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"DEBUG_PERFORMANCE_group_by_aggregate: time={end_groupby - start_groupby:.4f}s"
    )
    logger.info(
        f"DEBUG_MEMORY_group_by_aggregate: before={memory_used_before_groupby}, "
        f"after={memory_used_after_groupby}, used={memory_used_after_groupby - memory_used_before_groupby}"
    )

    pos_delete_indices = pc.invert(
        pc.is_in(
            final_data_to_dedupe[sc._GLOBAL_RECORD_IDX_COLUMN_NAME],
            value_set=final_data_table_indices[
                f"{sc._GLOBAL_RECORD_IDX_COLUMN_NAME}_max"
            ],
        )
    )

    # Monitor performance and memory for filter operation
    memory_used_before_filter = get_current_process_peak_memory_usage_in_bytes()
    start_filter = time.perf_counter()
    final_data_table_to_delete = final_data_to_dedupe.filter(pos_delete_indices)
    end_filter = time.perf_counter()
    memory_used_after_filter = get_current_process_peak_memory_usage_in_bytes()
    logger.info(f"DEBUG_PERFORMANCE_filter: time={end_filter - start_filter:.4f}s")
    logger.info(
        f"DEBUG_MEMORY_filter: before={memory_used_before_filter}, "
        f"after={memory_used_after_filter}, used={memory_used_after_filter - memory_used_before_filter}"
    )

    final_data_table_to_delete = final_data_table_to_delete.drop(
        [
            sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME,
            sc._GLOBAL_RECORD_IDX_COLUMN_NAME,
            sc._HASH_BUCKET_INDEX_COLUMN_NAME,
        ]
    )

    logger.info(
        f"Hash bucket {sub_bucket_index_number} - Deduped {len(final_data_table_to_delete)} Records based off identifier columns."
    )

    # Store position delete record count for correct aggregation
    position_delete_record_count = len(final_data_table_to_delete)

    # Return empty file list if no records to delete
    if position_delete_record_count == 0:
        logger.info(
            f"Hash bucket {sub_bucket_index_number} - No position delete records, returning empty file list"
        )
        return (
            [],
            0,  # Return 0 position delete record count
            int(final_data_to_dedupe.nbytes),
        )

    # Sort by file_path before writing to ensure deterministic output
    logger.info(
        f"Hash bucket {sub_bucket_index_number} - Sorting position delete table by file_path"
    )
    final_data_table_to_delete_sorted = final_data_table_to_delete.sort_by(
        [(sc._FILE_PATH_COLUMN_NAME, "ascending")]
    )

    # Write position delete table to files using write_sliced_table
    logger.info(
        f"Hash bucket {sub_bucket_index_number} - Writing {len(final_data_table_to_delete_sorted)} position delete records to files"
    )

    memory_used_before_write = get_current_process_peak_memory_usage_in_bytes()
    start_write = time.perf_counter()

    position_delete_files = write_sliced_table(
        table=final_data_table_to_delete_sorted,
        base_path=iceberg_table_warehouse_prefix_with_partition,
        table_writer_kwargs={},
        filesystem=filesystem,
    )

    end_write = time.perf_counter()
    memory_used_after_write = get_current_process_peak_memory_usage_in_bytes()
    logger.info(
        f"DEBUG_PERFORMANCE_write_sliced_table: time={end_write - start_write:.4f}s"
    )
    logger.info(
        f"DEBUG_MEMORY_write_sliced_table: before={memory_used_before_write}, "
        f"after={memory_used_after_write}, used={memory_used_after_write - memory_used_before_write}"
    )

    logger.info(
        f"Hash bucket {sub_bucket_index_number} - Successfully wrote {len(position_delete_files)} position delete files"
    )

    return (
        position_delete_files,
        position_delete_record_count,  # Return actual position delete count instead of total input count
        int(final_data_to_dedupe.nbytes),
    )
