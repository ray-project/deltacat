import pyarrow.compute as pc

import deltacat.compute.converter.utils.iceberg_columns as sc
import pyarrow as pa

from collections import defaultdict
import ray
import logging
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.steps.dedupe import (
    dedupe_data_files,
    dedupe_sub_hash_bucket,
)
from deltacat.compute.converter.steps.hash_bucket import hash_bucket
from deltacat.compute.converter.utils.io import write_sliced_table
from deltacat.compute.converter.utils.io import (
    download_data_table_and_append_iceberg_columns,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    partition_value_record_to_partition_value_string,
    sort_data_files_maintaining_order,
)
from deltacat.compute.converter.pyiceberg.overrides import (
    parquet_files_dict_to_iceberg_data_files,
)
from deltacat.compute.converter.model.convert_result import ConvertResult
from pyiceberg.manifest import DataFileContent
from deltacat import logs
from fsspec import AbstractFileSystem
from typing import List, Dict, Tuple, Optional, Any
from deltacat.utils.resources import get_current_process_peak_memory_usage_in_bytes
from deltacat.compute.converter.model.convert_input_files import (
    DataFileList,
    DataFileListGroup,
)
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
)
import time

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def convert(convert_input: ConvertInput) -> ConvertResult:
    convert_input_files = convert_input.convert_input_files
    convert_task_index = convert_input.convert_task_index
    iceberg_table_warehouse_prefix = convert_input.iceberg_table_warehouse_prefix
    identifier_fields = convert_input.identifier_fields
    table_io = convert_input.table_io
    table_metadata = convert_input.table_metadata
    compact_previous_position_delete_files = (
        convert_input.compact_previous_position_delete_files
    )
    position_delete_for_multiple_data_files = (
        convert_input.position_delete_for_multiple_data_files
    )
    max_parallel_data_file_download = convert_input.max_parallel_data_file_download
    filesystem = convert_input.filesystem
    s3_client_kwargs = convert_input.s3_client_kwargs
    task_memory = convert_input.task_memory

    if not position_delete_for_multiple_data_files:
        raise NotImplementedError(
            f"Distributed file level position delete compute is not supported yet"
        )
    if compact_previous_position_delete_files:
        raise NotImplementedError(f"Compact previous position delete not supported yet")

    logger.info(f"Starting convert task index: {convert_task_index}")

    applicable_data_files = convert_input_files.applicable_data_files
    applicable_equality_delete_files = (
        convert_input_files.applicable_equality_delete_files
    )
    applicable_position_delete_files = (
        convert_input_files.existing_position_delete_files
    )

    all_data_files_for_this_bucket = convert_input_files.all_data_files_for_dedupe

    partition_value_str = partition_value_record_to_partition_value_string(
        convert_input_files.partition_value, table_metadata
    )
    partition_value = convert_input_files.partition_value

    if partition_value_str:
        iceberg_table_warehouse_prefix_with_partition = (
            f"{iceberg_table_warehouse_prefix}/{partition_value_str}"
        )
    else:
        iceberg_table_warehouse_prefix_with_partition = (
            f"{iceberg_table_warehouse_prefix}"
        )
    logger.info(
        f"iceberg_table_warehouse_prefix_with_partition:{iceberg_table_warehouse_prefix_with_partition}"
    )
    enforce_primary_key_uniqueness = convert_input.enforce_primary_key_uniqueness
    total_pos_delete_table = []
    data_table_after_converting_equality_delete = []
    all_position_delete_files = []  # Initialize position delete files list
    data_file_to_dedupe_record_count = 0
    data_file_to_dedupe_size = 0
    dedupe_position_delete_record_count = 0  # Track position delete count from dedupe
    if applicable_equality_delete_files:
        (
            pos_delete_after_converting_equality_delete,
            data_table_after_converting_equality_delete,
        ) = compute_pos_delete_with_limited_parallelism(
            data_files_list=applicable_data_files,
            identifier_columns=identifier_fields,
            equality_delete_files_list=applicable_equality_delete_files,
            iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
            convert_task_index=convert_task_index,
            max_parallel_data_file_download=max_parallel_data_file_download,
            s3_file_system=filesystem,
            s3_client_kwargs=s3_client_kwargs,
        )
        if pos_delete_after_converting_equality_delete:
            total_pos_delete_table.append(pos_delete_after_converting_equality_delete)

    if enforce_primary_key_uniqueness:
        data_files_downloaded_during_convert = []
        if applicable_data_files:
            for file_list in applicable_data_files:
                for file in file_list:
                    data_files_downloaded_during_convert.append(file)

        data_files_to_dedupe = get_additional_applicable_data_files(
            all_data_files=all_data_files_for_this_bucket,
            data_files_downloaded=data_files_downloaded_during_convert,
        )

        dedupe_file_size_bytes = sum(
            data_file.file_size_in_bytes for _, data_file in data_files_to_dedupe
        )
        logger.info(
            f"Total on-disk size of files to dedupe: {dedupe_file_size_bytes} bytes"
        )

        logger.info(
            f"[Convert task {convert_task_index}]: Got {len(data_files_to_dedupe)} files to dedupe."
        )

        # Check if sub-bucketing is enabled
        if convert_input_files.sub_bucket_enabled:
            logger.info(
                f"[Convert task {convert_task_index}]: Sub-bucketing enabled, launching hash_bucket tasks"
            )

            (
                sub_bucket_index_to_object_ids,
                sub_bucket_index_to_record_counts,
            ) = process_sub_buckets_with_hash_bucket_tasks(
                convert_input_files=convert_input_files,
                identifier_fields=identifier_fields,
                s3_client_kwargs=s3_client_kwargs,
                convert_task_index=convert_task_index,
            )

            # Step 4: Invoke dedupe_sub_hash_bucket remote tasks for each hash bucket
            logger.info(
                f"[Convert task {convert_task_index}]: Launching dedupe_sub_hash_bucket tasks for {len(sub_bucket_index_to_object_ids)} hash buckets"
            )

            # Get list of bucket numbers to process
            bucket_numbers = list(sub_bucket_index_to_object_ids.keys())

            # Create options provider for dedupe tasks with proper memory estimation based on actual record counts
            def dedupe_options_provider(
                index: int, bucket_number: int
            ) -> Dict[str, Any]:
                # Get actual record count for this specific hash bucket
                record_count = sub_bucket_index_to_record_counts.get(bucket_number, 0)

                # Use the centralized memory estimation function
                from deltacat.compute.converter.utils.convert_task_options import (
                    dedupe_hash_bucket_memory_options_provider,
                )

                return dedupe_hash_bucket_memory_options_provider(
                    bucket_number=bucket_number,
                    record_count=record_count,
                    identifier_fields=identifier_fields,
                )

            # Create kwargs provider for dedupe task inputs
            def dedupe_kwargs_provider(
                index: int, bucket_number: int
            ) -> Dict[str, Any]:
                logger.info(
                    f"[Convert task {convert_task_index}]: Launching dedupe task for hash bucket {bucket_number}"
                )

                return {
                    "sub_hash_bucket_num_to_object_ref": sub_bucket_index_to_object_ids,
                    "sub_bucket_index_number": bucket_number,
                    "iceberg_table_warehouse_prefix_with_partition": iceberg_table_warehouse_prefix_with_partition,
                    "filesystem": filesystem,
                    "parent_convert_task_index": convert_task_index,  # Add parent context
                    "hash_bucket_number": bucket_number,  # Add bucket number for logging
                }

            # Use invoke_parallel for dedupe_sub_hash_bucket tasks
            logger.info(
                f"[Convert task {convert_task_index}]: Invoking {len(bucket_numbers)} dedupe_sub_hash_bucket tasks"
            )

            dedupe_tasks_pending = invoke_parallel(
                items=bucket_numbers,
                ray_task=dedupe_sub_hash_bucket,
                max_parallelism=len(
                    bucket_numbers
                ),  # Process all hash buckets in parallel
                options_provider=dedupe_options_provider,
                kwargs_provider=dedupe_kwargs_provider,
            )

            # Wait for all dedupe tasks to complete and collect file results
            logger.info(
                f"[Convert task {convert_task_index}]: Waiting for {len(dedupe_tasks_pending)} dedupe tasks"
            )
            dedupe_results = ray.get(dedupe_tasks_pending)

            # Collect position delete files from all hash buckets
            all_position_delete_files = []
            total_record_count = 0
            total_memory_size = 0

            for (
                position_delete_files,
                position_delete_count,
                memory_size,
            ) in dedupe_results:
                if position_delete_files:  # Only add non-empty file lists
                    all_position_delete_files.extend(position_delete_files)
                # Now position_delete_count is the actual number of position delete records
                dedupe_position_delete_record_count += position_delete_count
                total_memory_size += memory_size

            logger.info(
                f"[Convert task {convert_task_index}]: Collected {len(all_position_delete_files)} position delete files "
                f"with {dedupe_position_delete_record_count} total position delete records "
                f"from {len(dedupe_tasks_pending)} hash bucket tasks"
            )

            # Store the file list for later processing
            # Note: files are already written by dedupe_sub_hash_bucket tasks
            convert_input_files.position_delete_files_from_sub_buckets = (
                all_position_delete_files
            )

            # Calculate stats for sub-bucket processing
            data_file_to_dedupe_record_count = total_record_count
            data_file_to_dedupe_size = total_memory_size

        else:
            # Normal dedupe processing
            (
                pos_delete_file_paths,
                data_file_to_dedupe_record_count,
                data_file_to_dedupe_size,
            ) = dedupe_data_files(
                data_file_to_dedupe=data_files_to_dedupe,
                identifier_columns=identifier_fields,
                remaining_data_table_after_convert=data_table_after_converting_equality_delete,
                merge_sort_column=sc._ORDERED_RECORD_IDX_COLUMN_NAME,
                s3_client_kwargs=s3_client_kwargs,
                iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
                filesystem=filesystem,
            )

            # Store file paths for consistent handling with sub-bucketing path
            all_position_delete_files = pos_delete_file_paths
            logger.info(
                f"[Convert task {convert_task_index}]: Dedupe produced {len(pos_delete_file_paths)} position delete files."
            )

    # Handle position delete files consistently for both normal dedupe and sub-bucketing paths
    to_be_added_files_list = []

    # Combine position delete files from equality deletes (if any) with dedupe files
    all_parquet_files = []

    # Add position delete files from equality delete processing (if any)
    if total_pos_delete_table:
        # Only concat tables if we have equality delete results
        equality_pos_delete = pa.concat_tables(total_pos_delete_table)

        if len(equality_pos_delete) > 0:
            # Sort and write equality delete results
            memory_used_before_sort = get_current_process_peak_memory_usage_in_bytes()
            start = time.perf_counter()
            equality_pos_delete_sorted = equality_pos_delete.sort_by(
                [(sc._FILE_PATH_COLUMN_NAME, "ascending")]
            )
            end = time.perf_counter()
            memory_used_after_sort = get_current_process_peak_memory_usage_in_bytes()
            logger.info(
                f"DEBUG_memory_usage_convert_task:{convert_task_index}, before_sort:{memory_used_before_sort},"
                f"after_sort:{memory_used_after_sort}, used:{memory_used_after_sort - memory_used_before_sort}"
            )
            logger.info(f"DEBUG_PERFORMANCE_sort_by: time to final sort:{end - start}")

            equality_pos_delete_files = write_sliced_table(
                table=equality_pos_delete_sorted,
                base_path=iceberg_table_warehouse_prefix_with_partition,
                table_writer_kwargs={},
                filesystem=filesystem,
            )
            all_parquet_files.extend(equality_pos_delete_files)

            logger.info(
                f"[Convert task {convert_task_index}]: Equality delete processing produced {len(equality_pos_delete_files)} position delete files."
            )

    # Add position delete files from dedupe processing (both normal and sub-bucketing)
    if all_position_delete_files:
        all_parquet_files.extend(all_position_delete_files)
        logger.info(
            f"[Convert task {convert_task_index}]: Dedupe processing contributed {len(all_position_delete_files)} position delete files."
        )

    # Convert all position delete files to Iceberg format
    if all_parquet_files:
        to_be_added_files_dict = defaultdict()
        to_be_added_files_dict[partition_value] = all_parquet_files

        logger.info(
            f"[Convert task {convert_task_index}]: Total {len(all_parquet_files)} position delete files to be added to table."
        )

        file_content_type = DataFileContent.POSITION_DELETES
        to_be_added_files_list = parquet_files_dict_to_iceberg_data_files(
            io=table_io,
            table_metadata=table_metadata,
            files_dict=to_be_added_files_dict,
            file_content_type=file_content_type,
        )

    to_be_delete_files_dict = defaultdict()

    if applicable_equality_delete_files:
        to_be_delete_files_dict[partition_value] = [
            equality_delete_file[1]
            for equality_delete_list in applicable_equality_delete_files
            for equality_delete_file in equality_delete_list
        ]

    if applicable_position_delete_files:
        to_be_delete_files_dict[partition_value] = [
            position_delete_file_tuple[1]
            for position_delete_file_tuple in applicable_position_delete_files
        ]

    if not enforce_primary_key_uniqueness:
        data_file_to_dedupe_record_count = 0
        data_file_to_dedupe_size = 0

    # Calculate position delete record count and memory usage from files
    total_position_delete_record_count = 0
    total_position_delete_memory_size = 0

    # Add counts from equality delete processing
    if total_pos_delete_table:
        equality_pos_delete = pa.concat_tables(total_pos_delete_table)
        total_position_delete_record_count += len(equality_pos_delete)
        total_position_delete_memory_size += int(equality_pos_delete.nbytes)

    # Add counts from dedupe processing (sub-bucketing path)
    if dedupe_position_delete_record_count > 0:
        total_position_delete_record_count += dedupe_position_delete_record_count
        logger.info(
            f"[Convert task {convert_task_index}]: Added {dedupe_position_delete_record_count} position delete records from dedupe processing"
        )

    peak_memory_usage_bytes = (
        get_current_process_peak_memory_usage_in_bytes()
    )  # Convert KB to bytes
    memory_usage_percentage = (peak_memory_usage_bytes / task_memory) * 100

    logger.info(
        f"[Convert task {convert_task_index}]: Memory usage stats - "
        f"Peak memory usage: {peak_memory_usage_bytes} bytes, "
        f"Allocated task memory: {convert_input.task_memory} bytes, "
        f"Usage percentage: {memory_usage_percentage:.2f}%"
    )

    convert_res = ConvertResult.of(
        convert_task_index=convert_task_index,
        to_be_added_files=to_be_added_files_list,
        to_be_deleted_files=to_be_delete_files_dict,
        position_delete_record_count=total_position_delete_record_count,
        input_data_files_record_count=data_file_to_dedupe_record_count,
        input_data_files_hash_columns_in_memory_sizes=data_file_to_dedupe_size,
        position_delete_in_memory_sizes=total_position_delete_memory_size,
        position_delete_on_disk_sizes=sum(
            file.file_size_in_bytes for file in to_be_added_files_list
        ),
        input_data_files_on_disk_size=dedupe_file_size_bytes,
        peak_memory_usage_bytes=peak_memory_usage_bytes,
        memory_usage_percentage=memory_usage_percentage,
    )
    return convert_res


def get_additional_applicable_data_files(
    all_data_files: DataFileList,
    data_files_downloaded: DataFileList,
) -> DataFileList:
    data_file_to_dedupe = []
    assert len(set(all_data_files)) >= len(set(data_files_downloaded)), (
        f"Length of all data files ({len(set(all_data_files))}) should never be less than "
        f"the length of candidate equality delete data files ({len(set(data_files_downloaded))})"
    )
    if data_files_downloaded:
        # set1.difference(set2) returns elements in set1 but not in set2
        data_file_to_dedupe.extend(
            list(set(all_data_files).difference(set(data_files_downloaded)))
        )
    else:
        data_file_to_dedupe = all_data_files
    return data_file_to_dedupe


def filter_rows_to_be_deleted(
    equality_delete_table: Optional[pa.Table],
    data_file_table: Optional[pa.Table],
    identifier_columns: List[str],
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    identifier_column = sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME
    if equality_delete_table and data_file_table:
        equality_deletes = pc.is_in(
            data_file_table[identifier_column],
            equality_delete_table[identifier_column],
        )
        data_file_record_remaining = pc.invert(
            pc.is_in(
                data_file_table[identifier_column],
                equality_delete_table[identifier_column],
            )
        )
        position_delete_table = data_file_table.filter(equality_deletes)
        remaining_data_table = data_file_table.filter(data_file_record_remaining)

        position_delete_table = position_delete_table.drop(
            [sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME]
        )
        assert len(position_delete_table) + len(remaining_data_table) == len(
            data_file_table
        ), (
            f"Expected undeleted data file record count plus length of pos deletes to match original data file record count of {len(data_file_table)}, "
            f"but found {len(position_delete_table)} pos deletes + {len(remaining_data_table)} equality deletes."
        )
    else:
        # Initialize variables when input tables are None or empty
        position_delete_table = None
        remaining_data_table = None

    return position_delete_table, remaining_data_table


def compute_pos_delete_converting_equality_deletes(
    equality_delete_table: Optional[pa.Table],
    data_file_table: Optional[pa.Table],
    identifier_columns: List[str],
    iceberg_table_warehouse_prefix_with_partition: str,
    s3_file_system: Optional[AbstractFileSystem],
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    new_position_delete_table, remaining_data_table = filter_rows_to_be_deleted(
        data_file_table=data_file_table,
        equality_delete_table=equality_delete_table,
        identifier_columns=identifier_columns,
    )
    if new_position_delete_table:
        logger.info(
            f"Length of position delete table after converting from equality deletes:{len(new_position_delete_table)}"
        )
        return new_position_delete_table, remaining_data_table
    elif not remaining_data_table:
        return None, None
    else:
        return None, remaining_data_table


def compute_pos_delete_with_limited_parallelism(
    data_files_list: DataFileListGroup,
    identifier_columns: List[str],
    equality_delete_files_list: DataFileListGroup,
    iceberg_table_warehouse_prefix_with_partition: str,
    convert_task_index: int,
    max_parallel_data_file_download: int,
    s3_file_system: Optional[AbstractFileSystem],
    s3_client_kwargs: Optional[Dict[str, Any]],
) -> Tuple[Optional[pa.Table], Optional[pa.Table]]:
    assert len(data_files_list) == len(equality_delete_files_list), (
        f"Number of lists of data files should equal to number of list of equality delete files, "
        f"But got {len(data_files_list)} data files lists vs {len(equality_delete_files_list)}."
    )

    new_pos_delete_table_total = []
    for data_files, equality_delete_files in zip(
        data_files_list, equality_delete_files_list
    ):
        data_table_total = []

        # Sort data files by file sequence number first, then file path to
        # make sure files having same sequence number are deterministically sorted
        data_files = sort_data_files_maintaining_order(data_files=data_files)

        for data_file in data_files:
            data_table = download_data_table_and_append_iceberg_columns(
                file=data_file[1],
                columns_to_download=identifier_columns,
                additional_columns_to_append=[
                    sc._FILE_PATH_COLUMN_NAME,
                    sc._ORDERED_RECORD_IDX_COLUMN_NAME,
                ],
                s3_client_kwargs=s3_client_kwargs,
            )
            data_table_total.append(data_table)
        data_table_total = pa.concat_tables(data_table_total)

        equality_delete_table_total = []
        for equality_delete in equality_delete_files:
            equality_delete_table = download_data_table_and_append_iceberg_columns(
                file=equality_delete[1],
                columns_to_download=identifier_columns,
                s3_client_kwargs=s3_client_kwargs,
            )
            equality_delete_table_total.append(equality_delete_table)
        equality_delete_table_total = pa.concat_tables(equality_delete_table_total)

        (
            new_pos_delete_table,
            remaining_data_table,
        ) = compute_pos_delete_converting_equality_deletes(
            equality_delete_table=equality_delete_table_total,
            data_file_table=data_table_total,
            iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
            identifier_columns=identifier_columns,
            s3_file_system=s3_file_system,
        )
        # Only append non-None tables to avoid concat_tables errors
        if new_pos_delete_table is not None:
            new_pos_delete_table_total.append(new_pos_delete_table)

    new_pos_delete_table_total_sorted = None
    # Filter out None values and concatenate only if we have valid tables
    if new_pos_delete_table_total:
        new_pos_delete_table_total_sorted = pa.concat_tables(new_pos_delete_table_total)
    else:
        new_pos_delete_table_total_sorted = None

    pos_delete_count = (
        len(new_pos_delete_table_total_sorted)
        if new_pos_delete_table_total_sorted is not None
        else 0
    )
    logger.info(
        f"[Convert task {convert_task_index}]: Find deletes got {len(data_table_total)} data table records, "
        f"{len(equality_delete_table_total)} equality deletes as input, "
        f"Produced {pos_delete_count} position deletes based off find deletes input."
    )

    if not new_pos_delete_table_total:
        logger.info("No records deleted based on equality delete convertion")

    if not remaining_data_table:
        logger.info("No data table remaining after converting equality deletes")

    return new_pos_delete_table_total_sorted, remaining_data_table


def process_sub_buckets_with_hash_bucket_tasks(
    convert_input_files,
    identifier_fields: List[str],
    s3_client_kwargs: Optional[Dict[str, Any]],
    convert_task_index: int,
):
    """
    Process sub-buckets using Ray remote hash_bucket tasks.

    Args:
        convert_input_files: ConvertInputFiles with sub-bucket information
        identifier_fields: List of identifier field names
        s3_client_kwargs: S3 client configuration
        convert_task_index: Current convert task index for logging

    Returns:
        Dict mapping bucket_number -> list of object_ref_ids from hash bucket processing
    """
    from deltacat.compute.converter.utils.convert_task_options import (
        sub_hash_bucket_input_options_provider,
    )

    logger.info(f"[Convert task {convert_task_index}]: Starting sub-bucket processing")

    sub_bucket_input_files = convert_input_files.sub_bucket_input_files
    sub_bucket_memory_estimates = convert_input_files.sub_bucket_memory_estimates

    if not sub_bucket_input_files:
        logger.warning(
            f"[Convert task {convert_task_index}]: No sub-bucket files to process"
        )
        return {}

    # Ensure memory estimates exist when sub-bucketing is enabled
    if not sub_bucket_memory_estimates or len(sub_bucket_memory_estimates) != len(
        sub_bucket_input_files
    ):
        raise ValueError(
            f"[Convert task {convert_task_index}]: Sub-bucket memory estimates must exist and match sub-bucket count. "
            f"Got {len(sub_bucket_memory_estimates) if sub_bucket_memory_estimates else 0} estimates "
            f"for {len(sub_bucket_input_files)} sub-buckets."
        )

    # Set number of hash buckets equal to number of sub-buckets
    num_hash_buckets = len(sub_bucket_input_files)
    logger.info(
        f"[Convert task {convert_task_index}]: Using {num_hash_buckets} hash buckets (equal to number of sub-buckets)"
    )

    # Create options provider using the existing sub_hash_bucket_input_options_provider
    def hash_bucket_options_provider(
        index: int, sub_bucket_files: DataFileList
    ) -> Dict[str, Any]:
        memory_estimate = sub_bucket_memory_estimates[index]
        return sub_hash_bucket_input_options_provider(
            sub_bucket_files=sub_bucket_files, memory_estimate=memory_estimate
        )

    # Create kwargs provider for hash_bucket task inputs
    def hash_bucket_kwargs_provider(
        index: int, sub_bucket_files: DataFileList
    ) -> Dict[str, Any]:
        logger.info(
            f"[Convert task {convert_task_index}]: Launching hash_bucket task {index} "
            f"with {len(sub_bucket_files)} files, memory: {sub_bucket_memory_estimates[index]:,} bytes"
        )

        return {
            "sub_bucket_input_files": sub_bucket_files,
            "identifier_columns": identifier_fields,
            "num_hash_buckets": num_hash_buckets,
            "s3_client_kwargs": s3_client_kwargs,
        }

    # Use invoke_parallel for hash_bucket tasks
    logger.info(
        f"[Convert task {convert_task_index}]: Invoking {len(sub_bucket_input_files)} hash_bucket tasks"
    )

    hash_bucket_tasks_pending = invoke_parallel(
        items=sub_bucket_input_files,
        ray_task=hash_bucket,
        max_parallelism=len(
            sub_bucket_input_files
        ),  # Process all sub-buckets in parallel
        options_provider=hash_bucket_options_provider,
        kwargs_provider=hash_bucket_kwargs_provider,
    )

    # Wait for all hash_bucket tasks to complete
    logger.info(
        f"[Convert task {convert_task_index}]: Waiting for {len(hash_bucket_tasks_pending)} hash_bucket tasks"
    )
    hash_bucket_results = ray.get(hash_bucket_tasks_pending)

    # Process results - each result is a tuple of (object_refs_dict, record_counts_dict)
    # Combine results from all sub-buckets into final dictionaries
    combined_object_refs = {}
    combined_record_counts = {}

    for sub_bucket_object_refs, sub_bucket_record_counts in hash_bucket_results:
        # Merge object references for each bucket number
        for bucket_number, object_ref_list in sub_bucket_object_refs.items():
            if bucket_number not in combined_object_refs:
                combined_object_refs[bucket_number] = []
            combined_object_refs[bucket_number].extend(object_ref_list)

        # Sum up record counts for each bucket number
        for bucket_number, record_count in sub_bucket_record_counts.items():
            if bucket_number not in combined_record_counts:
                combined_record_counts[bucket_number] = 0
            combined_record_counts[bucket_number] += record_count

    logger.info(
        f"[Convert task {convert_task_index}]: Combined results from {len(hash_bucket_results)} sub-buckets "
        f"into {len(combined_object_refs)} hash buckets with "
        f"{sum(combined_record_counts.values())} total records"
    )

    return combined_object_refs, combined_record_counts
