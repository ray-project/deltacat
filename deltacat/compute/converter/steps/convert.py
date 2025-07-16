import pyarrow.compute as pc

import deltacat.compute.converter.utils.iceberg_columns as sc
import pyarrow as pa

from collections import defaultdict
import ray
import logging
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.steps.dedupe import dedupe_data_files
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

    all_data_files_for_this_bucket = convert_input_files.all_data_files_for_dedupe

    partition_value_str = partition_value_record_to_partition_value_string(
        convert_input_files.partition_value
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

    enforce_primary_key_uniqueness = convert_input.enforce_primary_key_uniqueness
    total_pos_delete_table = []
    data_table_after_converting_equality_delete = []
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

        (
            pos_delete_after_dedupe,
            data_file_to_dedupe_record_count,
            data_file_to_dedupe_size,
        ) = dedupe_data_files(
            data_file_to_dedupe=data_files_to_dedupe,
            identifier_columns=identifier_fields,
            remaining_data_table_after_convert=data_table_after_converting_equality_delete,
            merge_sort_column=sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            s3_client_kwargs=s3_client_kwargs,
        )
        logger.info(
            f"[Convert task {convert_task_index}]: Dedupe produced {len(pos_delete_after_dedupe)} position delete records."
        )
        total_pos_delete_table.append(pos_delete_after_dedupe)

    total_pos_delete = pa.concat_tables(total_pos_delete_table)

    logger.info(
        f"[Convert task {convert_task_index}]: Total position delete produced:{len(total_pos_delete)}"
    )

    to_be_added_files_list = []
    if total_pos_delete:
        to_be_added_files_list_parquet = write_sliced_table(
            table=total_pos_delete,
            base_path=iceberg_table_warehouse_prefix_with_partition,
            table_writer_kwargs={},
            filesystem=filesystem,
        )

        to_be_added_files_dict = defaultdict()
        to_be_added_files_dict[partition_value] = to_be_added_files_list_parquet

        logger.info(
            f"[Convert task {convert_task_index}]: Produced {len(to_be_added_files_list_parquet)} position delete files."
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

    if not enforce_primary_key_uniqueness:
        data_file_to_dedupe_record_count = 0
        data_file_to_dedupe_size = 0

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
        position_delete_record_count=len(total_pos_delete),
        input_data_files_record_count=data_file_to_dedupe_record_count,
        input_data_files_hash_columns_in_memory_sizes=data_file_to_dedupe_size,
        position_delete_in_memory_sizes=int(total_pos_delete.nbytes),
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
            list(set(data_file_to_dedupe).difference(set(data_files_downloaded)))
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
        new_pos_delete_table_total.append(new_pos_delete_table)

    if new_pos_delete_table_total:
        new_pos_delete_table_total = pa.concat_tables(new_pos_delete_table_total)

    logger.info(
        f"[Convert task {convert_task_index}]: Find deletes got {len(data_table_total)} data table records, "
        f"{len(equality_delete_table_total)} equality deletes as input, "
        f"Produced {len(new_pos_delete_table_total)} position deletes based off find deletes input."
    )

    if not new_pos_delete_table_total:
        logger.info("No records deleted based on equality delete convertion")

    if not remaining_data_table:
        logger.info("No data table remaining after converting equality deletes")

    return new_pos_delete_table_total, remaining_data_table
