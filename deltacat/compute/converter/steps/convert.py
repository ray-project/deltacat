import pyarrow.compute as pc

import deltacat.compute.converter.utils.iceberg_columns as sc
import pyarrow as pa

from collections import defaultdict
import ray
import logging
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.steps.dedupe import dedupe_data_files
from deltacat.compute.converter.utils.s3u import upload_table_with_retry
from deltacat.compute.converter.utils.io import (
    download_data_table_and_append_iceberg_columns,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    partition_value_record_to_partition_value_string,
)
from deltacat.compute.converter.pyiceberg.overrides import (
    parquet_files_dict_to_iceberg_data_files,
)
from deltacat.compute.converter.model.convert_result import ConvertResult
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def convert(convert_input: ConvertInput):
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
    s3_file_system = convert_input.s3_file_system
    s3_client_kwargs = convert_input.s3_client_kwargs
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
    if applicable_equality_delete_files:
        (
            pos_delete_after_converting_equality_delete
        ) = compute_pos_delete_with_limited_parallelism(
            data_files_list=applicable_data_files,
            identifier_columns=identifier_fields,
            equality_delete_files_list=applicable_equality_delete_files,
            iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
            convert_task_index=convert_task_index,
            max_parallel_data_file_download=max_parallel_data_file_download,
            s3_file_system=s3_file_system,
            s3_client_kwargs=s3_client_kwargs,
        )
        if pos_delete_after_converting_equality_delete:
            total_pos_delete_table.append(pos_delete_after_converting_equality_delete)

    if enforce_primary_key_uniqueness:
        data_files_to_dedupe = get_additional_applicable_data_files(
            all_data_files=all_data_files_for_this_bucket,
            data_files_downloaded=applicable_data_files,
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
        to_be_added_files_list_parquet = upload_table_with_retry(
            table=total_pos_delete,
            s3_url_prefix=iceberg_table_warehouse_prefix_with_partition,
            s3_table_writer_kwargs={},
            s3_file_system=s3_file_system,
        )

        to_be_added_files_dict = defaultdict()
        to_be_added_files_dict[partition_value] = to_be_added_files_list_parquet

        logger.info(
            f"[Convert task {convert_task_index}]: Produced {len(to_be_added_files_list_parquet)} position delete files."
        )
        to_be_added_files_list = parquet_files_dict_to_iceberg_data_files(
            io=table_io,
            table_metadata=table_metadata,
            files_dict=to_be_added_files_dict,
        )

    to_be_delete_files_dict = defaultdict()
    if applicable_equality_delete_files:
        to_be_delete_files_dict[partition_value] = [
            equality_delete_file[1]
            for equality_delete_file in applicable_equality_delete_files
        ]

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
    )
    return convert_res


def get_additional_applicable_data_files(all_data_files, data_files_downloaded):
    data_file_to_dedupe = all_data_files
    if data_files_downloaded:
        data_file_to_dedupe = list(set(all_data_files) - set(data_files_downloaded))
    return data_file_to_dedupe


def filter_rows_to_be_deleted(
    equality_delete_table, data_file_table, identifier_columns
):
    identifier_column = identifier_columns[0]
    if equality_delete_table and data_file_table:
        equality_deletes = pc.is_in(
            data_file_table[identifier_column],
            equality_delete_table[identifier_column],
        )
        position_delete_table = data_file_table.filter(equality_deletes)
    return position_delete_table


def compute_pos_delete_converting_equality_deletes(
    equality_delete_table,
    data_file_table,
    identifier_columns,
    iceberg_table_warehouse_prefix_with_partition,
    s3_file_system,
):
    new_position_delete_table = filter_rows_to_be_deleted(
        data_file_table=data_file_table,
        equality_delete_table=equality_delete_table,
        identifier_columns=identifier_columns,
    )
    if new_position_delete_table:
        logger.info(
            f"Length of position delete table after converting from equality deletes:{len(new_position_delete_table)}"
        )
    else:
        return None
    return new_position_delete_table


def compute_pos_delete_with_limited_parallelism(
    data_files_list,
    identifier_columns,
    equality_delete_files_list,
    iceberg_table_warehouse_prefix_with_partition,
    convert_task_index,
    max_parallel_data_file_download,
    s3_file_system,
    s3_client_kwargs,
):
    for data_files, equality_delete_files in zip(
        data_files_list, equality_delete_files_list
    ):
        data_table_total = []
        for data_file in data_files:
            data_table = download_data_table_and_append_iceberg_columns(
                data_files=data_file[1],
                columns_to_download=identifier_columns,
                additional_columns_to_append=[
                    sc._FILE_PATH_COLUMN_NAME,
                    sc._ORDERED_RECORD_IDX_COLUMN_NAME,
                ],
                sequence_number=data_file[0],
                s3_client_kwargs=s3_client_kwargs,
            )
            data_table_total.append(data_table)
        data_table_total = pa.concat_tables(data_table_total)

        equality_delete_table_total = []
        for equality_delete in equality_delete_files:
            equality_delete_table = download_data_table_and_append_iceberg_columns(
                data_files=equality_delete[1],
                columns_to_download=identifier_columns,
                s3_client_kwargs=s3_client_kwargs,
            )
            equality_delete_table_total.append(equality_delete_table)
        equality_delete_table_total = pa.concat_tables(equality_delete_table_total)

    new_pos_delete_table = compute_pos_delete_converting_equality_deletes(
        equality_delete_table=equality_delete_table_total,
        data_file_table=data_table_total,
        iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
        identifier_columns=identifier_columns,
        s3_file_system=s3_file_system,
        s3_client_kwargs=s3_client_kwargs,
    )

    logger.info(
        f"[Convert task {convert_task_index}]: Find deletes got {len(data_table_total)} data table records, "
        f"{len(equality_delete_table_total)} equality deletes as input, "
        f"Produced {len(new_pos_delete_table)} position deletes based off find deletes input."
    )

    if not new_pos_delete_table:
        logger.info("No records deleted based on equality delete convertion")

    return new_pos_delete_table
