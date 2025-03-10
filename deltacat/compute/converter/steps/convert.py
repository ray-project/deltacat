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

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def convert(convert_input: ConvertInput):
    convert_input_files = convert_input.convert_input_files
    convert_task_index = convert_input.convert_task_index
    iceberg_table_warehouse_prefix = convert_input.iceberg_table_warehouse_prefix
    identifier_fields = convert_input.identifier_fields
    compact_small_files = convert_input.compact_small_files
    position_delete_for_multiple_data_files = (
        convert_input.position_delete_for_multiple_data_files
    )
    max_parallel_data_file_download = convert_input.max_parallel_data_file_download
    s3_file_system = convert_input.s3_file_system
    if not position_delete_for_multiple_data_files:
        raise NotImplementedError(
            f"Distributed file level position delete compute is not supported yet"
        )
    if compact_small_files:
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
    iceberg_table_warehouse_prefix_with_partition = (
        f"{iceberg_table_warehouse_prefix}/{partition_value_str}"
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
            max_parallel_data_file_download=max_parallel_data_file_download,
            s3_file_system=s3_file_system,
        )
        if pos_delete_after_converting_equality_delete:
            total_pos_delete_table.append(pos_delete_after_converting_equality_delete)

    if enforce_primary_key_uniqueness:
        data_files_to_dedupe = get_additional_applicable_data_files(
            all_data_files=all_data_files_for_this_bucket,
            data_files_downloaded=applicable_data_files,
        )
        pos_delete_after_dedupe = dedupe_data_files(
            data_file_to_dedupe=data_files_to_dedupe,
            identify_column_name_concatenated=identifier_fields[0],
            identifier_columns=identifier_fields,
            merge_sort_column=sc._ORDERED_RECORD_IDX_COLUMN_NAME,
        )
        total_pos_delete_table.append(pos_delete_after_dedupe)

    total_pos_delete = pa.concat_tables(total_pos_delete_table)
    to_be_added_files_list = upload_table_with_retry(
        table=total_pos_delete,
        s3_url_prefix=iceberg_table_warehouse_prefix_with_partition,
        s3_table_writer_kwargs={},
        s3_file_system=s3_file_system,
    )

    to_be_delete_files_dict = defaultdict()
    if applicable_equality_delete_files:
        to_be_delete_files_dict[partition_value] = [
            equality_delete_file[1]
            for equality_delete_file in applicable_equality_delete_files
        ]
    to_be_added_files_dict = defaultdict()
    to_be_added_files_dict[partition_value] = to_be_added_files_list
    return (to_be_delete_files_dict, to_be_added_files_dict)


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
        logger.info(f"positional_delete_table:{position_delete_table.to_pydict()}")
        logger.info(f"data_file_table:{data_file_table.to_pydict()}")
        logger.info(
            f"length_pos_delete_table, {len(position_delete_table)}, length_data_table:{len(data_file_table)}"
        )
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


def download_bucketed_table(data_files, equality_delete_files):
    from deltacat.utils.pyarrow import s3_file_to_table

    compacted_table = s3_file_to_table(
        [data_file.file_path for data_file in data_files]
    )
    equality_delete_table = s3_file_to_table(
        [eq_file.file_path for eq_file in equality_delete_files]
    )
    return compacted_table, equality_delete_table


def compute_pos_delete_with_limited_parallelism(
    data_files_list,
    identifier_columns,
    equality_delete_files_list,
    iceberg_table_warehouse_prefix_with_partition,
    max_parallel_data_file_download,
    s3_file_system,
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
            )
            data_table_total.append(data_table)
        data_table_total = pa.concat_tables(data_table_total)

        equality_delete_table_total = []
        for equality_delete in equality_delete_files:
            equality_delete_table = download_data_table_and_append_iceberg_columns(
                data_files=equality_delete[1],
                columns_to_download=identifier_columns,
            )
            equality_delete_table_total.append(equality_delete_table)
        equality_delete_table_total = pa.concat_tables(equality_delete_table_total)

    new_pos_delete_table = compute_pos_delete_converting_equality_deletes(
        equality_delete_table=equality_delete_table_total,
        data_file_table=data_table_total,
        iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
        identifier_columns=identifier_columns,
        s3_file_system=s3_file_system,
    )
    if not new_pos_delete_table:
        logger.info("No records deleted based on equality delete converstion")

    logger.info(
        f"Number of records to delete based on equality delete convertion:{len(new_pos_delete_table)}"
    )
    return new_pos_delete_table
