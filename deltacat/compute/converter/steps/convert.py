import pyarrow.compute as pc
import deltacat.compute.converter.utils.iceberg_columns as sc
import pyarrow as pa
import numpy as np
import daft
from collections import defaultdict
import ray
import logging
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.utils.s3u import upload_table_with_retry
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def convert(convert_input: ConvertInput):
    files_for_each_bucket = convert_input.files_for_each_bucket
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
    data_files, equality_delete_files, position_delete_files = files_for_each_bucket[1]
    # Get string representation of partition value out of Record[partition_value]
    partition_value_str = (
        files_for_each_bucket[0].__repr__().split("[", 1)[1].split("]")[0]
    )
    partition_value_str = (
        files_for_each_bucket[0].__repr__().split("[", 1)[1].split("]")[0]
    )
    partition_value = files_for_each_bucket[0]
    iceberg_table_warehouse_prefix_with_partition = (
        f"{iceberg_table_warehouse_prefix}/{partition_value_str}"
    )
    (
        to_be_deleted_files_list,
        to_be_added_files_list,
    ) = compute_pos_delete_with_limited_parallelism(
        data_files_list=data_files,
        identifier_columns=identifier_fields,
        equality_delete_files_list=equality_delete_files,
        iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
        max_parallel_data_file_download=max_parallel_data_file_download,
        s3_file_system=s3_file_system,
    )
    to_be_delete_files_dict = defaultdict()
    to_be_delete_files_dict[partition_value] = to_be_deleted_files_list
    to_be_added_files_dict = defaultdict()
    to_be_added_files_dict[partition_value] = to_be_added_files_list
    return (to_be_delete_files_dict, to_be_added_files_dict)


def filter_rows_to_be_deleted(
    equality_delete_table, data_file_table, identifier_columns
):
    identifier_column = identifier_columns[0]
    if equality_delete_table and data_file_table:
        equality_deletes = pc.is_in(
            data_file_table[identifier_column],
            equality_delete_table[identifier_column],
        )
        positional_delete_table = data_file_table.filter(equality_deletes)
        logger.info(f"positional_delete_table:{positional_delete_table.to_pydict()}")
        logger.info(f"data_file_table:{data_file_table.to_pydict()}")
        logger.info(
            f"length_pos_delete_table, {len(positional_delete_table)}, length_data_table:{len(data_file_table)}"
        )
    if positional_delete_table:
        # TODO: Add support for multiple identify columns
        identifier_column = identifier_columns[0]
        positional_delete_table = positional_delete_table.drop(identifier_column)
    if len(positional_delete_table) == len(data_file_table):
        return True, None
    return False, positional_delete_table


def compute_pos_delete(
    equality_delete_table,
    data_file_table,
    identifier_columns,
    iceberg_table_warehouse_prefix_with_partition,
    s3_file_system,
):
    delete_whole_file, new_position_delete_table = filter_rows_to_be_deleted(
        data_file_table=data_file_table,
        equality_delete_table=equality_delete_table,
        identifier_columns=identifier_columns,
    )
    if new_position_delete_table:
        logger.info(f"compute_pos_delete_table:{new_position_delete_table.to_pydict()}")
    if new_position_delete_table:
        new_pos_delete_s3_link = upload_table_with_retry(
            table=new_position_delete_table,
            s3_url_prefix=iceberg_table_warehouse_prefix_with_partition,
            s3_table_writer_kwargs={},
            s3_file_system=s3_file_system,
        )
    return delete_whole_file, new_pos_delete_s3_link


def download_bucketed_table(data_files, equality_delete_files):
    from deltacat.utils.pyarrow import s3_file_to_table

    compacted_table = s3_file_to_table(
        [data_file.file_path for data_file in data_files]
    )
    equality_delete_table = s3_file_to_table(
        [eq_file.file_path for eq_file in equality_delete_files]
    )
    return compacted_table, equality_delete_table


def download_data_table(data_files, columns):
    data_tables = []
    for file in data_files:
        table = download_parquet_with_daft_hash_applied(
            identify_columns=columns, file=file, s3_client_kwargs={}
        )
        table = table.append_column(
            sc._FILE_PATH_COLUMN_FIELD,
            pa.array(np.repeat(file.file_path, len(table)), sc._FILE_PATH_COLUMN_TYPE),
        )
        record_idx_iterator = iter(range(len(table)))
        table = sc.append_record_idx_col(table, record_idx_iterator)
        data_tables.append(table)
    return pa.concat_tables(data_tables)


def compute_pos_delete_with_limited_parallelism(
    data_files_list,
    identifier_columns,
    equality_delete_files_list,
    iceberg_table_warehouse_prefix_with_partition,
    max_parallel_data_file_download,
    s3_file_system,
):
    to_be_deleted_file_list = []
    to_be_added_pos_delete_file_list = []

    for data_files, equality_delete_files in zip(
        data_files_list, equality_delete_files_list
    ):
        data_table = download_data_table(
            data_files=data_files, columns=identifier_columns
        )
        equality_delete_table = download_data_table(
            data_files=equality_delete_files, columns=identifier_columns
        )
        delete_whole_file, new_pos_delete_s3_link = compute_pos_delete(
            equality_delete_table=equality_delete_table,
            data_file_table=data_table,
            iceberg_table_warehouse_prefix_with_partition=iceberg_table_warehouse_prefix_with_partition,
            identifier_columns=identifier_columns,
            s3_file_system=s3_file_system,
        )
        if delete_whole_file:
            to_be_deleted_file_list.extend(data_files)
        to_be_deleted_file_list.extend(equality_delete_files)
        if new_pos_delete_s3_link:
            to_be_added_pos_delete_file_list.extend(new_pos_delete_s3_link)

    to_be_deleted_file_list.extend(equality_delete_files)
    logger.info(f"convert_to_be_deleted_file_list:{to_be_deleted_file_list}")
    logger.info(
        f"convert_to_be_added_pos_delete_file_list:{to_be_added_pos_delete_file_list}"
    )
    return to_be_deleted_file_list, to_be_added_pos_delete_file_list


def download_parquet_with_daft_hash_applied(
    identify_columns, file, s3_client_kwargs, **kwargs
):
    from daft import TimeUnit

    # TODO: Add correct read kwargs as in:
    #  https://github.com/ray-project/deltacat/blob/383855a4044e4dfe03cf36d7738359d512a517b4/deltacat/utils/daft.py#L97

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )

    from deltacat.utils.daft import _get_s3_io_config

    # TODO: Use Daft SHA1 hash instead to minimize probably of data corruption
    io_config = _get_s3_io_config(s3_client_kwargs=s3_client_kwargs)
    df = daft.read_parquet(
        path=file.file_path,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )
    df = df.select(daft.col(identify_columns[0]).hash())
    arrow_table = df.to_arrow()
    return arrow_table
