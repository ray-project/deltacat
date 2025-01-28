# from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
    task_resource_options_provider,
)
import ray
import functools
from deltacat.compute.converter.utils.convert_task_options import (
    convert_resource_options_provider,
)
import logging
from deltacat import logs
from collections import defaultdict
from deltacat.compute.converter.model.converter_session_params import (
    ConverterSessionParams,
)
from deltacat.compute.converter.constants import DEFAULT_MAX_PARALLEL_DATA_FILE_DOWNLOAD
from deltacat.compute.converter.steps.convert import convert
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.pyiceberg.overrides import (
    fetch_all_bucket_files,
    parquet_files_dict_to_iceberg_data_files,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    check_data_files_sequence_number,
)
from deltacat.compute.converter.pyiceberg.replace_snapshot import (
    commit_overwrite_snapshot,
)
from deltacat.compute.converter.pyiceberg.catalog import load_table

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def converter_session(params: ConverterSessionParams, **kwargs):
    """
    Convert equality delete to position delete.
    Compute and memory heavy work from downloading equality delete table and compute position deletes
    will be executed on Ray remote tasks.
    """

    catalog = params.catalog
    table_name = params.iceberg_table_name
    iceberg_table = load_table(catalog, table_name)
    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(
        iceberg_table
    )

    # files_for_each_bucket contains the following files list:
    # {partition_value: [(equality_delete_files_list, data_files_list, pos_delete_files_list)]
    files_for_each_bucket = defaultdict(tuple)
    for k, v in data_file_dict.items():
        logger.info(f"data_file: k, v:{k, v}")
    for k, v in equality_delete_dict.items():
        logger.info(f"equality_delete_file: k, v:{k, v}")
    for partition_value, equality_delete_file_list in equality_delete_dict.items():
        (
            result_equality_delete_file,
            result_data_file,
        ) = check_data_files_sequence_number(
            data_files_list=data_file_dict[partition_value],
            equality_delete_files_list=equality_delete_dict[partition_value],
        )
        logger.info(f"result_data_file:{result_data_file}")
        logger.info(f"result_equality_delete_file:{result_equality_delete_file}")
        files_for_each_bucket[partition_value] = (
            result_data_file,
            result_equality_delete_file,
            [],
        )

    iceberg_warehouse_bucket_name = params.iceberg_warehouse_bucket_name
    print(f"iceberg_warehouse_bucket_name:{iceberg_warehouse_bucket_name}")
    merge_keys = params.merge_keys
    # Using table identifier fields as merge keys if merge keys not provided
    if not merge_keys:
        identifier_fields_set = iceberg_table.schema().identifier_field_names()
        identifier_fields = list(identifier_fields_set)
    else:
        identifier_fields = merge_keys
    if len(identifier_fields) > 1:
        raise NotImplementedError(
            f"Multiple identifier fields lookup not supported yet."
        )
    convert_options_provider = functools.partial(
        task_resource_options_provider,
        resource_amount_provider=convert_resource_options_provider,
    )

    # TODO (zyiqin): max_parallel_data_file_download should be determined by memory requirement for each bucket.
    #  Specifically, for case when files for one bucket memory requirement exceed one worker node's memory limit, WITHOUT rebasing with larger hash bucket count,
    #  1. We can control parallel files to download by adjusting max_parallel_data_file_download.
    #  2. Implement two-layer converter tasks, with convert tasks to spin up child convert tasks.
    #  Note that approach 2 will ideally require shared object store to avoid download equality delete files * number of child tasks times.
    max_parallel_data_file_download = DEFAULT_MAX_PARALLEL_DATA_FILE_DOWNLOAD

    compact_small_files = params.compact_small_files
    position_delete_for_multiple_data_files = (
        params.position_delete_for_multiple_data_files
    )
    task_max_parallelism = params.task_max_parallelism

    def convert_input_provider(index, item):
        return {
            "convert_input": ConvertInput.of(
                files_for_each_bucket=item,
                convert_task_index=index,
                iceberg_warehouse_bucket_name=iceberg_warehouse_bucket_name,
                identifier_fields=identifier_fields,
                compact_small_files=compact_small_files,
                position_delete_for_multiple_data_files=position_delete_for_multiple_data_files,
                max_parallel_data_file_download=max_parallel_data_file_download,
            )
        }

    # Ray remote task: convert
    # Assuming that memory consume by each bucket doesn't exceed one node's memory limit.
    # TODO: Add split mechanism to split large buckets
    convert_tasks_pending = invoke_parallel(
        items=files_for_each_bucket.items(),
        ray_task=convert,
        max_parallelism=task_max_parallelism,
        options_provider=convert_options_provider,
        kwargs_provider=convert_input_provider,
    )
    to_be_deleted_files_list = []
    to_be_added_files_dict_list = []
    convert_results = ray.get(convert_tasks_pending)
    for convert_result in convert_results:
        to_be_deleted_files_list.extend(convert_result[0].values())
        to_be_added_files_dict_list.append(convert_result[1])

    new_position_delete_files = parquet_files_dict_to_iceberg_data_files(
        io=iceberg_table.io,
        table_metadata=iceberg_table.metadata,
        files_dict_list=to_be_added_files_dict_list,
    )
    commit_overwrite_snapshot(
        iceberg_table=iceberg_table,
        # equality_delete_files + data file that all rows are deleted
        to_be_deleted_files_list=to_be_deleted_files_list[0],
        new_position_delete_files=new_position_delete_files,
    )
