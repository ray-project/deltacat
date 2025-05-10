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
from deltacat.compute.converter.model.converter_session_params import (
    ConverterSessionParams,
)


from deltacat.compute.converter.constants import DEFAULT_MAX_PARALLEL_DATA_FILE_DOWNLOAD
from deltacat.compute.converter.steps.convert import convert
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.pyiceberg.overrides import (
    fetch_all_bucket_files,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    construct_iceberg_table_prefix,
)
from deltacat.compute.converter.pyiceberg.update_snapshot_overrides import (
    commit_replace_snapshot,
    commit_append_snapshot,
)
from deltacat.compute.converter.pyiceberg.catalog import load_table
from deltacat.compute.converter.utils.converter_session_utils import (
    group_all_files_to_each_bucket,
)

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
    enforce_primary_key_uniqueness = params.enforce_primary_key_uniqueness
    iceberg_warehouse_bucket_name = params.iceberg_warehouse_bucket_name
    iceberg_namespace = params.iceberg_namespace
    merge_keys = params.merge_keys
    compact_previous_position_delete_files = (
        params.compact_previous_position_delete_files
    )
    task_max_parallelism = params.task_max_parallelism
    s3_client_kwargs = params.s3_client_kwargs
    s3_file_system = params.s3_file_system
    location_provider_prefix_override = params.location_provider_prefix_override
    position_delete_for_multiple_data_files = (
        params.position_delete_for_multiple_data_files
    )

    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(
        iceberg_table
    )

    convert_input_files_for_all_buckets = group_all_files_to_each_bucket(
        data_file_dict=data_file_dict,
        equality_delete_dict=equality_delete_dict,
        pos_delete_dict=pos_delete_dict,
    )

    if not location_provider_prefix_override:
        iceberg_table_warehouse_prefix = construct_iceberg_table_prefix(
            iceberg_warehouse_bucket_name=iceberg_warehouse_bucket_name,
            table_name=table_name,
            iceberg_namespace=iceberg_namespace,
        )
    else:
        iceberg_table_warehouse_prefix = location_provider_prefix_override

    # Using table identifier fields as merge keys if merge keys not provided
    if not merge_keys:
        identifier_fields_set = iceberg_table.schema().identifier_field_names()
        identifier_fields = list(identifier_fields_set)
    else:
        identifier_fields = merge_keys

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

    def convert_input_provider(index, item):
        return {
            "convert_input": ConvertInput.of(
                convert_input_files=item,
                convert_task_index=index,
                iceberg_table_warehouse_prefix=iceberg_table_warehouse_prefix,
                identifier_fields=identifier_fields,
                compact_previous_position_delete_files=compact_previous_position_delete_files,
                table_io=iceberg_table.io,
                table_metadata=iceberg_table.metadata,
                enforce_primary_key_uniqueness=enforce_primary_key_uniqueness,
                position_delete_for_multiple_data_files=position_delete_for_multiple_data_files,
                max_parallel_data_file_download=max_parallel_data_file_download,
                s3_client_kwargs=s3_client_kwargs,
                s3_file_system=s3_file_system,
            )
        }

    logger.info(f"Getting remote convert tasks...")
    # Ray remote task: convert
    # TODO: Add split mechanism to split large buckets
    convert_tasks_pending = invoke_parallel(
        items=convert_input_files_for_all_buckets,
        ray_task=convert,
        max_parallelism=task_max_parallelism,
        options_provider=convert_options_provider,
        kwargs_provider=convert_input_provider,
    )

    to_be_deleted_files_list = []
    logger.info(f"Finished invoking {len(convert_tasks_pending)} convert tasks.")

    convert_results = ray.get(convert_tasks_pending)
    logger.info(f"Got {len(convert_tasks_pending)} convert tasks.")

    total_position_delete_record_count = sum(
        convert_result.position_delete_record_count
        for convert_result in convert_results
    )
    total_input_data_file_record_count = sum(
        convert_result.input_data_files_record_count
        for convert_result in convert_results
    )
    total_data_file_hash_columns_in_memory_sizes = sum(
        convert_result.input_data_files_hash_columns_in_memory_sizes
        for convert_result in convert_results
    )
    total_position_delete_file_in_memory_sizes = sum(
        convert_result.position_delete_in_memory_sizes
        for convert_result in convert_results
    )
    total_position_delete_on_disk_sizes = sum(
        convert_result.position_delete_on_disk_sizes
        for convert_result in convert_results
    )

    to_be_added_files_list = []
    for convert_result in convert_results:
        to_be_added_files = convert_result.to_be_added_files
        to_be_deleted_files = convert_result.to_be_deleted_files

        to_be_deleted_files_list.extend(to_be_deleted_files.values())
        to_be_added_files_list.extend(to_be_added_files)

    if not to_be_deleted_files_list and to_be_added_files_list:
        commit_append_snapshot(
            iceberg_table=iceberg_table,
            new_position_delete_files=to_be_added_files_list,
        )
    else:
        commit_replace_snapshot(
            iceberg_table=iceberg_table,
            to_be_deleted_files_list=to_be_deleted_files_list,
            new_position_delete_files=to_be_added_files_list,
        )
    logger.info(
        f"Aggregated stats for {table_name}: "
        f"total position delete record count: {total_position_delete_record_count}, "
        f"total input data file record_count: {total_input_data_file_record_count}, "
        f"total data file hash columns in memory sizes: {total_data_file_hash_columns_in_memory_sizes}, "
        f"total position delete file in memory sizes: {total_position_delete_file_in_memory_sizes}, "
        f"total position delete file on disk sizes: {total_position_delete_on_disk_sizes}."
    )

    logger.info(f"Committed new Iceberg snapshot.")
