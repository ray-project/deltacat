from deltacat.constants import DEFAULT_NAMESPACE
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
from typing import Dict, List, Any, Callable, Tuple
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
from deltacat.compute.converter.model.convert_result import ConvertResult
from deltacat.compute.converter.utils.converter_session_utils import (
    _get_snapshot_action_description,
    _determine_snapshot_type,
    SnapshotType,
)

from pyiceberg.manifest import DataFile
from pyiceberg.table.metadata import TableMetadata

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def converter_session(
    params: ConverterSessionParams, **kwargs: Any
) -> Tuple[TableMetadata, int]:
    """
    Convert equality deletes to position deletes with option to enforce primary key uniqueness.

    This function processes Iceberg table files to convert equality delete files to position delete files.
    It can optionally enforce primary key uniqueness by keeping only the latest version of each
    primary key across all data files.

    **Memory Requirements:**
    - Minimum 512MB of free memory is required to run the converter

    **Process Overview:**
    1. Fetches all bucket files (data files, equality deletes, position deletes)
    2. Groups files by bucket for parallel processing
    3. Converts equality deletes to position deletes using Ray parallel tasks
    4. Enforces primary key uniqueness if enabled
    5. Commits appropriate snapshot (append, replace, or delete) to the Iceberg table


    Args:
        params: ConverterSessionParams containing all configuration parameters
            - catalog: Iceberg catalog instance
            - iceberg_table_name: Name of the target Iceberg table
            - enforce_primary_key_uniqueness: Whether to enforce PK uniqueness
            - iceberg_warehouse_bucket_name: S3 bucket for Iceberg warehouse
            - iceberg_namespace: Iceberg namespace
            - merge_keys: Optional list of merge key fields (uses table identifier fields if not provided)
            - compact_previous_position_delete_files: Whether to compact existing position delete files
            - task_max_parallelism: Maximum number of parallel Ray tasks
            - s3_client_kwargs: Additional S3 client configuration
            - s3_file_system: S3 file system instance
            - location_provider_prefix_override: Optional prefix override for file locations
            - position_delete_for_multiple_data_files: Whether to generate position deletes for multiple data files
        **kwargs: Additional keyword arguments (currently unused)

    Returns:
        Tuple[TableMetadata, int]: A tuple containing the table metadata and the committed snapshot ID

    Raises:
        Exception: If snapshot commitment fails or other critical errors occur

    """

    catalog = params.catalog
    table_name = params.iceberg_table_name
    if "." not in table_name:
        iceberg_namespace = params.iceberg_namespace or DEFAULT_NAMESPACE
        table_name = params.iceberg_table_name
        table_identifier = f"{iceberg_namespace}.{table_name}"
    else:
        table_identifier = table_name
        identifier_parts = table_identifier.split(".")
        iceberg_namespace = identifier_parts[0]
        table_name = identifier_parts[1]
    iceberg_table = load_table(catalog, table_identifier)
    enforce_primary_key_uniqueness = params.enforce_primary_key_uniqueness
    iceberg_warehouse_bucket_name = params.iceberg_warehouse_bucket_name
    merge_keys = params.merge_keys
    compact_previous_position_delete_files = (
        params.compact_previous_position_delete_files
    )
    task_max_parallelism = params.task_max_parallelism
    s3_client_kwargs = params.s3_client_kwargs
    s3_file_system = params.filesystem
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

    convert_options_provider: Callable = functools.partial(
        task_resource_options_provider,
        resource_amount_provider=convert_resource_options_provider,
    )

    # TODO (zyiqin): max_parallel_data_file_download should be determined by memory requirement for each bucket.
    #  Specifically, for case when files for one bucket memory requirement exceed one worker node's memory limit, WITHOUT rebasing with larger hash bucket count,
    #  1. We can control parallel files to download by adjusting max_parallel_data_file_download.
    #  2. Implement two-layer converter tasks, with convert tasks to spin up child convert tasks.
    #  Note that approach 2 will ideally require shared object store to avoid download equality delete files * number of child tasks times.
    max_parallel_data_file_download = DEFAULT_MAX_PARALLEL_DATA_FILE_DOWNLOAD

    def convert_input_provider(index: int, item: Any) -> Dict[str, ConvertInput]:
        task_opts = convert_options_provider(index, item)
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
                filesystem=s3_file_system,
                task_memory=task_opts["memory"],
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

    to_be_deleted_files_list: List[List[DataFile]] = []
    logger.info(f"Finished invoking {len(convert_tasks_pending)} convert tasks.")

    convert_results: List[ConvertResult] = ray.get(convert_tasks_pending)
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
    total_input_data_files_on_disk_size = sum(
        convert_result.input_data_files_on_disk_size
        for convert_result in convert_results
    )

    # Calculate memory usage statistics
    max_peak_memory_usage = max(
        convert_result.peak_memory_usage_bytes for convert_result in convert_results
    )
    avg_memory_usage_percentage = sum(
        convert_result.memory_usage_percentage for convert_result in convert_results
    ) / len(convert_results)
    max_memory_usage_percentage = max(
        convert_result.memory_usage_percentage for convert_result in convert_results
    )

    logger.info(
        f"Aggregated stats for {table_identifier}: "
        f"total position delete record count: {total_position_delete_record_count}, "
        f"total input data file record count: {total_input_data_file_record_count}, "
        f"total data file hash columns in memory sizes: {total_data_file_hash_columns_in_memory_sizes}, "
        f"total position delete file in memory sizes: {total_position_delete_file_in_memory_sizes}, "
        f"total position delete file on disk sizes: {total_position_delete_on_disk_sizes}, "
        f"total input data files on disk size: {total_input_data_files_on_disk_size}, "
        f"max peak memory usage: {max_peak_memory_usage} bytes, "
        f"average memory usage percentage: {avg_memory_usage_percentage:.2f}%, "
        f"max memory usage percentage: {max_memory_usage_percentage:.2f}%"
    )

    to_be_added_files_list: List[DataFile] = []
    for convert_result in convert_results:
        to_be_added_files = convert_result.to_be_added_files
        to_be_deleted_files = convert_result.to_be_deleted_files

        to_be_deleted_files_list.extend(to_be_deleted_files.values())
        to_be_added_files_list.extend(to_be_added_files)

    logger.info(f"To be deleted files list length: {len(to_be_deleted_files_list)}")
    logger.info(f"To be added files list length: {len(to_be_added_files_list)}")

    # Determine snapshot type and commit
    snapshot_type = _determine_snapshot_type(
        to_be_deleted_files_list, to_be_added_files_list
    )

    if snapshot_type == SnapshotType.NONE:
        logger.info(
            _get_snapshot_action_description(
                snapshot_type, to_be_deleted_files_list, to_be_added_files_list
            )
        )
        return iceberg_table.metadata, iceberg_table.metadata.current_snapshot_id

    logger.info(
        f"Snapshot action: {_get_snapshot_action_description(snapshot_type, to_be_deleted_files_list, to_be_added_files_list)}"
    )

    try:
        if snapshot_type == SnapshotType.APPEND:
            logger.info(f"Committing append snapshot for {table_identifier}.")
            converter_snapshot_id = commit_append_snapshot(
                iceberg_table=iceberg_table,
                new_position_delete_files=to_be_added_files_list,
            )
        elif snapshot_type == SnapshotType.REPLACE:
            logger.info(f"Committing replace snapshot for {table_identifier}.")
            converter_snapshot_id = commit_replace_snapshot(
                iceberg_table=iceberg_table,
                to_be_deleted_files=to_be_deleted_files_list,
                new_position_delete_files=to_be_added_files_list,
            )
        elif snapshot_type == SnapshotType.DELETE:
            logger.info(f"Committing delete snapshot for {table_identifier}.")
            converter_snapshot_id = commit_replace_snapshot(
                iceberg_table=iceberg_table,
                to_be_deleted_files=to_be_deleted_files_list,
                new_position_delete_files=[],  # No new files to add
            )
        else:
            logger.warning(f"Unexpected snapshot type: {snapshot_type}")
            return iceberg_table.metadata, iceberg_table.metadata.current_snapshot_id

        logger.info(
            f"Committed new Iceberg snapshot for {table_identifier}: {converter_snapshot_id}"
        )

        # Return the converter committed snapshot id
        return iceberg_table.metadata, converter_snapshot_id
    except Exception as e:
        logger.error(f"Failed to commit snapshot for {table_identifier}: {str(e)}")
        raise
