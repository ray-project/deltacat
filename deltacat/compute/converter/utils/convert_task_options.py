from typing import Optional, Dict
from deltacat.exceptions import RetryableError

AVERAGE_FILE_PATH_COLUMN_SIZE_BYTES = 80
AVERAGE_POS_COLUMN_SIZE_BYTES = 4
XXHASH_BYTE_PER_RECORD = 8
MEMORY_BUFFER_RATE = 2
# TODO: Add audit info to check this number in practice
# Worst case 2 as no duplicates exists across all pk
PYARROW_AGGREGATE_MEMORY_MULTIPLIER = 2


def estimate_fixed_hash_columns(hash_value_size_bytes_per_record, total_record_count):
    return hash_value_size_bytes_per_record * total_record_count


def get_total_record_from_iceberg_files(iceberg_files_list):
    total_record_count = 0
    # file are in form of tuple (sequence_number, DataFile)
    total_record_count += sum(file[1].record_count for file in iceberg_files_list)
    return total_record_count


def estimate_iceberg_pos_delete_additional_columns(
    include_columns, num_of_record_count
):
    total_additional_columns_sizes = 0
    if "file_path" in include_columns:
        total_additional_columns_sizes += (
            AVERAGE_FILE_PATH_COLUMN_SIZE_BYTES * num_of_record_count
        )
    elif "pos" in include_columns:
        total_additional_columns_sizes += (
            AVERAGE_POS_COLUMN_SIZE_BYTES * num_of_record_count
        )
    return total_additional_columns_sizes


def estimate_convert_remote_option_resources(data_files, equality_delete_files):
    data_file_record_count = get_total_record_from_iceberg_files(data_files)
    equality_delete_record_count = get_total_record_from_iceberg_files(
        equality_delete_files
    )
    hash_column_sizes = estimate_fixed_hash_columns(
        XXHASH_BYTE_PER_RECORD, data_file_record_count + equality_delete_record_count
    )
    pos_delete_sizes = estimate_iceberg_pos_delete_additional_columns(
        ["file_path", "pos"], data_file_record_count + equality_delete_record_count
    )
    total_memory_required = hash_column_sizes + pos_delete_sizes
    return total_memory_required * MEMORY_BUFFER_RATE


def _get_task_options(
    memory: float,
    ray_custom_resources: Optional[Dict] = None,
    scheduling_strategy: str = "SPREAD",
) -> Dict:

    # NOTE: With DEFAULT scheduling strategy in Ray 2.20.0, autoscaler does
    # not spin up enough nodes fast and hence we see only approximately
    # 20 tasks get scheduled out of 100 tasks in queue. Hence, we use SPREAD
    # which is also ideal for merge and hash bucket tasks.
    # https://docs.ray.io/en/latest/ray-core/scheduling/index.html
    task_opts = {
        "memory": memory,
        "scheduling_strategy": scheduling_strategy,
    }

    if ray_custom_resources:
        task_opts["resources"] = ray_custom_resources

    task_opts["max_retries"] = 3

    # List of possible botocore exceptions are available at
    # https://github.com/boto/botocore/blob/develop/botocore/exceptions.py
    task_opts["retry_exceptions"] = [RetryableError]

    return task_opts


def estimate_dedupe_memory(all_data_files_for_dedupe):
    dedupe_record_count = get_total_record_from_iceberg_files(all_data_files_for_dedupe)
    produced_pos_memory_required = estimate_iceberg_pos_delete_additional_columns(
        ["file_path", "pos"], dedupe_record_count
    )
    download_pk_memory_required = estimate_fixed_hash_columns(
        XXHASH_BYTE_PER_RECORD, dedupe_record_count
    )
    memory_required_by_dedupe = (
        produced_pos_memory_required + download_pk_memory_required
    ) * PYARROW_AGGREGATE_MEMORY_MULTIPLIER
    memory_with_buffer = memory_required_by_dedupe * MEMORY_BUFFER_RATE
    return memory_with_buffer


def convert_resource_options_provider(index, convert_input_files):
    applicable_data_files = convert_input_files.applicable_data_files
    applicable_equality_delete_files = (
        convert_input_files.applicable_equality_delete_files
    )
    all_data_files_for_dedupe = convert_input_files.all_data_files_for_dedupe
    total_memory_required = 0
    if applicable_data_files and applicable_equality_delete_files:
        memory_requirement_for_convert_equality_deletes = (
            estimate_convert_remote_option_resources(
                applicable_data_files, applicable_equality_delete_files
            )
        )
        total_memory_required += memory_requirement_for_convert_equality_deletes
    if all_data_files_for_dedupe:
        memory_requirement_for_dedupe = estimate_dedupe_memory(
            all_data_files_for_dedupe
        )
        total_memory_required += memory_requirement_for_dedupe
    return _get_task_options(memory=total_memory_required)
