from typing import Optional, Dict
from deltacat.exceptions import RetryableError

AVERAGE_FILE_PATH_COLUMN_SIZE_BYTES = 80
AVERAGE_POS_COLUMN_SIZE_BYTES = 4
XXHASH_BYTE_PER_RECORD = 8
MEMORY_BUFFER_RATE = 1.2


def estimate_fixed_hash_columns(hash_value_size_bytes_per_record, total_record_count):
    return hash_value_size_bytes_per_record * total_record_count


def get_total_record_from_iceberg_files(iceberg_files_list):
    total_record_count = 0
    for iceberg_files in iceberg_files_list:
        total_record_count += sum(file.record_count for file in iceberg_files)
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


def convert_resource_options_provider(index, files_for_each_bucket):
    (
        data_files_list,
        equality_delete_files_list,
        position_delete_files_list,
    ) = files_for_each_bucket[1]
    memory_requirement = estimate_convert_remote_option_resources(
        data_files_list, equality_delete_files_list
    )
    return _get_task_options(memory=memory_requirement)
