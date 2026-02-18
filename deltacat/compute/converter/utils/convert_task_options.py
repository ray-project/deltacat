from typing import Optional, Dict, List, Tuple, Any
from deltacat.exceptions import RetryableError
from pyiceberg.manifest import DataFile
import logging
from deltacat import logs
from deltacat.compute.converter.model.convert_input_files import (
    ConvertInputFiles,
    DataFileList,
)

AVERAGE_FILE_PATH_COLUMN_SIZE_BYTES = 160
AVERAGE_POS_COLUMN_SIZE_BYTES = 4
XXHASH_BYTE_PER_RECORD = 8
SHA1_BYTE_PER_RECORD = 20
MEMORY_BUFFER_RATE = 1
# Worst case 2 as no duplicates exists across all pk
PYARROW_AGGREGATE_MEMORY_MULTIPLIER = 1.6
# Observed base memory usage at the beginning of each worker process
BASE_MEMORY_BUFFER = 0.3 * 1024 * 1024 * 1024
# Memory reserved for parent tasks when sub-bucketing is enabled (500MB)
SUB_BUCKET_PARENT_TASK_MEMORY_RESERVED = 500 * 1024 * 1024
# Default sub-bucket threshold for memory estimation (100 million records)
SUB_BUCKET_THRESHOLD = 100_000_000
# Threshold for splitting files into sub-buckets (50 million records)
SUB_BUCKET_SPLIT_THRESHOLD = 50_000_000

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def estimate_fixed_hash_columns(
    hash_value_size_bytes_per_record: int,
    total_record_count: int,
    identifier_fields: List[str],
) -> int:
    num_of_identifier_fields = len(identifier_fields)
    return (
        hash_value_size_bytes_per_record * total_record_count * num_of_identifier_fields
    )


def get_total_record_from_iceberg_files(
    iceberg_files_list: List[Tuple[int, DataFile]]
) -> int:
    total_record_count = 0
    # file are in form of tuple (sequence_number, DataFile)
    total_record_count += sum(file[1].record_count for file in iceberg_files_list)
    return total_record_count


def estimate_iceberg_pos_delete_additional_columns(
    include_columns: List[str], num_of_record_count: int
) -> int:
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


def estimate_convert_remote_option_resources(
    data_files: List[Tuple[int, DataFile]],
    equality_delete_files: List[Tuple[int, DataFile]],
    identifier_fields: List[str],
) -> float:
    data_file_record_count = get_total_record_from_iceberg_files(data_files)
    equality_delete_record_count = get_total_record_from_iceberg_files(
        equality_delete_files
    )
    hash_column_sizes = estimate_fixed_hash_columns(
        SHA1_BYTE_PER_RECORD,
        data_file_record_count + equality_delete_record_count,
        identifier_fields,
    )
    pos_delete_sizes = estimate_iceberg_pos_delete_additional_columns(
        ["file_path", "pos"], data_file_record_count + equality_delete_record_count
    )
    total_memory_required = hash_column_sizes + pos_delete_sizes
    return total_memory_required * MEMORY_BUFFER_RATE


def _get_task_options(
    memory: float,
    ray_custom_resources: Optional[Dict[str, Any]] = None,
    scheduling_strategy: str = "SPREAD",
) -> Dict[str, Any]:

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
    task_opts["num_cpus"] = 1
    task_opts["resources"] = {"convert_task": 1}
    # List of possible botocore exceptions are available at
    # https://github.com/boto/botocore/blob/develop/botocore/exceptions.py
    task_opts["retry_exceptions"] = [RetryableError]

    return task_opts


def dedupe_hash_bucket_memory_options_provider(
    bucket_number: int, record_count: int, identifier_fields: List[str], **kwargs
) -> Dict[str, Any]:
    """
    Provide resource options for dedupe_sub_hash_bucket tasks based on actual record count.

    Args:
        bucket_number: Hash bucket number for logging
        record_count: Actual number of records in this hash bucket
        identifier_fields: List of identifier field names

    Returns:
        Task options dict with precise memory estimation for dedupe tasks
    """
    if record_count > 0:
        # Calculate memory for position delete columns (file_path + pos)
        pos_memory = estimate_iceberg_pos_delete_additional_columns(
            ["file_path", "pos"], record_count
        )

        # Calculate memory for hash columns (SHA1 concatenated identifier fields)
        hash_memory = estimate_fixed_hash_columns(
            SHA1_BYTE_PER_RECORD, record_count, identifier_fields
        )

        # Apply multipliers and buffer
        memory_estimate = (
            (pos_memory + hash_memory)
            * PYARROW_AGGREGATE_MEMORY_MULTIPLIER
            * MEMORY_BUFFER_RATE
        )

        # Ensure minimum memory allocation using BASE_MEMORY_BUFFER
        memory_estimate = max(memory_estimate, BASE_MEMORY_BUFFER)

        logger.info(
            f"Hash bucket {bucket_number}: {record_count:,} records -> {memory_estimate:,} bytes memory"
        )
    else:
        # Default memory for empty buckets using BASE_MEMORY_BUFFER
        memory_estimate = BASE_MEMORY_BUFFER
        logger.info(
            f"Hash bucket {bucket_number}: empty bucket -> {memory_estimate:,} bytes default memory"
        )

    return {
        "memory": memory_estimate,
        "scheduling_strategy": "SPREAD",
        "max_retries": 3,
        "num_cpus": 1,
        "resources": {"dedupe_task": 1},
    }


def split_files_into_sub_buckets(
    all_data_files_for_dedupe: List[Tuple[int, DataFile]],
    threshold: int = SUB_BUCKET_SPLIT_THRESHOLD,
) -> List[DataFileList]:
    """
    Split files into sub-buckets based on record count threshold.
    Each sub-bucket will contain files with total record count not exceeding the threshold.
    """
    if not all_data_files_for_dedupe:
        return []

    sub_buckets = []
    current_bucket = []
    current_bucket_record_count = 0

    for file_tuple in all_data_files_for_dedupe:
        sequence_number, data_file = file_tuple
        file_record_count = data_file.record_count

        # If adding this file would exceed threshold, start a new bucket
        if current_bucket and (
            current_bucket_record_count + file_record_count > threshold
        ):
            sub_buckets.append(current_bucket)
            current_bucket = []
            current_bucket_record_count = 0

        # Add file to current bucket
        current_bucket.append(file_tuple)
        current_bucket_record_count += file_record_count

    # Don't forget the last bucket
    if current_bucket:
        sub_buckets.append(current_bucket)

    logger.info(
        f"Split {len(all_data_files_for_dedupe)} files into {len(sub_buckets)} sub-buckets "
        f"with threshold {threshold}"
    )

    return sub_buckets


def estimate_memory_for_single_sub_bucket(
    sub_bucket_files: List[Tuple[int, DataFile]],
    identifier_fields: List[str],
) -> float:
    """
    Estimate memory required for a single sub-bucket.

    Args:
        sub_bucket_files: List of files in the sub-bucket
        identifier_fields: List of identifier field names

    Returns:
        Memory required in bytes for processing this sub-bucket
    """

    sub_bucket_record_count = get_total_record_from_iceberg_files(sub_bucket_files)

    produced_pos_memory_required = estimate_iceberg_pos_delete_additional_columns(
        ["file_path", "pos"], sub_bucket_record_count
    )
    download_pk_memory_required = estimate_fixed_hash_columns(
        SHA1_BYTE_PER_RECORD, sub_bucket_record_count, identifier_fields
    )
    memory_required_by_dedupe = (
        produced_pos_memory_required + download_pk_memory_required
    ) * PYARROW_AGGREGATE_MEMORY_MULTIPLIER

    memory_with_buffer = memory_required_by_dedupe * MEMORY_BUFFER_RATE

    logger.info(
        f"Sub-bucket memory estimation: {sub_bucket_record_count:,} records -> {memory_with_buffer:,} bytes"
    )

    return memory_with_buffer


def estimate_dedupe_memory(
    all_data_files_for_dedupe: List[Tuple[int, DataFile]],
    identifier_fields: List[str],
    sub_bucket_threshold_override: int = SUB_BUCKET_THRESHOLD,
    enable_sub_bucket: bool = True,
) -> Tuple[float, bool, Optional[List[DataFileList]], Optional[List[float]]]:
    """
    Estimate memory required for deduplication and determine if sub-bucketing is needed.

    Args:
        all_data_files_for_dedupe: List of data files for deduplication
        identifier_fields: List of identifier field names
        sub_bucket_threshold_override: Custom threshold for sub-bucketing (defaults to SUB_BUCKET_THRESHOLD)
        enable_sub_bucket: Whether to enable sub-bucketing (defaults to True)

    Returns:
        Tuple of (parent_memory_required, sub_bucket_enabled, sub_bucket_input_files, sub_bucket_memory_estimates)
    """
    dedupe_record_count = get_total_record_from_iceberg_files(all_data_files_for_dedupe)

    # Check if record count exceeds threshold for sub-bucketing AND if sub-bucketing is enabled
    sub_bucket_enabled = enable_sub_bucket and (
        dedupe_record_count > sub_bucket_threshold_override
    )
    sub_bucket_input_files = None
    sub_bucket_memory_estimates = None

    if sub_bucket_enabled:
        logger.info(
            f"Record count {dedupe_record_count} exceeds threshold {sub_bucket_threshold_override}. "
            f"Enabling sub-bucketing."
        )
        sub_bucket_input_files = split_files_into_sub_buckets(all_data_files_for_dedupe)

        # Calculate memory estimates for each sub-bucket
        sub_bucket_memory_estimates = []
        for i, sub_bucket in enumerate(sub_bucket_input_files):
            memory_estimate = estimate_memory_for_single_sub_bucket(
                sub_bucket, identifier_fields
            )
            sub_bucket_memory_estimates.append(memory_estimate)
            logger.info(
                f"Sub-bucket {i}: {len(sub_bucket)} files, "
                f"{get_total_record_from_iceberg_files(sub_bucket):,} records, "
                f"{memory_estimate:,} bytes memory"
            )

        # Parent task only schedules sub-tasks, so use minimal memory
        parent_memory = SUB_BUCKET_PARENT_TASK_MEMORY_RESERVED
        logger.info(
            f"Parent task memory reservation: {parent_memory} bytes, "
            f"Sub-bucket count: {len(sub_bucket_input_files)}"
        )
    else:
        # Normal processing - calculate memory based on all files
        produced_pos_memory_required = estimate_iceberg_pos_delete_additional_columns(
            ["file_path", "pos"], dedupe_record_count
        )
        download_pk_memory_required = estimate_fixed_hash_columns(
            SHA1_BYTE_PER_RECORD, dedupe_record_count, identifier_fields
        )
        memory_required_by_dedupe = (
            produced_pos_memory_required + download_pk_memory_required
        ) * PYARROW_AGGREGATE_MEMORY_MULTIPLIER

        logger.info(
            f"dedupe_record_count:{dedupe_record_count},"
            f"produced_pos_memory_require:{produced_pos_memory_required},"
            f"download_pk_memory_required:{download_pk_memory_required},"
            f"memory_required_by_dedupe:{memory_required_by_dedupe}"
        )

        parent_memory = memory_required_by_dedupe * MEMORY_BUFFER_RATE

    logger.info(
        f"sub_bucket_enabled:{sub_bucket_enabled},"
        f"with parent task memory:{parent_memory}"
    )

    return (
        parent_memory,
        sub_bucket_enabled,
        sub_bucket_input_files,
        sub_bucket_memory_estimates,
    )


def convert_options_provider(
    index: int,
    convert_input_files: ConvertInputFiles,
    identifier_fields: List[str] = None,
    sub_bucket_threshold_override: int = SUB_BUCKET_THRESHOLD,
    enable_sub_bucket: bool = True,
    **kwargs,
) -> Tuple[Dict[str, Any], ConvertInputFiles]:
    """
    Provide resource options for convert tasks and updated ConvertInputFiles with sub-bucket info.

    Args:
        index: Task index
        convert_input_files: Input files for conversion
        identifier_fields: List of identifier field names
        sub_bucket_threshold_override: Custom threshold for sub-bucketing (defaults to SUB_BUCKET_THRESHOLD)
        enable_sub_bucket: Whether to enable sub-bucketing (defaults to True)

    Returns:
        Tuple of (task_options, updated_convert_input_files)
    """
    applicable_data_files = convert_input_files.applicable_data_files
    applicable_equality_delete_files = (
        convert_input_files.applicable_equality_delete_files
    )
    all_data_files_for_dedupe = convert_input_files.all_data_files_for_dedupe

    total_memory_required = 0
    total_memory_required += BASE_MEMORY_BUFFER

    # Initialize sub-bucket parameters
    sub_bucket_enabled = False
    sub_bucket_input_files = None
    sub_bucket_memory_estimates = None

    if applicable_data_files and applicable_equality_delete_files:
        memory_requirement_for_convert_equality_deletes = (
            estimate_convert_remote_option_resources(
                applicable_data_files, applicable_equality_delete_files
            )
        )
        total_memory_required += memory_requirement_for_convert_equality_deletes

    if all_data_files_for_dedupe:
        # Pass enable_sub_bucket to estimate_dedupe_memory to handle sub-bucketing logic
        (
            memory_requirement_for_dedupe,
            sub_bucket_enabled,
            sub_bucket_input_files,
            sub_bucket_memory_estimates,
        ) = estimate_dedupe_memory(
            all_data_files_for_dedupe,
            identifier_fields,
            sub_bucket_threshold_override,
            enable_sub_bucket,
        )
        total_memory_required += memory_requirement_for_dedupe

    # Create updated ConvertInputFiles with sub-bucket information
    updated_convert_input_files = ConvertInputFiles.of(
        partition_value=convert_input_files.partition_value,
        all_data_files_for_dedupe=convert_input_files.all_data_files_for_dedupe,
        applicable_data_files=convert_input_files.applicable_data_files,
        applicable_equality_delete_files=convert_input_files.applicable_equality_delete_files,
        existing_position_delete_files=convert_input_files.existing_position_delete_files,
        sub_bucket_enabled=sub_bucket_enabled,
        sub_bucket_input_files=sub_bucket_input_files,
    )

    # Set sub-bucket memory estimates if available
    if sub_bucket_memory_estimates is not None:
        updated_convert_input_files.sub_bucket_memory_estimates = (
            sub_bucket_memory_estimates
        )

    task_options = _get_task_options(memory=total_memory_required)
    return task_options, updated_convert_input_files


def sub_hash_bucket_input_options_provider(
    sub_bucket_files: DataFileList, memory_estimate: float, **kwargs
) -> Dict[str, Any]:
    """
    Provide resource options for sub-bucket hash_bucket tasks.

    Args:
        sub_bucket_files: List of files for this sub-bucket
        memory_estimate: Memory estimate in bytes for this sub-bucket

    Returns:
        Task options dict with memory and custom resources for hash_bucket tasks
    """
    logger.info(
        f"Sub-bucket hash task options: {len(sub_bucket_files)} files, {memory_estimate:,} bytes memory"
    )

    # Use the memory estimate for this specific sub-bucket
    task_opts = {
        "memory": memory_estimate,
        "scheduling_strategy": "SPREAD",
        "max_retries": 3,
        "num_cpus": 1,
        "resources": {"hash_bucket_task": 1},  # Custom resource for hash bucket tasks
    }

    # List of possible botocore exceptions
    task_opts["retry_exceptions"] = [RetryableError]

    return task_opts
