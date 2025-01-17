from deltacat.utils.common import env_bool, env_integer

TOTAL_BYTES_IN_SHA1_HASH = 20

PK_DELIMITER = "L6kl7u5f"

MAX_RECORDS_PER_COMPACTED_FILE = 4_000_000

# The maximum amount of delta bytes allowed in a batch.
# A single task will not process more than these many bytes
# unless a single manifest entry (non-parquet) or single row
# group (parquet) is bigger than this size.
MIN_DELTA_BYTES_IN_BATCH = 5_000_000_000

# The total number of files that can be processed in a
# batch. Hence, if there are tiny files, this value can be
# limited so that enough parallelism can be attained.
MIN_FILES_IN_BATCH = float("inf")

# The average record size in a table.
AVERAGE_RECORD_SIZE_BYTES = 1000

# Maximum parallelism for the tasks at each BSP step.
# Default is the number of vCPUs in about 128
# r5.8xlarge EC2 instances.
TASK_MAX_PARALLELISM = 4096

# The percentage of memory that needs to be allocated
# as buffer. This value will ensure the job doesn't run out
# of memory by considering buffer for uncertainities.
TOTAL_MEMORY_BUFFER_PERCENTAGE = 30

# The total size of records that will be hash bucketed at once
# Since, sorting is nlogn, we ensure that is not performed
# on a very large dataset for best performance.
MAX_SIZE_OF_RECORD_BATCH_IN_GIB = env_integer(
    "MAX_SIZE_OF_RECORD_BATCH_IN_GIB", 2 * 1024 * 1024 * 1024
)

# Whether to drop duplicates during merge.
DROP_DUPLICATES = True

# PARQUET to PYARROW inflation multiplier
# This is the observed upper bound inflation for parquet
# size in metadata to pyarrow table size.
PARQUET_TO_PYARROW_INFLATION = 4

# Maximum size of the parquet metadata
MAX_PARQUET_METADATA_SIZE = 100_000_000  # 100 MB

# By default, copy by reference is enabled
DEFAULT_DISABLE_COPY_BY_REFERENCE = False

# Metric Names
# Time taken for a hash bucket task
HASH_BUCKET_TIME_IN_SECONDS = "hash_bucket_time"

# Hash bucket success count
HASH_BUCKET_SUCCESS_COUNT = "hash_bucket_success_count"

# Hash bucket failure count
HASH_BUCKET_FAILURE_COUNT = "hash_bucket_failure_count"

# Time taken for a merge task
MERGE_TIME_IN_SECONDS = "merge_time"

# Merge success count
MERGE_SUCCESS_COUNT = "merge_success_count"

# Merge failure count
MERGE_FAILURE_COUNT = "merge_failure_count"

# Metric prefix for discover deltas
DISCOVER_DELTAS_METRIC_PREFIX = "discover_deltas"

# Metric prefix for prepare deletes
PREPARE_DELETES_METRIC_PREFIX = "prepare_deletes"

# Metric prefix for compact partition method
COMPACT_PARTITION_METRIC_PREFIX = "compact_partition"

# Number of rounds to run hash/merge for a single
# partition. (For large table support)
DEFAULT_NUM_ROUNDS = 1

# Whether to perform sha1 hashing when required to
# optimize memory. For example, hashing is always
# required for bucketing where it's not mandatory
# when dropping duplicates. Setting this to True
# will disable sha1 hashing in cases where it isn't
# mandatory. This flag is False by default.
SHA1_HASHING_FOR_MEMORY_OPTIMIZATION_DISABLED = env_bool(
    "SHA1_HASHING_FOR_MEMORY_OPTIMIZATION_DISABLED", False
)
