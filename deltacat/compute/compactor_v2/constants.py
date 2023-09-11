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
MAX_SIZE_OF_RECORD_BATCH_IN_GIB = 2 * 1024 * 1024 * 1024

# Whether to drop duplicates during merge.
DROP_DUPLICATES = True

# PARQUET to PYARROW inflation multiplier
# This is the observed upper bound inflation for parquet
# size in metadata to pyarrow table size.
PARQUET_TO_PYARROW_INFLATION = 4
