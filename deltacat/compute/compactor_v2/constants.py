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
# Default is the number of vCPUs in about 168
# r5.8xlarge EC2 instances.
MAX_PARALLELISM = 5367
