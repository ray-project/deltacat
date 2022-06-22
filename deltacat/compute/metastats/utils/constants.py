# Default to use r5.8xlarge instance type for stats collection cluster
STATS_CLUSTER_R5_INSTANCE_TYPE = 8
# Using R5 instance type, 8GiB memory is available per cpu
R5_MEMORY_PER_CPU = 8
# Default to use r5.8xlarge instance type for stats collection cluster
DEFAULT_CPUS_PER_INSTANCE_R5_8XLARGE = 32
# memory reserved for head node object store
HEAD_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO = 0.3
# memory reserved for worker node object store
WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO = 0.1
# each cpu should not be processing more than this number of files to avoid unreasonable S3 I/O latency
MANIFEST_FILE_COUNT_PER_CPU = 200
# MAX_WORKER_MULTIPLIER * min_workers = max_workers to determine max workers based on min workers given
MAX_WORKER_MULTIPLIER = 2
# default trace id used for metastats collection triggered without trace id
DEFAULT_JOB_RUN_TRACE_ID = "0"
