# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
import pyarrow as pa
from boto3.resources.base import ServiceResource

TEST_S3_RCF_BUCKET_NAME = "test-compaction-artifacts-bucket"
# REBASE  src = spark compacted table to create an initial version of ray compacted table
BASE_TEST_SOURCE_NAMESPACE = "source_test_namespace"
BASE_TEST_SOURCE_TABLE_NAME = "source_test_table"
BASE_TEST_SOURCE_TABLE_VERSION = "1"

BASE_TEST_DESTINATION_NAMESPACE = "destination_test_namespace"
BASE_TEST_DESTINATION_TABLE_NAME = "destination_test_table"
BASE_TEST_DESTINATION_TABLE_VERSION = "1"

REBASING_NAMESPACE = "compacted"
REBASING_NAME_SUFFIX = "_compacted"

RAY_COMPACTED_NAMESPACE = "compacted_ray"
RAY_COMPACTED_NAME_SUFFIX = "_compacted_ray"


HASH_BUCKET_COUNT: int = 1

MAX_RECORDS_PER_FILE: int = 1

DEFAULT_NUM_WORKERS = 1
DEFAULT_WORKER_INSTANCE_CPUS = 1

UTC_ISO_8601_FORMAT_WITHOUT_MILLIS = "%Y-%m-%dT%H:%M:%SZ"  # '2018-09-05T14:09:03Z'