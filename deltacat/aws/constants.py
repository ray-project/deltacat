from typing import List

from deltacat.utils.common import env_integer, env_string

DAFT_MAX_S3_CONNECTIONS_PER_FILE = env_integer("DAFT_MAX_S3_CONNECTIONS_PER_FILE", 8)
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 5)
TIMEOUT_ERROR_CODES: List[str] = ["ReadTimeoutError", "ConnectTimeoutError"]
AWS_REGION = env_string("AWS_REGION", "us-east-1")

# Metric Names
DOWNLOAD_MANIFEST_ENTRY_METRIC_PREFIX = "download_manifest_entry"
UPLOAD_SLICED_TABLE_METRIC_PREFIX = "upload_sliced_table"
