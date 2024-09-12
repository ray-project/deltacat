import botocore
from typing import Set
from daft.exceptions import DaftTransientError
from deltacat.utils.common import env_integer, env_string


DAFT_MAX_S3_CONNECTIONS_PER_FILE = env_integer("DAFT_MAX_S3_CONNECTIONS_PER_FILE", 8)
DEFAULT_FILE_READ_TIMEOUT_MS = env_integer(
    "DEFAULT_FILE_READ_TIMEOUT_MS", 300_000
)  # 5 mins
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 5)
BOTO_TIMEOUT_ERROR_CODES: Set[str] = {"ReadTimeoutError", "ConnectTimeoutError"}
BOTO_THROTTLING_ERROR_CODES: Set[str] = {"Throttling", "SlowDown"}
RETRYABLE_TRANSIENT_ERRORS = (
    OSError,
    botocore.exceptions.ConnectionError,
    botocore.exceptions.HTTPClientError,
    botocore.exceptions.NoCredentialsError,
    botocore.exceptions.ConnectTimeoutError,
    botocore.exceptions.ReadTimeoutError,
    DaftTransientError,
)
AWS_REGION = env_string("AWS_REGION", "us-east-1")
UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY = env_integer(
    "UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY", 10 * 60
)
UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY = env_integer(
    "UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY", 30 * 60
)
DOWNLOAD_MANIFEST_ENTRY_RETRY_STOP_AFTER_DELAY = env_integer(
    "DOWNLOAD_MANIFEST_ENTRY_RETRY_STOP_AFTER_DELAY", 30 * 60
)
