import botocore
from typing import Set
from enum import Enum

from deltacat.utils.common import env_integer, env_string


class S3ErrorCodes(str, Enum):
    # S3 error codes - see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
    READ_TIMEOUT_ERROR = "ReadTimeoutError"
    CONNECT_TIMEOUT_ERROR = "ConnectTimeoutError"
    REQUEST_TIME_TOO_SKEWED_ERROR = "RequestTimeTooSkewed"
    SLOW_DOWN_ERROR = "SlowDown"

    @classmethod
    def get_timeout_error_codes(cls):
        return [cls.READ_TIMEOUT_ERROR, cls.CONNECT_TIMEOUT_ERROR]

    @classmethod
    def get_read_table_retryable_error_codes(cls):
        return [cls.READ_TIMEOUT_ERROR, cls.CONNECT_TIMEOUT_ERROR]

    @classmethod
    def get_upload_table_retryable_error_codes(cls):
        return [cls.REQUEST_TIME_TOO_SKEWED_ERROR, cls.SLOW_DOWN_ERROR]


DAFT_MAX_S3_CONNECTIONS_PER_FILE = env_integer("DAFT_MAX_S3_CONNECTIONS_PER_FILE", 8)
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
