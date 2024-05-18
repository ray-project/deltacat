from typing import List, Set

from botocore.exceptions import NoCredentialsError, ReadTimeoutError

from deltacat.utils.common import env_integer, env_string

DAFT_MAX_S3_CONNECTIONS_PER_FILE = env_integer("DAFT_MAX_S3_CONNECTIONS_PER_FILE", 8)
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 5)
TIMEOUT_ERROR_CODES: List[str] = ["ReadTimeoutError", "ConnectTimeoutError"]
RETRYABLE_PUT_OBJECT_ERROR_CODES: Set[str] = {"Throttling", "SlowDown"}
RETRYABLE_TRANSIENT_ERRORS = (NoCredentialsError, ReadTimeoutError, ConnectionError)
AWS_REGION = env_string("AWS_REGION", "us-east-1")
RETRY_STOP_AFTER_DELAY = env_integer("RETRY_STOP_AFTER_DELAY", 10 * 60)
