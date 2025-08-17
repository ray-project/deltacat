from typing import Set
from deltacat.utils.common import env_integer, env_string


DAFT_MAX_S3_CONNECTIONS_PER_FILE = env_integer("DAFT_MAX_S3_CONNECTIONS_PER_FILE", 8)
BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 5)
BOTO_TIMEOUT_ERROR_CODES: Set[str] = {"ReadTimeoutError", "ConnectTimeoutError"}
BOTO_THROTTLING_ERROR_CODES: Set[str] = {"Throttling", "SlowDown"}
AWS_REGION = env_string("AWS_REGION", "us-east-1")
