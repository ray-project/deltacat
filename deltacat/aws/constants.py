from deltacat.utils.common import env_integer
from typing import List


BOTO_MAX_RETRIES = env_integer("BOTO_MAX_RETRIES", 15)
TIMEOUT_ERROR_CODES: List[str] = ["ReadTimeoutError", "ConnectTimeoutError"]
