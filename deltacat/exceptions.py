from py4j.protocol import Py4JJavaError
from typing import Dict


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class ConcurrentModificationError(Exception):
    pass
