from py4j.protocol import Py4JJavaError
from typing import Dict


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class ConcurrentModificationError(Exception):
    pass


def java_error_info(e: Py4JJavaError) -> Dict[str, str]:
    java_exception_str = e.java_exception.toString()
    java_exception_parts = java_exception_str.split(": ", 1)
    return {
        "stack_trace": e.java_exception.getStackTrace().toString(),
        "type": java_exception_parts[0],  # e.g. "java.lang.RuntimeException"
        "message": java_exception_parts[1]
    }
