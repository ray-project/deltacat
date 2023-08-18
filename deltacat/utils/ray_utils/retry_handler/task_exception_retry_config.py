from __future__ import annotations
from typing import List
from deltacat.utils.ray_utils.retry_handler.failures.aws_security_token_rate_exceeded_exception import \
    AWSSecurityTokenRateExceededException

from deltacat.utils.ray_utils.retry_handler.task_constants import (
    DEFAULT_MAX_RAY_REMOTE_TASK_RETRY_ATTEMPTS,
    DEFAULT_RAY_REMOTE_TASK_RETRY_INITIAL_BACK_OFF_IN_MS,
    DEFAULT_RAY_REMOTE_TASK_RETRY_BACK_OFF_FACTOR,
    DEFAULT_RAY_REMOTE_TASK_MEMORY_MULTIPLICATION_FACTOR,
    RAY_REMOTE_TASK_MEMORY_MULTIPLICATION_FACTOR_FOR_OUT_OF_MEMORY_ERROR
)

from deltacat.utils.ray_utils.retry_handler.failures.retryable_error import RetryableError


class TaskExceptionRetryConfig:
    """
    Determines how to handle and retry specific exceptions during task executions
    """

    def __init__(self, exception: Exception,
                 max_retry_attempts: int = DEFAULT_MAX_RAY_REMOTE_TASK_RETRY_ATTEMPTS,
                 initial_back_off_in_ms: int = DEFAULT_RAY_REMOTE_TASK_RETRY_INITIAL_BACK_OFF_IN_MS,
                 back_off_factor: int = DEFAULT_RAY_REMOTE_TASK_RETRY_BACK_OFF_FACTOR,
                 ray_remote_task_memory_multiplication_factor: float = DEFAULT_RAY_REMOTE_TASK_MEMORY_MULTIPLICATION_FACTOR,
                 is_throttling_exception: bool = False) -> None:
        self.exception = exception
        self.max_retry_attempts = max_retry_attempts
        self.initial_back_off_in_ms = initial_back_off_in_ms
        self.back_off_factor = back_off_factor
        self.ray_remote_task_memory_multiply_factor = ray_remote_task_memory_multiplication_factor
        self.is_throttling_exception = is_throttling_exception

    def getDefaultConfig() -> List[TaskExceptionRetryConfig]:
        return [TaskExceptionRetryConfig(exception=RetryableError(), is_throttling_exception=True),
                TaskExceptionRetryConfig(exception=AWSSecurityTokenRateExceededException(),
                                         ray_remote_task_memory_multiplication_factor=RAY_REMOTE_TASK_MEMORY_MULTIPLICATION_FACTOR_FOR_OUT_OF_MEMORY_ERROR)]
