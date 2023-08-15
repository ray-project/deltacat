from typing import List, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from builtins import Exception
from deltacat.utils.ray_utils.retry_handler.retry_task_interface import RetryTaskInterface
from deltacat.utils.ray_utils.retry_handler.failures.retryable_error import RetryableError


class RetryTaskDefault(RetryTaskInterface):
    def __init__(self, max_retries: int):
        self.max_retries = max_retries
        self.attempts = {}

    def should_retry(self, task: TaskInfoObject, exception: Exception):
        """
        Given a task, determine whether it should be retried or not based on if its an instance of the RetryableError
        """
        if isinstance(exception, RetryableError):
            return True

    def get_wait_time(self, task: TaskInfoObject):
        """
        Configures an exponential backoff strategy
        """

        attempt = self.attempts.get(task.task_id, 0)
        return 2 ** attempt

    def retry(self, task: TaskInfoObject, exception: Exception):
        """
        Executes retry behavior for the given exception
        """
        task_id = task.task_id
        if self.should_retry(task, exception):
            self.attempts[task_id] = self.attempts.get(task_id, 0) + 1
            if self.attempts[task_id] > self.max_retries:
                raise Exception("Max retry attempts exceeded")
            wait_time = self.get_wait_time(task)
