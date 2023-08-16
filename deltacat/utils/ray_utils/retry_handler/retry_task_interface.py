from typing import List, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from builtins import Exception

class RetryTaskInterface(Protocol):
    def should_retry(self, exception: Exception) -> bool:
        """
        Given a task, determine whether it should be retried or not based on if its an instance of the RetryableError
        """
        pass

    def get_wait_time(self, task: TaskInfoObject) -> int:
        """
        Determines the wait time between retries
        """
        pass

    def retry(self, task: TaskInfoObject, exception: Exception) -> None:
        """
        Executes retry behavior for the given exception
        """
        pass