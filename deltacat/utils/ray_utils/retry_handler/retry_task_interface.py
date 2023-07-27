from typing import List, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
import Exception

class RetryTaskInterface(Protocol):
    def init_tasks(self, task_infos: List[TaskInfoObject]) -> None:
        """
        Loads all tasks to check for retries if exception occurs
        """
        pass

    def should_retry(self, task: TaskInfoObject, exception: Exception) -> bool:
        """
        Given a task, determine whether it should be retried or not
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