from typing import List, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
import Exception

class RetryTaskDefault(RetryTaskInterface):
    def __init__(self, max_retries: int):
        self.max_retries = max_retries
    def should_retry(self, task: TaskInfoObject, exception: Exception):
        """
        Given a task, determine whether it should be retried or not based on if its an instance of the RetryableError
        """
        if isinstance(exception, RetryableError):
            return True


    def get_wait_time(self, task: TaskInfoObject):
        """
        Determines the wait time between retries
        """
        pass

    def retry(self, task: TaskInfoObject, exception: Exception):
        """
        Executes retry behavior for the given exception
        """
        task_id = task.task_id
        if self.should_retry(task, exception):
            wait_time = self.get_wait_time(task)
            time.sleep(wait_time)
            #increase retry count here
            self.execute_task(task)