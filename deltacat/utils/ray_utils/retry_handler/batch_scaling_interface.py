from typing import List, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
class BatchScalingInterface(Protocol):
    """
    Interface for a generic batch scaling that the client can provide.
    """
    def has_next_batch(self) -> bool:
        """
        Returns true if there are tasks remaining in the overall List of tasks to create a new batch
        """
        pass
    def next_batch(self, task_info: TaskInfoObject) -> List:
        """
        Gets the next batch to execute on
        """
        pass
    def mark_task_complete(self, task_info: TaskInfoObject) -> None:
        """
        If the task has been completed, mark some field of it as true
        so we know what tasks are completed and what need to be executed
        """
        pass

    def mark_batch_failed(self, task_info: TaskInfoObject) -> None:
        """
        If the task returns the exception that was caught, we mark the task as failed
        :param task_info:
        :return:
        """
        pass

    def is_task_completed(self, task_id: int) -> bool:
        """
        Given a task ID, returns whether the task has been completed or not
        """
        pass
