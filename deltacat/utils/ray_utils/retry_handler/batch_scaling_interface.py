from typing import List, Any, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
class BatchScalingInterface(Protocol):
    """
    Interface for a generic batch scaling that the client can provide.
    """
    def init_tasks(self, initial_batch_size: int, max_batch_size: int, min_batch_size: int, task_infos: List[Any]) -> None:
        """
        Loads all tasks to be executed for retry and straggler detection
        """
        pass
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
    def mark_task_complete(self, task_info: TaskInfoObject) -> List:
        """
        If the task has been completed, mark some field of it as true
        so we know what tasks are completed and what need to be executed
        """
        pass

