from abc import ABC, abstractmethod
from typing import List

class BatchScalingInterface(ABC):
    """
    Interface for a generic batch scaling that the client can provide.
    """
    """
    Loads all tasks to be executed for retry and straggler detection
    """
    def init_tasks(self, task_infos):
        pass
    """
    Gets the next batch of x size to execute on 
    """
    def next_batch(self, task_info) -> List:
        pass
    """
    Returns true if there are tasks remaining in the overall List of tasks
    """
    def has_next_batch(self, running_tasks) -> bool:
        pass
