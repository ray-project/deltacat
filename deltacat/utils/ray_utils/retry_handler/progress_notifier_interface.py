from typing import List, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
class ProgressNotifierInterface(Protocol):
    """
    Interface for client injected progress notification system.
    """
    def has_heartbeat(self, task_info: TaskInfoObject) -> bool:
        """
        Sends progress of current task to parent task
        """
        pass
    def send_heartbeat(self, parent_task_info: TaskInfoObject) -> bool:
        """
        Tells parent task if the current task has a heartbeat or not
        """
        pass

