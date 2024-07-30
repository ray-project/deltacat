from typing import Any, Protocol
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject
from deltacat.utils.ray_utils.retry_handler.task_context import TaskContext


class StragglerDetectionInterface(Protocol):
    """
    Using TaskContext, handles the client-side implementation for straggler detection
    """

    def is_straggler(self, task: TaskInfoObject, task_context: TaskContext) -> bool:
        """
        Given all the info, returns whether this specific task is a straggler or not
        """
        pass
