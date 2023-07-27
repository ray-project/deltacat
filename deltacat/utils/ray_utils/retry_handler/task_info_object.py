from dataclasses import dataclass
from typing import Any, Callable, List
from deltacat.utils.ray_utils.retry_handler.task_exception_retry_config import TaskExceptionRetryConfig
from deltacat.utils.ray_utils.retry_handler.task_options import RayRemoteTaskOptions

@dataclass
class TaskInfoObject:
    """
    Dataclass holding important fields representing the Task as an object
    """
    def __init__(self,
                 task_callable: Callable[[Any], [Any]],
                 task_input: Any,
                 ray_remote_task_options: RayRemoteTaskOptions = RayRemoteTaskOptions(),
                 task_exception_retry_config: List[TaskExceptionRetryConfig]):
        self.task_callable = task_callable
        self.task_input = task_input
        self.ray_remote_task_options = ray_remote_task_options
        self.task_exception_retry_config = task_exception_retry_config
        self.num_of_attempts = 0
