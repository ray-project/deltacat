from dataclasses import dataclass
from typing import Any, Callable, List
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskExceptionRetryConfig
from deltacat.utils.ray_utils.retry_handler.task_options import RayRemoteTaskOptions

@dataclass
Class TaskInfoObject:
    def __init__(self,
                 task_callable: Callable[[Any], [Any],
                 task_input: Any,
                 ray_remote_task_options: RayRemoteTaskOptions = RayRemoteTaskOptions(),
                 exception_retry_strategy_configs: List[TaskExceptionRetryConfig]):
        self.task_callable = task_callable
        self.task_input = task_input
        self.ray_remote_task_options = ray_remote_task_options
        self.exception_retry_strategy_configs = exception_retry_strategy_configs
        self.num_of_attempts = 0
