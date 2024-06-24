class RayRemoteTaskExecutionError:
    """
    An error class that denotes the Ray Remote Task Execution Failure
    """
    def __init__(self, exception: Exception, ray_remote_task_info: RayRemoteTaskInfo) -> None:
        self.exception = exception
        self.ray_remote_task_info = ray_remote_task_info