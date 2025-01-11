from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject


class TaskExecutionError:
    """
    An error class that denotes the Ray Remote Task Execution Failure
    """

    def __init__(self, exception: Exception, task_info: TaskInfoObject) -> None:
        self.exception = exception
        self.task_info = task_info
