from dataclasses import dataclass
from deltacat.utils.ray_utils.retry_handler.progress_notifier_interface import ProgressNotifierInterface
@dataclass
class TaskContext():
    """
    This class represents important info pertaining to the task that other interfaces like Straggler Detection
    can use to make decisions
    """
    def __init__(self, progress_notifier: ProgressNotifierInterface, timeout: float):
        self.progress_notifier = progress_notifier
        self.timeout = timeout
