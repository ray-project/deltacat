from deltacat.utils.ray_utils.retry_handler.task_constants import DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BATCH_SIZE_MULTIPLICATIVE_DECREASE_FACTOR, DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BACK_OFF_IN_MS, DEFAULT_RAY_REMOTE_TASK_BATCH_POSITIVE_FEEDBACK_BATCH_SIZE_ADDITIVE_INCREASE
from dataclasses import dataclass

class RayRemoteTasksBatchScalingParams(BatchScalingStrategy):
    """
    Represents the batch scaling params of the Ray remote tasks
    need to add constants that this file refers to
    """
    def __init__(self,
                 straggler_detection: StragglerDetectionInterface):
        self.straggler_detection = straggler_detection

    def init_tasks(self, task_infos):
        pass

    def next_batch(self, task_info) -> List:
        pass

    def has_next_batch(self, running_tasks) -> bool:
        pass