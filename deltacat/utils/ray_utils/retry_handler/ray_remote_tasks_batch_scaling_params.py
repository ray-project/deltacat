from deltacat.utils.ray_utils.retry_handler.task_constants import DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BATCH_SIZE_MULTIPLICATIVE_DECREASE_FACTOR, DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BACK_OFF_IN_MS, DEFAULT_RAY_REMOTE_TASK_BATCH_POSITIVE_FEEDBACK_BATCH_SIZE_ADDITIVE_INCREASE
from dataclasses import dataclass

class RayRemoteTasksBatchScalingParams(BatchScalingStrategy):
    """
    Represents the batch scaling params of the Ray remote tasks
    need to add constants that this file refers to
    """
    def __init__(self,
                 initial_batch_size: int,
                 negative_feedback_back_off_in_ms: int = DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BACK_OFF_IN_MS,
                 positive_feedback_batch_size_additive_increase: int = DEFAULT_RAY_REMOTE_TASK_BATCH_POSITIVE_FEEDBACK_BATCH_SIZE_ADDITIVE_INCREASE,
                 negative_feedback_batch_size_multiplicative_decrease_factor: int = DEFAULT_RAY_REMOTE_TASK_BATCH_NEGATIVE_FEEDBACK_BATCH_SIZE_MULTIPLICATIVE_DECREASE_FACTOR):
        self.initial_batch_size = initial_batch_size
        self.negative_feedback_back_off_in_ms = negative_feedback_back_off_in_ms
        self.positive_feedback_batch_size_additive_increase = positive_feedback_batch_size_additive_increase
        self.negative_feedback_batch_size_multiplicative_decrease_factor = negative_feedback_batch_size_multiplicative_decrease_factor
        self.current_batch_size = initial_batch_size

    def get_batch_size(self) -> int:
        return self.current_batch_size

    def increase_batch_size(self, current_size: int) -> int:
        self.current_batch_size = current_size + self.positive_feedback_batch_size_additive_increase
        return self.current_batch_size

    def decrease_batch_size(self, current_size: int) -> int:
        self.current_batch_size = int(current_size * self.negative_feedback_batch_size_multiplicative_decrease_factor)
        return self.current_batch_size