from typing import List, Any
from deltacat.utils.ray_utils.retry_handler.batch_scaling_interface import BatchScalingInterface
class AIMDBasedBatchScalingStrategy(BatchScalingInterface):
    """
    Default batch scaling parameters for if the client does not provide their own batch_scaling parameters
    """
    def __init__(self, additive_increase: int, multiplicative_decrease: float):
        self.task_infos = []
        self.batch_index = 0
        self.batch_size = None
        self.max_batch_size = None
        self.min_batch_size = None
        self.additive_increase = additive_increase
        self.multiplicative_decrease = multiplicative_decrease
    def init_tasks(self, initial_batch_size: int, max_batch_size: int, min_batch_size: int, task_infos: List[TaskInfoObject])-> None:
        """
        Setup AIMD scaling for the batches as the default
        """
        self.task_infos = task_infos
        self.batch_size = initial_batch_size
        self.max_batch_size = max_batch_size
        self.min_batch_size = min_batch_size


    def has_next_batch(self) -> bool:
        """
        Returns the list of tasks included in the next batch of whatever size based on AIMD
        """
        return self.batch_index < len(self.task_infos)


    def next_batch(self) -> List[TaskInfoObject]:
        """
        If there are no more tasks to execute that can not create a batch, return False
        """
        batch_end = min(self.batch_index + self.batch_size, len(self.task_infos))
        batch = self.task_infos[self.batch_index:batch_end]
        self.batch_index = batch_end
        return batch

    def mark_task_complete(self, task_info: TaskInfoObject):
        task_info.completed = True

    def increase_batch_size(self):
        self.batch_size = min(self.batch_size + self.additive_increase, self.max_batch_size)


    def decrease_batch_size(self):
        self.batch_size = max(self.batch_size * self.multiplicative_decrease, self.min_batch_size)