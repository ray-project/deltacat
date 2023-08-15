from typing import List, Any
from deltacat.utils.ray_utils.retry_handler.batch_scaling_interface import BatchScalingInterface
import math
from deltacat.utils.ray_utils.retry_handler.task_info_object import TaskInfoObject


class AIMDBasedBatchScalingStrategy(BatchScalingInterface):
    """
    Default batch scaling parameters for if the client does not provide their own batch_scaling parameters
    """

    def __init__(self,
                 task_infos: List[TaskInfoObject],
                 initial_batch_size: int,
                 max_batch_size: int,
                 min_batch_size: int,
                 additive_increase: int,
                 multiplicative_decrease: float):
        self.task_infos = task_infos
        self.batch_index = 0
        self.batch_size = initial_batch_size
        self.max_batch_size = max_batch_size
        self.min_batch_size = min_batch_size
        self.additive_increase = additive_increase
        self.multiplicative_decrease = multiplicative_decrease

        # dictionary
        self.task_completion_status: Dict[str, bool] = {task.task_id: False for task in self.task_infos}

    def has_next_batch(self) -> bool:
        """
        Returns the list of tasks included in the next batch of whatever size based on AIMD
        """
        return self.batch_index < len(self.task_infos)

    def next_batch(self) -> List[TaskInfoObject]:
        """
        If there are no more tasks to execute that can not create a batch, return False
        """
        batch_end = math.floor(min(self.batch_index + self.batch_size, len(self.task_infos)))
        print("in next_batch")
        print(self.batch_index)
        print(batch_end)
        batch = self.task_infos[self.batch_index:batch_end]
        self.batch_index = batch_end
        return batch

    def mark_task_complete(self, task_id: int):
        self.task_completion_status[task_id] = True
        if (self.batch_size + self.additive_increase) > self.max_batch_size:
            self.batch_size = self.max_batch_size
        else:
            self.batch_size = self.batch_size + self.additive_increase

    def mark_task_failed(self, task_id: int):
        self.task_completion_status[task_id] = False
        if (self.batch_size * self.multiplicative_decrease) < self.min_batch_size:
            self.batch_size = self.min_batch_size
        else:
            self.batch_size = math.floor(self.batch_size * self.multiplicative_decrease)

    def is_task_completed(self, task_id: int) -> bool:
        """
        Returns True if the task is completed, otherwise returns False.
        """
        return self.task_completion_status.get(task_id, False)
