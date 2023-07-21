from abc import ABC, abstractmethod
from typing import Any

class StragglerDetectionInterface(ABC):

    @abstractmethod
    def is_straggler(self, task, task_context) -> bool:
        """
        Given all the info, returns whether this specific task is a straggler or not
        """
        pass