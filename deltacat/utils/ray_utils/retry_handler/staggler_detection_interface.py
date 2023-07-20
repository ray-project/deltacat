from abc import ABC, abstractmethod
from typing import Any

class StragglerDetectionInterface(ABC):
    @abstractmethod
    def calc_timeout_val(self, task: Any) -> float:
        """
        Determines timeout value for ray.wait() parameter based on client config
        """
        pass

    @abstractmethod
    def is_straggler(self, task: Any) -> bool:
        """
        Given all the info, returns whether this specific task is a straggler or not
        """
        pass

    @abstractmethod
    def get_progress(self, task: Any) -> float:
        """

        :param task: takes in a task
        :return: returns progress output of child task
        """