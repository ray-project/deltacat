from abc import ABC, abstractmethod

class ProgressNotifierInterface(ABC):
    """
    Gets progress message regarding current task
    """
    @abstractmethod
    def get_progress(self, task):
        pass

    """
    Tells parent task if the current task has a heartbeat or not
    """
    @abstractmethod
    def has_heartbeat(self, task) -> bool:
        pass

    """
    Sends progress of current task to parent task 
    """
    @abstractmethod
    def send_progress(self, task):
        pass