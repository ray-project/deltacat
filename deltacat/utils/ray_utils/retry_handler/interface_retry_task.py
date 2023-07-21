from abc import ABC, abstractmethod
class RetryTaskInterface(ABC):
    @abstractmethod
    def init_tasks(self, task_infos):
        """
        Loads all tasks to check for retries if exception
        :param task_infos:
        :return: List of tasks
        """
        pass
    @abstractmethod
    def should_retry(self, task) -> bool:
        """
        Given a task, determine whether it can be retried or not
        :param task:
        :return: True or False
        """
        pass
    @abstractmethod
    def get_wait_time(self, task):
        """
        Wait time between retries
        :param task:
        :return:
        """
        pass
    @abstractmethod
    def retry(self, task):
        """
        Executes retry behavior for the exception
        :param task:
        :return:
        """
        pass