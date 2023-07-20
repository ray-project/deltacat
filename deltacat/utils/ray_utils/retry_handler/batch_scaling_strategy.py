from abc import ABC, abstractmethod

class BatchScalingStrategy(ABC):
    """
    Interface for a generic batch scaling that the client can provide.
    """

    @abstractmethod
    def increase_batch_size(self, current_size: int) -> int:
        pass

    @abstractmethod
    def decrease_batch_size(self, current_size: int) -> int:
        pass

    @abstractmethod
    def get_batch_size(self) -> int:
        pass
