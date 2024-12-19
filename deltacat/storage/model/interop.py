from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

T = TypeVar("T")
U = TypeVar("U")


class ModelMapper(ABC, Generic[T, U]):
    @staticmethod
    @abstractmethod
    def map(obj: Optional[T], *args, **kwargs) -> Optional[U]:
        pass

    @staticmethod
    @abstractmethod
    def unmap(obj: Optional[U], **kwargs) -> Optional[T]:
        pass


class OneWayModelMapper(ABC, Generic[T, U]):
    @staticmethod
    @abstractmethod
    def map(obj: Optional[T], **kwargs) -> Optional[U]:
        pass
