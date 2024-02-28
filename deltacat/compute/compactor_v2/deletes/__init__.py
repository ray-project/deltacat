from abc import ABC, ABCMeta, abstractmethod, abstractclassmethod, abstractstaticmethod, abstractproperty

class DeleteStrategy(ABC):
    pass

class PositionalDeleteStrategy(DeleteStrategy):
    pass


class EqualityDeleteStrategy(DeleteStrategy):
    pass