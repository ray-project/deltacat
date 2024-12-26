import io
from abc import ABC, abstractmethod
from typing import Protocol


class InputStream(Protocol):
    """A protocol with a subset of IOBase for file-like input objects"""

    @abstractmethod
    def read(self, size: int = -1) -> bytes: ...

    @abstractmethod
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int: ...

    @abstractmethod
    def tell(self) -> int: ...

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> "InputStream": ...

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...


class InputFile(ABC):
    """Abstraction for interacting with input files"""

    def __init__(self, location: str):
        self._location = location

    @property
    def location(self) -> str:
        return self._location

    @abstractmethod
    def exists(self) -> bool:
        """Return whether the location exists.

        Raises:
            PermissionError: If this has insufficient permissions to access the file at location.
        """

    @abstractmethod
    def open(self) -> InputStream:
        """Return a file-like object for input

        Raises:
            FileNotFoundError: If the file does not exist at self.location.
            PermissionError: If this has insufficient permissions to access the file at location.
        """
