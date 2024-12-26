from abc import ABC, abstractmethod
from typing import Protocol

from deltacat.storage.rivulet.fs.input_file import InputFile


class OutputStream(Protocol):  # pragma: no cover
    """A protocol with a subset of IOBase for file-like output objects"""

    @abstractmethod
    def write(self, b: bytes) -> int:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def __enter__(self) -> "OutputStream":
        ...

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...


class OutputFile(ABC):
    """Abstraction for interacting with output files"""

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
    def to_input_file(self) -> InputFile:
        """Return an InputFile for this output file's location"""

    @abstractmethod
    def create(self) -> OutputStream:
        """Return a file-like object for output

        TODO: overwrite protection (FileExistsError?)
        Raises:
            PermissionError: If this has insufficient permissions to access the file at location.
        """
