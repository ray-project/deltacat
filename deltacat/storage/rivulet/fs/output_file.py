from abc import ABC, abstractmethod
from contextlib import contextmanager
import posixpath
from typing import Protocol

from pyarrow.fs import FileSystem, FileType

from deltacat.storage.rivulet.fs.input_file import FSInputFile, InputFile


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


class FSOutputFile(OutputFile):
    def __init__(self, location: str, fs: FileSystem):
        self._location = location
        self.fs = fs

    def exists(self) -> bool:
        file_info = self.fs.get_file_info(self._location)
        return file_info.type != FileType.NotFound

    def to_input_file(self) -> "FSInputFile":
        return FSInputFile(self._location, self.fs)

    @contextmanager
    def create(self):
        """Create and open the file for writing."""
        try:
            parent_dir = posixpath.dirname(self._location)
            if parent_dir:  # Check if there's a parent directory to create
                self.fs.create_dir(parent_dir, recursive=True)

            with self.fs.open_output_stream(self._location) as output_stream:
                yield output_stream
        except Exception as e:
            raise IOError(f"Failed to create or write to file '{self._location}': {e}")
