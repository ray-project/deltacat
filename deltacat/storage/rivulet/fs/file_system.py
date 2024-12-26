from abc import abstractmethod
from contextlib import contextmanager
from io import FileIO
from typing import Protocol, Iterator
from deltacat.storage.rivulet.fs.input_file import InputFile, InputStream
from deltacat.storage.rivulet.fs.output_file import OutputFile, OutputStream


NormalizedPath = str
"""Alias for the normalized path generated upon resolution."""


class FileSystem(Protocol):
    """Interface for interacting with a given file system"""

    @abstractmethod
    def create(self, location: NormalizedPath) -> OutputStream:
        """Return a binary OutputStream that acts as a file-like output object."""

    @abstractmethod
    def exists(self, location: NormalizedPath) -> bool:
        """Return whether the location exists.

        Raises:
            PermissionError: If this has insufficient permissions to access the file at location.
        """

    @abstractmethod
    def open(self, location: NormalizedPath) -> InputStream:
        """Return a binary InputStream that acts as a file-like input object.

        Raises:
            FileNotFoundError: If the file does not exist at location.
            PermissionError: If this has insufficient permissions to access the file at location.
        """

    @abstractmethod
    def list_files(self, location: NormalizedPath) -> Iterator[InputFile]:
        """List the files at the given location
        TODO figure out the right abstractions for the object/rivulet
        """


class FSOutputFile(OutputFile):
    """OutputFile implementation using a filesystem"""

    def __init__(self, location: str, fs: FileSystem):
        super().__init__(location)
        self.fs = fs

    def exists(self) -> bool:
        return self.fs.exists(self._location)

    def to_input_file(self) -> InputFile:
        return FSInputFile(self._location, self.fs)

    @contextmanager
    def create(self) -> FileIO:
        with self.fs.create(self._location) as f:
            yield f


class FSInputFile(InputFile):
    """InputFile implementation using a filesystem"""

    def __init__(self, location: str, fs: FileSystem):
        super().__init__(location)
        self.fs = fs

    def exists(self) -> bool:
        return self.fs.exists(self._location)

    @contextmanager
    def open(self, seekable=False) -> InputStream:
        with self.fs.open(self._location) as f:
            yield f
