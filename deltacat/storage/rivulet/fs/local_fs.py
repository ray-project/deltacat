import errno
import os
from contextlib import contextmanager
from typing import Iterator, IO

from deltacat.storage.rivulet.fs.file_system import (
    FileSystem,
    FSInputFile,
    NormalizedPath,
)
from deltacat.storage.rivulet.fs.input_file import InputFile, InputStream
from deltacat.storage.rivulet.fs.output_file import OutputStream


class FSInputStream(InputStream):
    """InputStream implementation for local disk."""

    def __init__(self, stream: IO):
        self.stream: IO = stream

    @property
    def closed(self):
        return self.stream.closed

    def read(self, size: int = -1) -> bytes:
        return self.stream.read(size)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self.stream.seek(offset, whence)

    def tell(self) -> int:
        return self.stream.tell()

    def close(self) -> None:
        self.stream.close()

    def __enter__(self) -> "FSInputStream":
        return self.stream.__enter__()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stream.__exit__(exc_type, exc_value, traceback)


class FSOutputStream(OutputStream):
    """OutputStream implementation for local disk."""

    def __init__(self, stream: IO):
        self.stream: IO = stream

    @property
    def closed(self):
        return self.stream.closed

    def write(self, b: bytes) -> int:
        return self.stream.write(b)

    def close(self) -> None:
        return self.stream.close()

    def __enter__(self) -> "FSOutputStream":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stream.__exit__(exc_type, exc_value, traceback)


class LocalFS(FileSystem):
    """FileSystem implementation for local disk"""

    def exists(self, location: NormalizedPath) -> bool:
        return os.path.isfile(location)

    @contextmanager
    def create(self, location: NormalizedPath) -> OutputStream:
        os.makedirs(os.path.dirname(location), exist_ok=True)
        with open(location, "wb") as file_obj:
            yield FSOutputStream(file_obj)

    @contextmanager
    def open(self, location: NormalizedPath) -> InputStream:
        if not self.exists(location):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), location)
        with open(location, "rb") as file_obj:
            yield FSInputStream(file_obj)

    def list_files(self, location: NormalizedPath) -> Iterator[InputFile]:
        if not os.path.isdir(location):
            return
        for entry in os.scandir(location):
            if entry.is_file():
                yield FSInputFile(entry.path, self)
