from contextlib import contextmanager
from typing import Iterator

from fsspec import AbstractFileSystem

from deltacat.storage.rivulet.fs.file_system import FileSystem, FSInputFile, NormalizedPath
from deltacat.storage.rivulet.fs.input_file import InputFile, InputStream
from deltacat.storage.rivulet.fs.local_fs import FSInputStream, FSOutputStream
from deltacat.storage.rivulet.fs.output_file import OutputStream


class FsspecFileSystem(FileSystem):
    """Adapter FS implementation around fsspec FileSystem implementations."""
    def __init__(self, delegate: AbstractFileSystem):
        self.delegate: AbstractFileSystem = delegate

    def exists(self, location: NormalizedPath) -> bool:
        return self.delegate.exists(location)

    @contextmanager
    def create(self, location: NormalizedPath) -> OutputStream:
        parent = self.delegate._parent(location)
        self.delegate.makedirs(parent, exist_ok=True)
        with self.delegate.open(location, "wb") as file_obj:
            yield FSOutputStream(file_obj)

    @contextmanager
    def open(self, location: NormalizedPath) -> InputStream:
        with self.delegate.open(location, "rb") as file_obj:
            yield FSInputStream(file_obj)

    def list_files(self, location: NormalizedPath) -> Iterator[InputFile]:
        if not self.delegate.isdir(location):
            yield
        for entry in self.delegate.listdir(location, detail=True):
            if entry['type'] == 'file':
                yield FSInputFile(entry['name'], self)
