from typing import Tuple, Iterator

from deltacat.storage.rivulet.fs.file_system import FileSystem, FSInputFile, FSOutputFile, NormalizedPath
from deltacat.storage.rivulet.fs.input_file import InputFile
from deltacat.storage.rivulet.fs.local_fs import LocalFS
from deltacat.storage.rivulet.fs.output_file import OutputFile


class FileStore:
    """Entrypoint for storing and retrieving files"""

    def new_input_file(self, data_uri: str) -> InputFile:
        """Create a new InputFile for the given URI"""
        fs, path = self._resolve(data_uri)
        return FSInputFile(path, fs)

    def new_output_file(self, data_uri: str) -> OutputFile:
        """Create a new OutputFile for the given URI"""
        fs, path = self._resolve(data_uri)
        return FSOutputFile(path, fs)

    def list_files(self, data_uri: str) -> Iterator[InputFile]:
        fs, path = self._resolve(data_uri)
        return fs.list_files(path)

    def _resolve(self, data_uri: str) -> Tuple[FileSystem, NormalizedPath]:
        """Resolve the given URI to a filesystem and normalize path.
        TODO: proper protocol resolution and fs-caching
        """
        return LocalFS(), data_uri
