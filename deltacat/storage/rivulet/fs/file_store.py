from typing import Tuple, Iterator, Optional
from pyarrow.fs import FileSystem, FileType, FileSelector

# TODO(deltacat): Rely on deltacat implementation to resolve path and filesystem.
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

from deltacat.storage.rivulet.fs.input_file import FSInputFile
from deltacat.storage.rivulet.fs.output_file import FSOutputFile


class FileStore:
    """
    Manages the filesystem and low-level file operations.
    This class is designed to work with any filesystem supported by PyArrow; local, S3, HDFS, GCP,
    and other fsspec-compatible filesystems.

    TODO: Add better error consolidation between filesystems. Will be handled by deltacat implementation?

    method: `filesystem`: Resolves and normalizes a given path and filesystem.
    method: `file_exists`: Checks if a file exists at a given URI.
    method: `create_file`: Creates a new file for writing at a specified URI.
    method: `read_file`: Reads an existing file from a specified URI.
    method: `list_files`: Lists all files within a specified directory URI.
    """

    def __init__(self, path: str, filesystem: Optional[FileSystem] = None):
        """
        Serves as the source of truth for all file operations, ensuring that
        all paths and operations are relative to the specified filesystem,
        providing consistency and compatibility across fsspec supported backends.

        TODO (deltacat): maybe rely on deltacat catalog as a source of truth for rivulet filesystem.

        param: path (str): The base URI or path for the filesystem.
        param: filesystem (FileSystem): A PyArrow filesystem instance.
        """
        _, fs = FileStore.filesystem(path, filesystem)
        self.filesystem = filesystem or fs

    @staticmethod
    def filesystem(
        path: str, filesystem: Optional[FileSystem] = None
    ) -> Tuple[str, FileSystem]:
        """
        Resolves and normalizes the given path and filesystem.

        param: path (str): The URI or path to resolve.
        param: filesystem (Optional[FileSystem]): An optional filesystem instance.
        returns: Tuple[str, FileSystem]: The normalized path and filesystem.
        raises: AssertionError: If multiple paths are resolved.
        """
        paths, filesystem = _resolve_paths_and_filesystem(
            paths=path, filesystem=filesystem
        )
        assert len(paths) == 1, "Multiple paths not supported"
        return paths[0], filesystem

    def file_exists(
        self, data_uri: str, filesystem: Optional[FileSystem] = None
    ) -> bool:
        """
        Checks if a file exists at the specified URI.

        param: data_uri (str): The URI of the file to check.
        param: filesystem (Optional[FileSystem]): Filesystem to use. Defaults to the instance filesystem.
        returns: bool: True if the file exists, False otherwise.
        """
        path, filesystem = FileStore.filesystem(data_uri, filesystem or self.filesystem)
        return filesystem.get_file_info(path).type != FileType.NotFound

    def create_output_file(
        self, data_uri: str, filesystem: Optional[FileSystem] = None
    ) -> FSOutputFile:
        """
        Creates a new output file for writing at the specified URI.

        param: data_uri (str): The URI where the file will be created.
        param: filesystem (Optional[FileSystem]): Filesystem to use. Defaults to the instance filesystem.
        returns: FSOutputFile: An object for writing to the file.
        raises: IOError: If file creation fails.
        """
        try:
            path, filesystem = FileStore.filesystem(
                data_uri, filesystem or self.filesystem
            )
            return FSOutputFile(path, filesystem)
        except Exception as e:
            raise IOError(f"Failed to create file '{data_uri}': {e}")

    def create_input_file(
        self, data_uri: str, filesystem: Optional[FileSystem] = None
    ) -> FSInputFile:
        """
        Create a new input file for reading at the specified URI.

        param: data_uri (str): The URI of the file to read.
        param: filesystem (Optional[FileSystem]): Filesystem to use. Defaults to the instance filesystem.
        returns: FSInputFile: An object for reading the file.
        raises: IOError: If file reading fails.
        """
        try:
            path, filesystem = FileStore.filesystem(
                data_uri, filesystem or self.filesystem
            )
            return FSInputFile(path, filesystem)
        except Exception as e:
            raise IOError(f"Failed to read file '{data_uri}': {e}")

    def list_files(
        self, data_uri: str, filesystem: Optional[FileSystem] = None
    ) -> Iterator[FSInputFile]:
        """
        Lists all files in the specified directory URI.

        param: data_uri (str): The URI of the directory to list files from.
        param: filesystem (Optional[FileSystem]): Filesystem to use. Defaults to the instance filesystem.
        returns: Iterator[FSInputFile]: An iterator of FSInputFile objects representing the files.
        raises: IOError: If listing files fails.
        """
        try:
            path, filesystem = FileStore.filesystem(
                data_uri, filesystem or self.filesystem
            )
            file_info = filesystem.get_file_info(FileSelector(path, recursive=False))

            for file in file_info:
                if file.type == FileType.File:
                    yield FSInputFile(file.path, filesystem)
        except Exception as e:
            raise IOError(f"Failed to list files in '{data_uri}': {e}")
