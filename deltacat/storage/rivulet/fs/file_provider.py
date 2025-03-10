import posixpath
import time
from typing import List, Generator

from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.input_file import InputFile
from deltacat.storage.rivulet.fs.output_file import OutputFile
from deltacat.utils.metafile_locator import _find_partition_path


class FileProvider:
    """
    Manages the generation of URIs for data and metadata files and facilitates the creation of files at those URIs.
    All files are generated relative to the root of the storage location.

    This class is inspired by the Iceberg `LocationProvider` and provides methods
    to generate paths for various types of files (e.g., data files, SSTs, and manifests)
    while maintaining a clear structure within the dataset.

    TODO (deltacat): FileProvider will be replaced/refactored once we are able to integrate with Deltacat.
    TODO: Incorporate additional file naming conventions, such as including
          partitionId, taskId, and operationId, to improve traceability and
          idempotency.
    """

    uri: str

    def __init__(self, uri: str, locator: PartitionLocator, file_store: FileStore):
        """
        Initializes the file provider.

        param: uri: Base URI of the dataset.
        param: file_store: FileStore instance for creating and reading files.
        """
        self.uri = uri
        self._locator = locator
        self._file_store = file_store

    def provide_data_file(self, extension: str) -> OutputFile:
        """
        Creates a new data file.

        TODO: Ensure storage interface can provide data files.

        param: extension: File extension (e.g., "parquet").
        returns: OutputFile instance pointing to the created data file.
        """
        partition_path = _find_partition_path(self.uri, self._locator)
        uri = posixpath.join(
            partition_path, "data", f"{int(time.time_ns())}.{extension}"
        )
        return self._file_store.create_output_file(uri)

    def provide_l0_sst_file(self) -> OutputFile:
        """
        Creates a new L0 SST file.

        TODO: Ensure storage interface can provide sst files.

        returns: OutputFile instance pointing to the created SST file.
        """
        partition_path = _find_partition_path(self.uri, self._locator)
        uri = posixpath.join(
            partition_path, "metadata", "ssts", "0", f"{int(time.time_ns())}.json"
        )
        return self._file_store.create_output_file(uri)

    def provide_input_file(self, uri: str) -> InputFile:
        """
        Reads an existing file.

        param sst_uri: URI of the file to read.
        returns: InputFile instance for the specified URI.
        """
        return self._file_store.create_input_file(uri)

    def provide_manifest_file(self) -> OutputFile:
        """
        Creates a new manifest file.

        returns: OutputFile instance pointing to the created manifest file.
        """
        uri = f"{self.uri}/metadata/manifests/{int(time.time_ns())}.json"
        return self._file_store.create_output_file(uri)

    def get_sst_scan_directories(self) -> List[str]:
        """
        Retrieves SST scan directories.

        returns: List of directories containing SSTs.
        """
        partition_path = _find_partition_path(self.uri, self._locator)
        return [f"{partition_path}/metadata/ssts/0/"]

    def generate_sst_uris(self) -> Generator[InputFile, None, None]:
        """
        Generates all SST URIs.

        returns: Generator of InputFile instances for SSTs.
        """
        sst_directories = self.get_sst_scan_directories()
        for directory in sst_directories:
            for file in self._file_store.list_files(directory):
                yield file
