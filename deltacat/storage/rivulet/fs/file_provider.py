import time
from typing import List, Generator

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.input_file import InputFile
from deltacat.storage.rivulet.fs.output_file import OutputFile


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

    def __init__(self, uri: str, file_store: FileStore):
        """
        Initializes the file provider.

        param: uri: Base URI of the dataset.
        param: file_store: FileStore instance for creating and reading files.
        """
        self.uri = uri
        self._file_store = file_store

    def provide_data_file(self, extension: str) -> OutputFile:
        """
        Creates a new data file.

        param: extension: File extension (e.g., "parquet").
        returns: OutputFile instance pointing to the created data file.
        """
        uri = f"{self.uri}/data/{int(time.time_ns())}.{extension}"
        return self._file_store.create_output_file(uri)

    def provide_l0_sst_file(self) -> OutputFile:
        """
        Creates a new L0 SST file.

        returns: OutputFile instance pointing to the created SST file.
        """
        uri = f"{self.uri}/metadata/ssts/0/{int(time.time_ns())}.json"
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
        return [f"{self.uri}/metadata/ssts/0/"]

    def get_manifest_scan_directories(self) -> List[str]:
        """
        Retrieves manifest scan directories.

        returns: List of directories containing manifests.
        """
        return [f"{self.uri}/metadata/manifests/"]

    def generate_manifest_uris(self) -> Generator[InputFile, None, None]:
        """
        Generates all manifest URIs.

        returns: Generator of InputFile instances for manifests.
        """
        manifest_directory = self.get_manifest_scan_directories()
        for directory in manifest_directory:
            for file in self._file_store.list_files(directory):
                yield file

    def generate_sst_uris(self) -> Generator[InputFile, None, None]:
        """
        Generates all SST URIs.

        returns: Generator of InputFile instances for SSTs.
        """
        sst_directories = self.get_sst_scan_directories()
        for directory in sst_directories:
            for file in self._file_store.list_files(directory):
                yield file
