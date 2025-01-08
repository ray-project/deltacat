import time
from typing import List, Generator

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.input_file import InputFile
from deltacat.storage.rivulet.fs.output_file import OutputFile


class FileLocationProvider:
    """
    Similar to iceberg LocationProvider, this class assigns locations of data and metadata files

    For now it will be super simple URI of the base path of the riv dataset and methods to construct new filepaths

    TODO FUTURE IMPROVEMENTS
    1. Consider adding other parts to the file specification, some of which may be supplied by writer. For instance, Iceberg filename includes partitionId, taskId, operationId (see [here](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L93)

    This may allow for idempotency if jobs get re-run. Note in the iceberg-spark case that it will be non-idempotent since Spark uses new task ids.
    """

    uri: str

    def __init__(self, uri, file_store: FileStore = FileStore()):
        self.uri = uri
        self._file_store: FileStore = file_store

    def new_data_file_uri(self, extension) -> OutputFile:
        uri = f"{self.uri}/data/{int(time.time_ns())}.{extension}"
        return self._file_store.new_output_file(uri)

    def new_l0_sst_file_uri(self) -> OutputFile:
        uri = f"{self.uri}/metadata/ssts/0/{int(time.time_ns())}.json"
        return self._file_store.new_output_file(uri)

    def new_manifest_file_uri(self) -> OutputFile:
        uri = f"{self.uri}/metadata/manifests/{int(time.time_ns())}.json"
        return self._file_store.new_output_file(uri)

    def get_sst_scan_directories(self) -> List[str]:
        """
        Return a list of all directories where SSTs may exist
        """
        return [f"{self.uri}/metadata/ssts/0/"]

    def get_manifest_scan_directories(self) -> List[str]:
        """
        Return a list of all directories where manifests may exist
        """
        return [f"{self.uri}/metadata/manifests/"]

    def generate_manifest_uris(self) -> Generator[InputFile, None, None]:
        """Given a rivulet base uri, generate all SST URIs
        TODO (deltacat integration?) Consider whehter we want a wrapper class for managing rivulet structure. This may end up being delta cat. So for now just using FileLocationProvider
        TODO (support filesystem interface) Abstract to work on any filesystem or S3-like interface
        """
        manifest_directory = self.get_manifest_scan_directories()
        for directory in manifest_directory:
            for p in self._file_store.list_files(directory):
                yield p

    def generate_sst_uris(self) -> Generator[InputFile, None, None]:
        """Given a rivulet base uri, generate all SST URIs

        TODO (deltacat integration?) Consider whehter we want a wrapper class for managing rivulet structure. This may end up being delta cat. So for now just using FileLocationProvider
        """
        sst_directories = self.get_sst_scan_directories()
        for directory in sst_directories:
            for p in self._file_store.list_files(directory):
                yield p
