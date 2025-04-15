from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class DataFile:
    """Represents a data file, e.g. a S3 object, or a local file."""

    file_path: str


class ScanTask(ABC):
    """Base class representing a unit of data to be read by a compute worker"""

    @abstractmethod
    def data_files(self) -> list[DataFile]:
        pass


@dataclass
class FileScanTask(ScanTask):
    """A unit of data in the form of data files"""

    data_file_list: list[DataFile]

    def data_files(self) -> list[DataFile]:
        return self.data_file_list


class ShardedScanTask(ScanTask):
    """A unit of data in the form of shards (e.g. shard 1-10 each represents 1/10 of all data in a Table)"""

    def data_files(self) -> list[DataFile]:
        raise NotImplementedError("data_files is not implemented for ShardedScanTask")
