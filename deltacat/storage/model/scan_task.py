from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class DataFile:
    file_path: str

class ScanTask(ABC):
    @abstractmethod
    def data_files(self) -> list[DataFile]:
        pass

@dataclass
class FileScanTask(ScanTask):
    data_file_list: list[DataFile]

    def data_files(self) -> list[DataFile]:
        return self.data_file_list


class ShardedScanTask:

    def data_files(self) -> list[DataFile]:
        raise NotImplementedError("data_files is not implemented for ShardedScanTask")

