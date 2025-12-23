from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple
from pyiceberg.manifest import DataFile

# Type aliases to simplify nested types
DataFileWithSequence = Tuple[int, DataFile]  # (sequence_number, data_file)
DataFileList = List[DataFileWithSequence]  # List of data files with sequence numbers
DataFileListGroup = List[DataFileList]  # Group of data file lists


class ConvertInputFiles(Dict):
    @staticmethod
    def of(
        partition_value: Any,
        all_data_files_for_dedupe: Optional[DataFileList] = None,
        applicable_data_files: Optional[DataFileListGroup] = None,
        applicable_equality_delete_files: Optional[DataFileListGroup] = None,
        existing_position_delete_files: Optional[DataFileList] = None,
        sub_bucket_enabled: bool = False,
        sub_bucket_input_files: Optional[List[DataFileList]] = None,
    ) -> ConvertInputFiles:

        result = ConvertInputFiles()
        result["partition_value"] = partition_value
        result["all_data_files_for_dedupe"] = all_data_files_for_dedupe
        result["applicable_data_files"] = applicable_data_files
        result["applicable_equality_delete_files"] = applicable_equality_delete_files
        result["existing_position_delete_files"] = existing_position_delete_files
        result["sub_bucket_enabled"] = sub_bucket_enabled
        result["sub_bucket_input_files"] = sub_bucket_input_files
        result["sub_bucket_memory_estimates"] = None
        return result

    @property
    def partition_value(self) -> Any:
        return self["partition_value"]

    @property
    def all_data_files_for_dedupe(self) -> Optional[DataFileList]:
        return self["all_data_files_for_dedupe"]

    @property
    def applicable_data_files(self) -> Optional[DataFileListGroup]:
        return self["applicable_data_files"]

    @property
    def applicable_equality_delete_files(
        self,
    ) -> Optional[DataFileListGroup]:
        return self["applicable_equality_delete_files"]

    @property
    def existing_position_delete_files(self) -> Optional[DataFileList]:
        return self["existing_position_delete_files"]

    @property
    def sub_bucket_enabled(self) -> bool:
        return self["sub_bucket_enabled"]

    @property
    def sub_bucket_input_files(self) -> Optional[List[DataFileList]]:
        return self["sub_bucket_input_files"]

    @property
    def sub_bucket_memory_estimates(self) -> Optional[List[float]]:
        return self["sub_bucket_memory_estimates"]

    @partition_value.setter
    def partition_value(self, partition_value: Any) -> None:
        self["partition_value"] = partition_value

    @all_data_files_for_dedupe.setter
    def all_data_files_for_dedupe(
        self, all_data_files_for_dedupe: Optional[DataFileList]
    ) -> None:
        self["all_data_files_for_dedupe"] = all_data_files_for_dedupe

    @applicable_data_files.setter
    def applicable_data_files(
        self, applicable_data_files: Optional[DataFileListGroup]
    ) -> None:
        self["applicable_data_files"] = applicable_data_files

    @applicable_equality_delete_files.setter
    def applicable_equality_delete_files(
        self,
        applicable_equality_delete_files: Optional[DataFileListGroup],
    ) -> None:
        self["applicable_equality_delete_files"] = applicable_equality_delete_files

    @existing_position_delete_files.setter
    def existing_position_delete_files(
        self, existing_position_delete_files: Optional[DataFileList]
    ) -> None:
        self["existing_position_delete_files"] = existing_position_delete_files

    @sub_bucket_enabled.setter
    def sub_bucket_enabled(self, sub_bucket_enabled: bool) -> None:
        self["sub_bucket_enabled"] = sub_bucket_enabled

    @sub_bucket_input_files.setter
    def sub_bucket_input_files(
        self, sub_bucket_input_files: Optional[List[DataFileList]]
    ) -> None:
        self["sub_bucket_input_files"] = sub_bucket_input_files

    @sub_bucket_memory_estimates.setter
    def sub_bucket_memory_estimates(
        self, sub_bucket_memory_estimates: Optional[List[float]]
    ) -> None:
        self["sub_bucket_memory_estimates"] = sub_bucket_memory_estimates
