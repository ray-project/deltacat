from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple
from pyiceberg.manifest import DataFile


class ConvertInputFiles(Dict):
    @staticmethod
    def of(
        partition_value: Any,
        all_data_files_for_dedupe: Optional[List[Tuple[int, DataFile]]] = None,
        applicable_data_files: Optional[List[List[Tuple[int, DataFile]]]] = None,
        applicable_equality_delete_files: Optional[
            List[List[Tuple[int, DataFile]]]
        ] = None,
        existing_position_delete_files: Optional[List[Tuple[int, DataFile]]] = None,
    ) -> ConvertInputFiles:

        result = ConvertInputFiles()
        result["partition_value"] = partition_value
        result["all_data_files_for_dedupe"] = all_data_files_for_dedupe
        result["applicable_data_files"] = applicable_data_files
        result["applicable_equality_delete_files"] = applicable_equality_delete_files
        result["existing_position_delete_files"] = existing_position_delete_files
        return result

    @property
    def partition_value(self) -> Any:
        return self["partition_value"]

    @property
    def all_data_files_for_dedupe(self) -> Optional[List[Tuple[int, DataFile]]]:
        return self["all_data_files_for_dedupe"]

    @property
    def applicable_data_files(self) -> Optional[List[List[Tuple[int, DataFile]]]]:
        return self["applicable_data_files"]

    @property
    def applicable_equality_delete_files(
        self,
    ) -> Optional[List[List[Tuple[int, DataFile]]]]:
        return self["applicable_equality_delete_files"]

    @property
    def existing_position_delete_files(self) -> Optional[List[Tuple[int, DataFile]]]:
        return self["existing_position_delete_files"]

    @partition_value.setter
    def partition_value(self, partition_value: Any) -> None:
        self["partition_value"] = partition_value

    @all_data_files_for_dedupe.setter
    def all_data_files_for_dedupe(
        self, all_data_files_for_dedupe: Optional[List[Tuple[int, DataFile]]]
    ) -> None:
        self["all_data_files_for_dedupe"] = all_data_files_for_dedupe

    @applicable_data_files.setter
    def applicable_data_files(
        self, applicable_data_files: Optional[List[List[Tuple[int, DataFile]]]]
    ) -> None:
        self["applicable_data_files"] = applicable_data_files

    @applicable_equality_delete_files.setter
    def applicable_equality_delete_files(
        self,
        applicable_equality_delete_files: Optional[List[List[Tuple[int, DataFile]]]],
    ) -> None:
        self["applicable_equality_delete_files"] = applicable_equality_delete_files

    @existing_position_delete_files.setter
    def existing_position_delete_files(
        self, existing_position_delete_files: Optional[List[Tuple[int, DataFile]]]
    ) -> None:
        self["existing_position_delete_files"] = existing_position_delete_files
