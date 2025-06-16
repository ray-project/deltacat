from __future__ import annotations
from typing import Dict, List, Any
from pyiceberg.manifest import DataFile


class ConvertResult(Dict):
    @staticmethod
    def of(
        convert_task_index: int,
        to_be_added_files: List[DataFile],
        to_be_deleted_files: Dict[Any, List[DataFile]],
        position_delete_record_count: int,
        input_data_files_record_count: int,
        input_data_files_hash_columns_in_memory_sizes: int,
        position_delete_in_memory_sizes: int,
        position_delete_on_disk_sizes: int,
        input_data_files_on_disk_size: int,
        peak_memory_usage_bytes: int,
        memory_usage_percentage: float,
    ) -> ConvertResult:

        result = ConvertResult()
        result["convert_task_index"] = convert_task_index
        result["to_be_added_files"] = to_be_added_files
        result["to_be_deleted_files"] = to_be_deleted_files
        result["position_delete_record_count"] = position_delete_record_count
        result["input_data_files_record_count"] = input_data_files_record_count
        result[
            "input_data_files_hash_columns_in_memory_sizes"
        ] = input_data_files_hash_columns_in_memory_sizes
        result["position_delete_in_memory_sizes"] = position_delete_in_memory_sizes
        result["position_delete_on_disk_sizes"] = position_delete_on_disk_sizes
        result["input_data_files_on_disk_size"] = input_data_files_on_disk_size
        result["peak_memory_usage_bytes"] = peak_memory_usage_bytes
        result["memory_usage_percentage"] = memory_usage_percentage
        return result

    @property
    def convert_task_index(self) -> int:
        return self["convert_task_index"]

    @property
    def to_be_added_files(self) -> List[DataFile]:
        return self["to_be_added_files"]

    @property
    def to_be_deleted_files(self) -> Dict[Any, List[DataFile]]:
        return self["to_be_deleted_files"]

    @property
    def position_delete_record_count(self) -> int:
        return self["position_delete_record_count"]

    @property
    def input_data_files_record_count(self) -> int:
        return self["input_data_files_record_count"]

    @property
    def input_data_files_hash_columns_in_memory_sizes(self) -> int:
        return self["input_data_files_hash_columns_in_memory_sizes"]

    @property
    def position_delete_in_memory_sizes(self) -> int:
        return self["position_delete_in_memory_sizes"]

    @property
    def position_delete_on_disk_sizes(self) -> int:
        return self["position_delete_on_disk_sizes"]

    @property
    def input_data_files_on_disk_size(self) -> int:
        return self["input_data_files_on_disk_size"]

    @property
    def peak_memory_usage_bytes(self) -> int:
        return self["peak_memory_usage_bytes"]

    @property
    def memory_usage_percentage(self) -> float:
        return self["memory_usage_percentage"]
