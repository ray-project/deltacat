from __future__ import annotations
from typing import Dict


class ConvertResult(Dict):
    @staticmethod
    def of(
        convert_task_index,
        to_be_added_files,
        to_be_deleted_files,
        position_delete_record_count,
        input_data_files_record_count,
        input_data_files_hash_columns_in_memory_sizes,
        position_delete_in_memory_sizes,
        position_delete_on_disk_sizes,
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
        return result

    @property
    def convert_task_index(self) -> int:
        return self["convert_task_index"]

    @property
    def to_be_added_files(self):
        return self["to_be_added_files"]

    @property
    def to_be_deleted_files(self):
        return self["to_be_deleted_files"]

    @property
    def position_delete_record_count(self):
        return self["position_delete_record_count"]

    @property
    def input_data_files_record_count(self):
        return self["input_data_files_record_count"]

    @property
    def input_data_files_hash_columns_in_memory_sizes(self):
        return self["input_data_files_hash_columns_in_memory_sizes"]

    @property
    def position_delete_in_memory_sizes(self):
        return self["position_delete_in_memory_sizes"]

    @property
    def position_delete_on_disk_sizes(self):
        return self["position_delete_on_disk_sizes"]
