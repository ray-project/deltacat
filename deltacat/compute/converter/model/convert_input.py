from __future__ import annotations
from typing import Dict, List


class ConvertInput(Dict):
    @staticmethod
    def of(
        files_for_each_bucket,
        convert_task_index,
        iceberg_table_warehouse_prefix,
        identifier_fields,
        compact_small_files,
        position_delete_for_multiple_data_files,
        max_parallel_data_file_download,
        s3_file_system,
    ) -> ConvertInput:

        result = ConvertInput()
        result["files_for_each_bucket"] = files_for_each_bucket
        result["convert_task_index"] = convert_task_index
        result["identifier_fields"] = identifier_fields
        result["iceberg_table_warehouse_prefix"] = iceberg_table_warehouse_prefix
        result["compact_small_files"] = compact_small_files
        result[
            "position_delete_for_multiple_data_files"
        ] = position_delete_for_multiple_data_files
        result["max_parallel_data_file_download"] = max_parallel_data_file_download
        result["s3_file_system"] = s3_file_system

        return result

    @property
    def files_for_each_bucket(self) -> tuple:
        return self["files_for_each_bucket"]

    @property
    def identifier_fields(self) -> List[str]:
        return self["identifier_fields"]

    @property
    def convert_task_index(self) -> int:
        return self["convert_task_index"]

    @property
    def iceberg_table_warehouse_prefix(self) -> str:
        return self["iceberg_table_warehouse_prefix"]

    @property
    def compact_small_files(self) -> bool:
        return self["compact_small_files"]

    @property
    def position_delete_for_multiple_data_files(self) -> bool:
        return self["position_delete_for_multiple_data_files"]

    @property
    def max_parallel_data_file_download(self) -> int:
        return self["max_parallel_data_file_download"]

    @property
    def s3_file_system(self):
        return self["s3_file_system"]
