from __future__ import annotations
from typing import Dict, List, Optional, Any
class ConvertInput(Dict):
    @staticmethod
    def of(files_for_each_bucket,
           partition_value,
            identifier_fields,
            compact_small_files,
            position_delete_for_multiple_data_files,
            max_parallel_data_file_download
    ) -> ConvertInput:

        result = ConvertInput()
        result["files_for_each_bucket"] = files_for_each_bucket
        result["partition_value"] = partition_value
        result["identifier_fields"] = identifier_fields
        result["compact_small_files"] = compact_small_files
        result["position_delete_for_multiple_data_files"] = position_delete_for_multiple_data_files
        result["max_parallel_data_file_download"] = max_parallel_data_file_download
        return result

    @property
    def files_for_each_bucket(self) -> tuple:
        return self["files_for_each_bucket"]

    @property
    def partition_value(self) -> str:
        return self["partition_value"]

    @property
    def identifier_fields(self) -> List[str]:
        return self["identifier_fields"]

    @property
    def compact_small_files(self) -> bool:
        return self["compact_small_files"]

    @property
    def position_delete_for_multiple_data_files(self) -> bool:
        return self["position_delete_for_multiple_data_files"]

    @property
    def max_parallel_data_file_download(self) -> int:
        return self["max_parallel_data_file_download"]


