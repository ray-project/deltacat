from __future__ import annotations
from typing import Dict, List, Any, Optional
from deltacat.compute.converter.model.convert_input_files import ConvertInputFiles
from fsspec import AbstractFileSystem


class ConvertInput(Dict):
    @staticmethod
    def of(
        convert_input_files: ConvertInputFiles,
        convert_task_index: int,
        iceberg_table_warehouse_prefix: str,
        identifier_fields: List[str],
        table_io: Any,
        table_metadata: Any,
        compact_previous_position_delete_files: bool,
        enforce_primary_key_uniqueness: bool,
        position_delete_for_multiple_data_files: bool,
        max_parallel_data_file_download: int,
        filesystem: Optional[AbstractFileSystem],
        s3_client_kwargs: Optional[Dict[str, Any]],
        task_memory: float,
    ) -> ConvertInput:

        result = ConvertInput()
        result["convert_input_files"] = convert_input_files
        result["convert_task_index"] = convert_task_index
        result["identifier_fields"] = identifier_fields
        result["iceberg_table_warehouse_prefix"] = iceberg_table_warehouse_prefix
        result["table_io"] = table_io
        result["table_metadata"] = table_metadata
        result[
            "compact_previous_position_delete_files"
        ] = compact_previous_position_delete_files
        result["enforce_primary_key_uniqueness"] = enforce_primary_key_uniqueness
        result[
            "position_delete_for_multiple_data_files"
        ] = position_delete_for_multiple_data_files
        result["max_parallel_data_file_download"] = max_parallel_data_file_download
        result["filesystem"] = filesystem
        result["s3_client_kwargs"] = s3_client_kwargs
        result["task_memory"] = task_memory

        return result

    @property
    def convert_input_files(self) -> ConvertInputFiles:
        return self["convert_input_files"]

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
    def table_io(self) -> Any:
        return self["table_io"]

    @property
    def table_metadata(self) -> Any:
        return self["table_metadata"]

    @property
    def compact_previous_position_delete_files(self) -> bool:
        return self["compact_previous_position_delete_files"]

    @property
    def enforce_primary_key_uniqueness(self) -> bool:
        return self["enforce_primary_key_uniqueness"]

    @property
    def position_delete_for_multiple_data_files(self) -> bool:
        return self["position_delete_for_multiple_data_files"]

    @property
    def max_parallel_data_file_download(self) -> int:
        return self["max_parallel_data_file_download"]

    @property
    def filesystem(self) -> Optional[AbstractFileSystem]:
        return self["filesystem"]

    @property
    def s3_client_kwargs(self) -> Optional[Dict[str, Any]]:
        return self["s3_client_kwargs"]

    @property
    def task_memory(self) -> float:
        return self["task_memory"]
