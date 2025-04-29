from __future__ import annotations
from typing import Dict, List
from deltacat.compute.converter.model.convert_input_files import ConvertInputFiles


class ConvertInput(Dict):
    @staticmethod
    def of(
        convert_input_files,
        convert_task_index,
        iceberg_table_warehouse_prefix,
        identifier_fields,
        table_io,
        table_metadata,
        compact_previous_position_delete_files,
        enforce_primary_key_uniqueness,
        position_delete_for_multiple_data_files,
        max_parallel_data_file_download,
        s3_file_system,
        s3_client_kwargs,
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
        result["s3_file_system"] = s3_file_system
        result["s3_client_kwargs"] = s3_client_kwargs

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
    def table_io(self):
        return self["table_io"]

    @property
    def table_metadata(self):
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
    def s3_file_system(self):
        return self["s3_file_system"]

    @property
    def s3_client_kwargs(self):
        return self["s3_client_kwargs"]
