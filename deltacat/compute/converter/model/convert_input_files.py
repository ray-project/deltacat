from __future__ import annotations
from typing import Dict


class ConvertInputFiles(Dict):
    @staticmethod
    def of(
        partition_value,
        all_data_files_for_dedupe=None,
        applicable_data_files=None,
        applicable_equality_delete_files=None,
        existing_position_delete_files=None,
    ) -> ConvertInputFiles:

        result = ConvertInputFiles()
        result["partition_value"] = partition_value
        result["all_data_files_for_dedupe"] = all_data_files_for_dedupe
        result["applicable_data_files"] = applicable_data_files
        result["applicable_equality_delete_files"] = applicable_equality_delete_files
        result["existing_position_delete_files"] = existing_position_delete_files
        return result

    @property
    def partition_value(self):
        return self["partition_value"]

    @property
    def all_data_files_for_dedupe(self):
        return self["all_data_files_for_dedupe"]

    @property
    def applicable_data_files(self):
        return self["applicable_data_files"]

    @property
    def applicable_equality_delete_files(self):
        return self["applicable_equality_delete_files"]

    @property
    def existing_position_delete_files(self):
        return self["existing_position_delete_files"]

    @partition_value.setter
    def partition_value(self, partition_value):
        self["partition_value"] = partition_value

    @all_data_files_for_dedupe.setter
    def all_data_files_for_dedupe(self, all_data_files_for_dedupe):
        self["all_data_files_for_dedupe"] = all_data_files_for_dedupe

    @applicable_data_files.setter
    def applicable_data_files(self, applicable_data_files):
        self["applicable_data_files"] = applicable_data_files

    @applicable_equality_delete_files.setter
    def applicable_equality_delete_files(self, applicable_equality_delete_files):
        self["applicable_equality_delete_files"] = applicable_equality_delete_files

    @existing_position_delete_files.setter
    def existing_position_delete_files(self, existing_position_delete_files):
        self["existing_position_delete_files"] = existing_position_delete_files
