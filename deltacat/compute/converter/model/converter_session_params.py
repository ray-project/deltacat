from __future__ import annotations
from typing import Optional, Dict
from deltacat.compute.converter.constants import (
    DEFAULT_CONVERTER_TASK_MAX_PARALLELISM,
    DEFAULT_ICEBERG_NAMESPACE,
)
from fsspec import AbstractFileSystem


class ConverterSessionParams(dict):
    """
    This class represents the parameters passed to convert_ (deltacat/compute/compactor/compaction_session.py)
    """

    @staticmethod
    def of(params: Optional[Dict]) -> ConverterSessionParams:
        params = {} if params is None else params
        assert params.get("catalog") is not None, "catalog is a required arg"
        assert (
            params.get("iceberg_table_name") is not None
        ), "iceberg_table_name is a required arg"
        assert (
            params.get("iceberg_warehouse_bucket_name") is not None
        ), "iceberg_warehouse_bucket_name is a required arg"
        result = ConverterSessionParams(params)

        result.iceberg_namespace = params.get(
            "iceberg_namespace", DEFAULT_ICEBERG_NAMESPACE
        )
        result.enforce_primary_key_uniqueness = params.get(
            "enforce_primary_key_uniqueness", False
        )
        result.compact_small_files = params.get("compact_small_files", False)

        # For Iceberg v3 spec, option to produce delete vector that can establish 1:1 mapping with data files.
        result.position_delete_for_multiple_data_files = params.get(
            "position_delete_for_multiple_data_files", True
        )
        result.task_max_parallelism = params.get(
            "task_max_parallelism", DEFAULT_CONVERTER_TASK_MAX_PARALLELISM
        )
        result.merge_keys = params.get("merge_keys", None)
        result.s3_client_kwargs = params.get("s3_client_kwargs", {})
        result.s3_file_system = params.get("s3_file_system", None)
        result.s3_prefix_override = params.get("s3_prefix_override", None)

        return result

    @property
    def catalog(self):
        return self["catalog"]

    @property
    def iceberg_table_name(self) -> str:
        return self["iceberg_table_name"]

    @property
    def iceberg_warehouse_bucket_name(self) -> str:
        return self["iceberg_warehouse_bucket_name"]

    @property
    def iceberg_namespace(self) -> str:
        return self["iceberg_namespace"]

    @iceberg_namespace.setter
    def iceberg_namespace(self, iceberg_namespace) -> None:
        self["iceberg_namespace"] = iceberg_namespace

    @property
    def enforce_primary_key_uniqueness(self) -> bool:
        return self["enforce_primary_key_uniqueness"]

    @enforce_primary_key_uniqueness.setter
    def enforce_primary_key_uniqueness(self, enforce_primary_key_uniqueness) -> None:
        self["enforce_primary_key_uniqueness"] = enforce_primary_key_uniqueness

    @property
    def compact_small_files(self) -> bool:
        return self["compact_small_files"]

    @compact_small_files.setter
    def compact_small_files(self, compact_small_files) -> None:
        self["compact_small_files"] = compact_small_files

    @property
    def position_delete_for_multiple_data_files(self) -> bool:
        return self["position_delete_for_multiple_data_files"]

    @position_delete_for_multiple_data_files.setter
    def position_delete_for_multiple_data_files(
        self, position_delete_for_multiple_data_files
    ) -> None:
        self[
            "position_delete_for_multiple_data_files"
        ] = position_delete_for_multiple_data_files

    @property
    def task_max_parallelism(self) -> str:
        return self["task_max_parallelism"]

    @task_max_parallelism.setter
    def task_max_parallelism(self, task_max_parallelism) -> None:
        self["task_max_parallelism"] = task_max_parallelism

    @property
    def merge_keys(self) -> str:
        return self["merge_keys"]

    @merge_keys.setter
    def merge_keys(self, merge_keys) -> None:
        self["merge_keys"] = merge_keys

    @property
    def s3_client_kwargs(self) -> Dict:
        return self["s3_client_kwargs"]

    @s3_client_kwargs.setter
    def s3_client_kwargs(self, s3_client_kwargs) -> None:
        self["s3_client_kwargs"] = s3_client_kwargs

    @property
    def s3_file_system(self) -> AbstractFileSystem:
        return self["s3_file_system"]

    @s3_file_system.setter
    def s3_file_system(self, s3_file_system) -> None:
        self["s3_file_system"] = s3_file_system

    @property
    def location_provider_prefix_override(self) -> str:
        return self["location_provider_prefix_override"]

    @location_provider_prefix_override.setter
    def location_provider_prefix_override(
        self, location_provider_prefix_override
    ) -> None:
        self["location_provider_prefix_override"] = location_provider_prefix_override
