from __future__ import annotations
from typing import Optional, Dict
from deltacat.compute.converter.constants import DEFAULT_CONVERTER_TASK_MAX_PARALLELISM


class ConvertSessionParams(dict):
    """
    This class represents the parameters passed to convert_ (deltacat/compute/compactor/compaction_session.py)
    """

    @staticmethod
    def of(params: Optional[Dict]) -> ConvertSessionParams:
        params = {} if params is None else params
        assert params.get("catalog") is not None, "catalog is a required arg"
        assert (
            params.get("iceberg_table_name") is not None
        ), "iceberg_table_name is a required arg"
        assert (
            params.get("iceberg_warehouse_bucket_name") is not None
        ), "iceberg_warehouse_bucket_name is a required arg"
        result = ConvertSessionParams(params)

        result.compact_small_files = params.get("compact_small_files", False)
        result.position_delete_for_multiple_data_files = params.get(
            "position_delete_for_multiple_data_files", False
        )
        result.task_max_parallelism = params.get(
            "task_max_parallelism", DEFAULT_CONVERTER_TASK_MAX_PARALLELISM
        )
        result.merge_keys = params.get("merge_keys", None)
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
