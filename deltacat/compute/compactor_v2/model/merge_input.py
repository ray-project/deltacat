from __future__ import annotations

from ray.types import ObjectRef
from typing import Dict, List, Optional, Any
from deltacat.utils.metrics import MetricsConfig
from deltacat.utils.common import ReadKwargsProvider
from deltacat.io.object_store import IObjectStore
from deltacat.storage import (
    Partition,
    SortKey,
    interface as unimplemented_deltacat_storage,
)
from deltacat.compute.compactor_v2.constants import (
    DROP_DUPLICATES,
    MAX_RECORDS_PER_COMPACTED_FILE,
)
from deltacat.types.media import ContentType
from deltacat.compute.compactor.model.round_completion_info import RoundCompletionInfo
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups


class MergeInput(Dict):
    @staticmethod
    def of(
        dfe_groups_refs: List[ObjectRef[DeltaFileEnvelopeGroups]],
        write_to_partition: Partition,
        compacted_file_content_type: ContentType,
        primary_keys: List[str],
        hash_group_index: int,
        num_hash_groups: int,
        hash_bucket_count: int,
        drop_duplicates: Optional[bool] = DROP_DUPLICATES,
        sort_keys: Optional[List[SortKey]] = None,
        merge_task_index: Optional[int] = 0,
        max_records_per_output_file: Optional[int] = MAX_RECORDS_PER_COMPACTED_FILE,
        enable_profiler: Optional[bool] = False,
        metrics_config: Optional[MetricsConfig] = None,
        s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
        read_kwargs_provider: Optional[ReadKwargsProvider] = None,
        round_completion_info: Optional[RoundCompletionInfo] = None,
        object_store: Optional[IObjectStore] = None,
        deltacat_storage=unimplemented_deltacat_storage,
        deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
    ) -> MergeInput:

        result = MergeInput()
        result["dfe_groups_refs"] = dfe_groups_refs
        result["write_to_partition"] = write_to_partition
        result["compacted_file_content_type"] = compacted_file_content_type
        result["primary_keys"] = primary_keys
        result["hash_group_index"] = hash_group_index
        result["num_hash_groups"] = num_hash_groups
        result["hash_bucket_count"] = hash_bucket_count
        result["drop_duplicates"] = drop_duplicates
        result["sort_keys"] = sort_keys
        result["merge_task_index"] = merge_task_index
        result["max_records_per_output_file"] = max_records_per_output_file
        result["enable_profiler"] = enable_profiler
        result["metrics_config"] = metrics_config
        result["s3_table_writer_kwargs"] = s3_table_writer_kwargs or {}
        result["read_kwargs_provider"] = read_kwargs_provider
        result["round_completion_info"] = round_completion_info
        result["object_store"] = object_store
        result["deltacat_storage"] = deltacat_storage
        result["deltacat_storage_kwargs"] = deltacat_storage_kwargs or {}

        return result

    @property
    def dfe_groups_refs(self) -> List[ObjectRef[DeltaFileEnvelopeGroups]]:
        return self["dfe_groups_refs"]

    @property
    def write_to_partition(self) -> Partition:
        return self["write_to_partition"]

    @property
    def compacted_file_content_type(self) -> ContentType:
        return self["compacted_file_content_type"]

    @property
    def primary_keys(self) -> List[str]:
        return self["primary_keys"]

    @property
    def hash_group_index(self) -> int:
        return self["hash_group_index"]

    @property
    def num_hash_groups(self) -> int:
        return self["num_hash_groups"]

    @property
    def hash_bucket_count(self) -> int:
        return self["hash_bucket_count"]

    @property
    def drop_duplicates(self) -> int:
        return self["drop_duplicates"]

    @property
    def sort_keys(self) -> Optional[List[SortKey]]:
        return self.get("sort_keys")

    @property
    def merge_task_index(self) -> int:
        return self.get("merge_task_index")

    @property
    def max_records_per_output_file(self) -> int:
        return self.get("max_records_per_output_file")

    @property
    def enable_profiler(self) -> bool:
        return self.get("enable_profiler")

    @property
    def metrics_config(self) -> Optional[MetricsConfig]:
        return self.get("metrics_config")

    @property
    def s3_table_writer_kwargs(self) -> Optional[Dict[str, Any]]:
        return self.get("s3_table_writer_kwargs")

    @property
    def read_kwargs_provider(self) -> Optional[ReadKwargsProvider]:
        return self.get("read_kwargs_provider")

    @property
    def round_completion_info(self) -> Optional[RoundCompletionInfo]:
        return self.get("round_completion_info")

    @property
    def object_store(self) -> Optional[IObjectStore]:
        return self.get("object_store")

    @property
    def deltacat_storage(self) -> unimplemented_deltacat_storage:
        return self["deltacat_storage"]

    @property
    def deltacat_storage_kwargs(self) -> Optional[Dict[str, Any]]:
        return self.get("deltacat_storage_kwargs")
