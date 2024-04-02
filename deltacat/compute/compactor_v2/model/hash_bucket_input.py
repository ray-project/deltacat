from __future__ import annotations

from typing import Dict, List, Optional, Any
from deltacat.utils.metrics import MetricsConfig
from deltacat.utils.common import ReadKwargsProvider
from deltacat.io.object_store import IObjectStore
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.compute.compactor import DeltaAnnotated


class HashBucketInput(Dict):
    @staticmethod
    def of(
        annotated_delta: DeltaAnnotated,
        primary_keys: List[str],
        num_hash_buckets: int,
        num_hash_groups: int,
        hb_task_index: Optional[int] = 0,
        enable_profiler: Optional[bool] = False,
        metrics_config: Optional[MetricsConfig] = None,
        read_kwargs_provider: Optional[ReadKwargsProvider] = None,
        object_store: Optional[IObjectStore] = None,
        deltacat_storage=unimplemented_deltacat_storage,
        deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
        memory_logs_enabled: Optional[bool] = None,
    ) -> HashBucketInput:

        result = HashBucketInput()
        result["annotated_delta"] = annotated_delta
        result["primary_keys"] = primary_keys
        result["hb_task_index"] = hb_task_index
        result["num_hash_buckets"] = num_hash_buckets
        result["num_hash_groups"] = num_hash_groups
        result["enable_profiler"] = enable_profiler
        result["metrics_config"] = metrics_config
        result["read_kwargs_provider"] = read_kwargs_provider
        result["object_store"] = object_store
        result["deltacat_storage"] = deltacat_storage
        result["deltacat_storage_kwargs"] = deltacat_storage_kwargs or {}
        result["memory_logs_enabled"] = memory_logs_enabled

        return result

    @property
    def annotated_delta(self) -> DeltaAnnotated:
        return self["annotated_delta"]

    @property
    def primary_keys(self) -> List[str]:
        return self["primary_keys"]

    @property
    def hb_task_index(self) -> List[str]:
        return self["hb_task_index"]

    @property
    def num_hash_buckets(self) -> int:
        return self["num_hash_buckets"]

    @property
    def num_hash_groups(self) -> int:
        return self["num_hash_groups"]

    @property
    def enable_profiler(self) -> Optional[bool]:
        return self.get("enable_profiler")

    @property
    def metrics_config(self) -> Optional[MetricsConfig]:
        return self.get("metrics_config")

    @property
    def read_kwargs_provider(self) -> Optional[ReadKwargsProvider]:
        return self.get("read_kwargs_provider")

    @property
    def object_store(self) -> Optional[IObjectStore]:
        return self.get("object_store")

    @property
    def deltacat_storage(self) -> unimplemented_deltacat_storage:
        return self.get("deltacat_storage")

    @property
    def deltacat_storage_kwargs(self) -> Optional[Dict[str, Any]]:
        return self.get("deltacat_storage_kwargs")

    @property
    def memory_logs_enabled(self) -> Optional[bool]:
        return self.get("memory_logs_enabled")
