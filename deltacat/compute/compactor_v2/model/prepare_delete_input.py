from __future__ import annotations

from typing import Dict, List, Optional, Any
from deltacat.utils.metrics import MetricsConfig
from deltacat.utils.common import ReadKwargsProvider
from deltacat.io.object_store import IObjectStore
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.io.object_store import IObjectStore


class PrepareDeleteInput(Dict):
    @staticmethod
    def of(
        annotated_deltas: List[DeltaAnnotated],
        read_kwargs_provider: Optional[ReadKwargsProvider],
        deltacat_storage=unimplemented_deltacat_storage,
        deltacat_storage_kwargs: Optional[dict] = None,
        round_completion_info=None,
        delete_columns: Optional[List[str]] = None,
        primary_keys: List[str] = None,
        object_store: Optional[IObjectStore] = None,
    ) -> PrepareDeleteInput:

        result = PrepareDeleteInput()
        result["annotated_deltas"] = annotated_deltas
        result["read_kwargs_provider"] = read_kwargs_provider
        result["deltacat_storage"] = deltacat_storage
        result["deltacat_storage_kwargs"] = deltacat_storage_kwargs or {}
        result["round_completion_info"] = round_completion_info
        result["delete_columns"] = delete_columns
        result["primary_keys"] = primary_keys
        result["object_store"] = object_store
        return result

    @property
    def annotated_deltas(self) -> List[DeltaAnnotated]:
        return self["annotated_deltas"]

    @property
    def deltacat_storage(self) -> unimplemented_deltacat_storage:
        return self.get("deltacat_storage")

    @property
    def read_kwargs_provider(self) -> Optional[ReadKwargsProvider]:
        return self.get("read_kwargs_provider")

    @property
    def deltacat_storage_kwargs(self) -> Optional[Dict[str, Any]]:
        return self.get("deltacat_storage_kwargs")

    @property
    def round_completion_info(self):
        return self.get("round_completion_info")

    @property
    def delete_columns(self) -> List[str]:
        return self.get("delete_columns")

    @property
    def primary_keys(self) -> List[str]:
        return self.get("primary_keys")

    @property
    def object_store(self) -> Optional[IObjectStore]:
        return self.get("object_store")
