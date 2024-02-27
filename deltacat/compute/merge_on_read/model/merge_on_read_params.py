from __future__ import annotations
from typing import Optional, Dict, List, Union, Any
from deltacat.storage import (
    Delta,
    DeltaLocator,
    interface as unimplemented_deltacat_storage,
)


class MergeOnReadParams(dict):
    """
    This class represents the parameters passed to compact_partition (deltacat/compute/compactor/compaction_session.py)
    """

    @staticmethod
    def of(params: Optional[Dict]) -> MergeOnReadParams:
        params = {} if params is None else params

        result = MergeOnReadParams(params)
        assert result.deltas is not None, "deltas is a required arg"

        result.deltacat_storage = params.get(
            "deltacat_storage", unimplemented_deltacat_storage
        )
        result.reader_kwargs = params.get("reader_kwargs", {})
        result.deltacat_storage_kwargs = params.get("deltacat_storage_kwargs", {})

        return result

    @property
    def deltas(self) -> List[Union[Delta, DeltaLocator]]:
        """
        The list of deltas to compact in-memory.
        """
        return self["deltas"]

    @deltas.setter
    def deltas(self, to_set: List[Union[Delta, DeltaLocator]]) -> None:
        self["deltas"] = to_set

    @property
    def reader_kwargs(self) -> Dict[Any, Any]:
        """
        The key word arguments to be passed to the reader.
        """
        return self["reader_kwargs"]

    @reader_kwargs.setter
    def reader_kwargs(self, kwargs: Dict[Any, Any]) -> None:
        self["reader_kwargs"] = kwargs

    @property
    def deltacat_storage(self) -> unimplemented_deltacat_storage:
        return self["deltacat_storage"]

    @deltacat_storage.setter
    def deltacat_storage(self, storage: unimplemented_deltacat_storage) -> None:
        self["deltacat_storage"] = storage

    @property
    def deltacat_storage_kwargs(self) -> dict:
        return self["deltacat_storage_kwargs"]

    @deltacat_storage_kwargs.setter
    def deltacat_storage_kwargs(self, kwargs: dict) -> None:
        self["deltacat_storage_kwargs"] = kwargs
