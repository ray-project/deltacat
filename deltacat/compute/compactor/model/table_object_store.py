from __future__ import annotations

from ray.types import ObjectRef

from typing import Union

from abc import ABC, abstractmethod
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.storage import (
    LocalTable,
)

LocalTableReference = Union[ObjectRef, LocalTable]


class LocalTableStorageStrategy(ABC):
    @abstractmethod
    def store_table(self, table: LocalTable) -> LocalTableReference:
        pass

    @abstractmethod
    def get_table(self, table_like: LocalTableReference) -> LocalTable:
        pass



class LocalTableRayObjectStoreReferenceStorageStrategy(LocalTableStorageStrategy):
    """
    Stores the table in the RayPlasmaObjectStore - see deltacat/io/ray_plasma_object_store.py
    """

    def store_table(self, table: LocalTable) -> LocalTableReference:
        obj_ref: ObjectRef = RayPlasmaObjectStore().put(table)
        return obj_ref

    def get_table(self, table_like: LocalTableReference) -> LocalTable:
        table = RayPlasmaObjectStore().get(table_like)
        return table
