from __future__ import annotations

from ray.types import ObjectRef

from typing import Any, Union

from abc import ABC, abstractmethod, abstractproperty
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.storage import (
    LocalTable,
)
from deltacat.io.object_store import IObjectStore

LocalTableReference = Union[ObjectRef, LocalTable]


class LocalTableStorageStrategy(ABC):
    @abstractproperty
    def object_store(cls) -> IObjectStore:
        pass

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

    _object_store: IObjectStore = RayPlasmaObjectStore()

    @property
    def object_store(cls) -> IObjectStore:
        return cls._object_store

    def store_table(self, table: LocalTable) -> LocalTableReference:
        obj_ref: ObjectRef = self.object_store.put(table)
        return obj_ref

    def get_table(self, table_like: LocalTableReference) -> LocalTable:
        table = self.object_store.get(table_like)
        return table

    def get_table_reference(self, table_ref: Any) -> LocalTableReference:
        return self.object_store.deserialize_references([table_ref])[0]
