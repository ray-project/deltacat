from __future__ import annotations

import pyarrow as pa
from ray.types import ObjectRef

from typing import Union

from abc import ABC, abstractmethod
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore


class LocalTableStorageStrategy(ABC):
    @abstractmethod
    def store_table(
        self, table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        pass

    @abstractmethod
    def get_table(
        self, table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        pass


class LocalTableNOOPStorageStrategy(LocalTableStorageStrategy):
    """
    Stores the table as is without any modifications
    """

    def store_table(
        self, table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        return table_like

    def get_table(
        self, table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        return table_like


class LocalTableRayObjectStoreReferenceStorageStrategy(LocalTableStorageStrategy):
    """
    Stores the table in the RayPlasmaObjectStore - see deltacat/io/ray_plasma_object_store.py
    """

    def store_table(
        self, table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        obj_ref: ObjectRef = RayPlasmaObjectStore().put(table_like)
        return obj_ref

    def get_table(
        self, table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        table = RayPlasmaObjectStore().get(table_like)
        return table
