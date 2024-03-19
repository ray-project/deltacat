from deltacat.compute.compactor import (
    DeltaAnnotated,
)
from ray.types import ObjectRef

from typing import List, Union

from dataclasses import dataclass
import pyarrow as pa
from abc import ABC, abstractmethod
import ray


class DeleteTableStorageStrategy(ABC):
    @abstractmethod
    def store_table(self, delete_table_like) -> Union[pa.Table, ObjectRef]:
        pass

    @abstractmethod
    def get_table(self, delete_table_like) -> Union[pa.Table, ObjectRef]:
        pass


class DeleteTableNOOPStorageStrategy(DeleteTableStorageStrategy):
    def store_table(self, delete_table_like) -> Union[pa.Table, ObjectRef]:
        return delete_table_like

    def get_table(self, delete_table_like) -> Union[pa.Table, ObjectRef]:
        return delete_table_like


class DeleteTableReferenceStorageStrategy(DeleteTableStorageStrategy):
    def store_table(self, delete_table_like) -> Union[pa.Table, ObjectRef]:
        obj_ref = ray.put(delete_table_like)
        return obj_ref

    def get_table(self, delete_table_like) -> Union[pa.Table, ObjectRef]:
        table = ray.get(delete_table_like)
        return table


class DeleteFileEnvelope:
    def __init__(
        self,
        stream_position: int,
        delete_table: pa.Table,
        delete_columns: List[str],
        strategy: DeleteTableStorageStrategy = DeleteTableNOOPStorageStrategy(),
    ):
        self.strategy: DeleteTableStorageStrategy = strategy
        self._stream_position: int = stream_position
        self._delete_table: Union[pa.Table, ObjectRef] = self.strategy.store_table(
            delete_table
        )
        self._delete_columns: List[str] = delete_columns

    @property
    def stream_position(self) -> int:
        return self._stream_position

    @property
    def delete_table(self) -> pa.Table:
        return self.strategy.get_table(self._delete_table)

    @property
    def delete_columns(self) -> List[str]:
        return self._delete_columns


@dataclass
class PrepareDeleteResult:
    transformed_deltas: [List[DeltaAnnotated]]
    delete_file_envelopes: List[DeleteFileEnvelope]
