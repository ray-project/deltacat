from deltacat.compute.compactor import (
    DeltaAnnotated,
)
from ray.types import ObjectRef

from typing import List, Union, Optional

from dataclasses import dataclass
import pyarrow as pa
from abc import ABC, abstractmethod
import ray
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
)

from typing import Tuple, Any, Dict


class DeleteTableStorageStrategy(ABC):
    @abstractmethod
    def store_table(
        self, delete_table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        pass

    @abstractmethod
    def get_table(
        self, delete_table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        pass


class DeleteTableNOOPStorageStrategy(DeleteTableStorageStrategy):
    def store_table(
        self, delete_table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        return delete_table_like

    def get_table(
        self, delete_table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        return delete_table_like


class DeleteTableReferenceStorageStrategy(DeleteTableStorageStrategy):
    def store_table(
        self, delete_table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        obj_ref: ObjectRef = ray.put(delete_table_like)
        return obj_ref

    def get_table(
        self, delete_table_like: Union[pa.Table, ObjectRef]
    ) -> Union[pa.Table, ObjectRef]:
        table = ray.get(delete_table_like)
        return table


class DeleteFileEnvelope:
    """
    TODO: pfaraone
    """

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
    """
    TODO: pfaraone
    """

    transformed_deltas: [List[DeltaAnnotated]]
    delete_file_envelopes: List[DeleteFileEnvelope]


class DeleteStrategy(ABC):
    """
    TODO: pfaraone
    """

    @property
    def name(self):
        pass

    @abstractmethod
    def prepare_deletes(
        self, params, input_deltas: List[DeltaAnnotated], *args, **kwargs
    ) -> PrepareDeleteResult:
        pass

    @abstractmethod
    def match_deletes(
        self,
        index_identifier: int,
        sorted_df_envelopes: List[DeltaFileEnvelope],
        deletes: List[DeleteFileEnvelope],
        *args,
        **kwargs
    ) -> Tuple[List[int], Dict[str, Any]]:
        pass

    @abstractmethod
    def rebatch_df_envelopes(
        self,
        index_identifier: int,
        df_envelopes: List[DeltaFileEnvelope],
        delete_locations: List[Any],
        *args,
        **kwargs
    ) -> List[List[DeltaFileEnvelope]]:
        pass

    @abstractmethod
    def apply_deletes(
        self,
        index_identifier: int,
        table: Optional[pa.Table],
        delete_envelope: DeleteFileEnvelope,
        *args,
        **kwargs
    ) -> Tuple[Any, int]:
        pass

    @abstractmethod
    def apply_all_deletes(
        self,
        index_identifier: int,
        table: Optional[pa.Table],
        all_delete_tables: List[pa.Table],
        *args,
        **kwargs
    ):
        pass
