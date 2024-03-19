from abc import abstractmethod, ABC
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
from ray.types import ObjectRef

from typing import List, Tuple, Any, Dict

from dataclasses import dataclass, fields
import pyarrow as pa


@dataclass
class DeleteFileEnvelope:
    stream_position: int
    object_ref: ObjectRef
    delete_columns: List[str]

    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))


@dataclass
class PrepareDeleteResult:
    transformed_deltas: [List[DeltaAnnotated]]
    delete_file_envelopes: List[DeleteFileEnvelope]


class DeleteStrategy(ABC):
    @property
    def name(self):
        pass

    @abstractmethod
    def prepare_deletes(
        self,
        params: CompactPartitionParams,
        input_deltas: List[DeltaAnnotated],
        *args,
        **kwargs
    ) -> PrepareDeleteResult:
        pass

    @abstractmethod
    def match_deletes(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        index_identifier: int,
        deletes: List[DeleteFileEnvelope],
    ) -> Tuple[List[int], Dict[str, Any]]:
        pass

    @abstractmethod
    def split_incrementals(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        index_identifier: int,
        delete_locations: List[Any],
    ) -> List[List[DeltaFileEnvelope]]:
        pass

    @abstractmethod
    def apply_deletes(
        self,
        table,
        table_stream_pos,
        delete_envelopes: List[DeleteFileEnvelope],
        upsert_stream_position_to_delete_table: Dict[str, Any],
    ) -> Tuple[Any, int]:
        pass

    @abstractmethod
    def apply_all_deletes(
        self,
        table,
        all_delete_tables: List[pa.Table],
    ):
        pass
