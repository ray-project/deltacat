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

from dataclasses import dataclass


@dataclass
class DeleteEnvelope:
    stream_position: int
    object_ref: ObjectRef
    delete_columns: List[str]


@dataclass
class PrepareDeleteResult:
    uniform_deltas: [List[DeltaAnnotated]]
    all_deletes: List[DeleteEnvelope]


class DeleteStrategy(ABC):
    @property
    def name(self):
        pass

    @abstractmethod
    def prepare_deletes(
        self,
        params: CompactPartitionParams,
        uniform_deltas: List[DeltaAnnotated],
        *args,
        **kwargs
    ) -> PrepareDeleteResult:
        pass

    @abstractmethod
    def get_deletes_indices(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        deletes: List[DeleteEnvelope],
        *args,
        **kwargs
    ) -> Tuple[List[int], Dict[int, Any]]:
        pass
