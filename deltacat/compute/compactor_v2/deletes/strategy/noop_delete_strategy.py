from abc import abstractmethod
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
from ray.types import ObjectRef

from typing import List, Tuple, Dict, Any

from deltacat.compute.compactor_v2.deletes.model import (
    DeleteStrategy,
    PrepareDeleteResult,
    DeleteEnvelope,
)


class NOOPDeleteStrategy(DeleteStrategy):
    _name: str = "NOOPDeleteStrategy"

    @property
    def name(cls):
        return cls._name

    @abstractmethod
    def prepare_deletes(
        self, params: CompactPartitionParams, uniform_deltas: List[DeltaAnnotated]
    ) -> PrepareDeleteResult:
        return PrepareDeleteResult(uniform_deltas, [])

    @abstractmethod
    def get_deletes_indices(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        deletes: List[DeleteEnvelope],
        *args,
        **kwargs
    ) -> Tuple[List[int], Dict[int, Any]]:
        return [], None
