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
    DeleteFileEnvelope,
)


class NOOPDeleteStrategy(DeleteStrategy):
    _name: str = "NOOPDeleteStrategy"

    @property
    def name(cls):
        return cls._name

    def prepare_deletes(
        self, params: CompactPartitionParams, uniform_deltas: List[DeltaAnnotated]
    ) -> PrepareDeleteResult:
        return PrepareDeleteResult(uniform_deltas, [])

    def apply_deletes(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        index_identifier: int,
        deletes: List[DeleteFileEnvelope],
        *args,
        **kwargs
    ) -> Tuple[List[int], Dict[int, Any]]:
        return [], None
