from abc import abstractmethod, ABC
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
from deltacat.compute.compactor_v2.deletes.model import (
    PrepareDeleteResult,
    DeleteFileEnvelope,
)

from typing import List, Tuple, Any, Dict

import pyarrow as pa


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
        index_identifier: int,
        df_envelopes: List[DeltaFileEnvelope],
        deletes: List[DeleteFileEnvelope],
    ) -> Tuple[List[int], Dict[str, Any]]:
        pass

    @abstractmethod
    def rebatch_df_envelopes(
        self,
        index_identifier: int,
        df_envelopes: List[DeltaFileEnvelope],
        delete_locations: List[Any],
    ) -> List[List[DeltaFileEnvelope]]:
        pass

    @abstractmethod
    def apply_deletes(
        self,
        index_identifier: int,
        table,
        table_stream_pos,
        delete_envelope: DeleteFileEnvelope,
    ) -> Tuple[Any, int]:
        pass

    @abstractmethod
    def apply_all_deletes(
        self,
        index_identifier: int,
        table,
        all_delete_tables: List[pa.Table],
    ):
        pass
