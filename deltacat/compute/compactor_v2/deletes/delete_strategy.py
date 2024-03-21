from typing import List, Optional

import pyarrow as pa
from abc import ABC, abstractmethod

from typing import Tuple, Any
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)


class DeleteStrategy(ABC):
    """
    Encapsulates a strategy for applying row-level deletes on tables during compaction
    """

    @property
    def name(self) -> str:
        pass

    @abstractmethod
    def apply_deletes(
        self,
        table: Optional[pa.Table],
        delete_file_envelope: DeleteFileEnvelope,
        *args,
        **kwargs,
    ) -> Tuple[Any, int]:
        pass

    @abstractmethod
    def apply_all_deletes(
        self,
        delete_file_envelopes: List[DeleteFileEnvelope],
        *args,
        **kwargs,
    ):
        pass
