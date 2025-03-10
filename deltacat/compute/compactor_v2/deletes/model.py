from deltacat.storage import (
    Delta,
)

from typing import List

from dataclasses import dataclass, fields
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from deltacat.compute.compactor_v2.deletes.delete_strategy import (
    DeleteStrategy,
)


@dataclass
class PrepareDeleteResult:
    non_delete_deltas: List[Delta]
    delete_file_envelopes: List[DeleteFileEnvelope]
    delete_strategy: DeleteStrategy

    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))
