from deltacat.compute.compactor import (
    Delta,
)

from typing import List

from dataclasses import dataclass
from deltacat.compute.compactor_v2.delete.delete_file_envelope import DeleteFileEnvelope


@dataclass
class PrepareDeleteResult:
    non_delete_deltas: [List[Delta]]
    delete_file_envelopes: List[DeleteFileEnvelope]
