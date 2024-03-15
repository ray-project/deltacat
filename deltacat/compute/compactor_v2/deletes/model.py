from dataclasses import dataclass
from typing import List

from deltacat.compute.compactor import (
    DeltaAnnotated,
)


@dataclass
class PrepareDeletesResult:
    uniform_deltas: List[DeltaAnnotated]
    deletes_obj_ref_by_stream_position: IntegerRangeDict
