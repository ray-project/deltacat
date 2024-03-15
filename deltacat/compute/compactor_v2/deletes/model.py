from dataclasses import dataclass
from typing import List

from deltacat.compute.compactor import (
    DeltaAnnotated,
)


@dataclass
class PrepareDeletesResult:
    uniform_deltas: List[DeltaAnnotated]
    delete_obj_ref_by_stream_position_list: List
