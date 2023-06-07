from typing import NamedTuple, List
from deltacat.compute.compactor import DeltaAnnotated


class RePartitionResult(NamedTuple):
    range_deltas: List[DeltaAnnotated]
