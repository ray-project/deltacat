from typing import NamedTuple
from deltacat.compute.compactor import DeltaAnnotated


class RePartitionResult(NamedTuple):
    cold_delta: DeltaAnnotated
    hot_delta: DeltaAnnotated
