from typing import NamedTuple, List
from deltacat.storage import Delta


class RePartitionResult(NamedTuple):
    range_deltas: List[Delta]
