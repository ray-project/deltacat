from typing import NamedTuple, List
from deltacat.storage import Delta


class RepartitionResult(NamedTuple):
    range_deltas: List[Delta]
