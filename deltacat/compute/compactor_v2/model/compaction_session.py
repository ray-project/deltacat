from dataclasses import dataclass, fields

from deltacat.storage import (
    Partition,
    PartitionLocator,
)
from deltacat.compute.compactor import (
    RoundCompletionInfo,
)
from typing import Optional


@dataclass(frozen=True)
class ExecutionCompactionResult:
    compacted_partition: Optional[Partition]
    round_completion_info: Optional[RoundCompletionInfo]
    round_completion_file_partition_locator: Optional[PartitionLocator]
    is_inplace_compacted: bool

    def __iter__(self):
        return (getattr(self, field.name) for field in fields(self))
