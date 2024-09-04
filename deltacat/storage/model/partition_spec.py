from __future__ import annotations
from typing import List

from deltacat.storage.model.transform import Transform


class PartitionSpec(dict):
    """
    This class determines how the underlying entities in the
    hierarchy are partitioned. Stream partitions deltas and
    delta partitions files.
    """

    @staticmethod
    def of(ordered_transforms: List[Transform] = None) -> PartitionSpec:
        partition_spec = PartitionSpec()
        partition_spec.ordered_transforms = ordered_transforms
        return partition_spec

    @property
    def ordered_transforms(self) -> List[Transform]:
        return self.get("orderedTransforms")

    @ordered_transforms.setter
    def ordered_transforms(self, value: List[Transform]) -> None:
        self["orderedTransforms"] = value


class DeltaPartitionSpec(PartitionSpec):
    """
    A class representing delta partition specification.
    The manifest entries in delta are partitioned based on this spec.
    """

    pass
