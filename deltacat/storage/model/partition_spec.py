from __future__ import annotations
from typing import List, Optional, Any
from deltacat.storage.model.transform import Transform

"""
An ordered list of partition values determining the values of
ordered transforms specified in the partition spec.
"""
PartitionValues = List[Any]


class PartitionFilter(dict):
    """
    This class represents a filter for partitions.
    It is used to filter partitions based on certain criteria.
    """

    @staticmethod
    def of(
        partition_values: Optional[PartitionValues] = None,
    ) -> PartitionFilter:
        """
        Creates a new PartitionFilter instance with the specified partition key and value.
        """
        partition_filter = PartitionFilter()
        partition_filter["partitionValues"] = partition_values
        return partition_filter

    @property
    def partition_values(self) -> Optional[PartitionValues]:
        return self.get("partitionValues")


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


class StreamPartitionSpec(PartitionSpec):
    """
    A class representing a stream partition specification.
    A stream partitions deltas into multiple different Partition
    """

    pass


class DeltaPartitionSpec(PartitionSpec):
    """
    A class representing delta partition specification.
    The manifest entries in delta are partitioned based on this spec.
    """

    pass
