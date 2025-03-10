from abc import abstractmethod
from typing import Iterable, Optional, Protocol, TypeVar, Union

from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore

# TODO: Add type validation in dataset/schema classes
T = TypeVar("T", bound=Union[int, str])


class Shard(Protocol[T]):
    """
    Abstract base class representing a shard with defined inclusive boundaries.

    A shard represents a logical partition of data, defined by its
    minimum and maximum keys. These keys determine the range of data
    within a dataset that the shard encompasses.
    """

    min_key: Optional[T]
    max_key: Optional[T]


class ShardingStrategy(Protocol):
    """
    A sharding strategy determines how the dataset is divided into shards.
    """

    @staticmethod
    def from_string(strategy: str) -> "ShardingStrategy":
        """
        Factory method to create the appropriate ShardingStrategy from a string.

        param: strategy: The string representation of the sharding strategy.
        return: ShardingStrategy class.
        """
        if strategy == "range":
            from deltacat.storage.rivulet.shard.range_shard import RangeShardingStrategy

            return RangeShardingStrategy()
        else:
            raise ValueError(f"Unsupported sharding strategy type: {strategy}")

    @abstractmethod
    def shards(self, num_shards: int, metastore: DatasetMetastore) -> Iterable[Shard]:
        """
        Generate the shards based on the chosen strategy.
        """
