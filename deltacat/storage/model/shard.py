from abc import ABC, abstractmethod
from enum import Enum
from typing import Iterable

from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore

class Shard(ABC):
    """ Abstract base class representing a shard.

    - A shard is a logical partition of data, used to define a subset of a dataset for scanning or processing.
    - A shard contains key metadata that defines its scan space, such as boundaries or identifiers.
    - A shard is not a dataset in itself. It acts as a reference to a portion of a dataset.

    params: metastore: The dataset metastore containing metadata about the dataset.

    TODO: Consider a shard to be a dataset in itself...
    """
    metastore: DatasetMetastore

    def __init__(self, metastore: DatasetMetastore):
        self.metastore = metastore

    @abstractmethod
    def __repr__(self) -> str:
        pass

class ShardingStrategyType(Enum):
    """ Converts the enum value to its corresponding sharding strategy class.

    param: num_shards: The number of shards to divide the data into.
    returns: An instance of the corresponding ShardingStrategy subclass.
    raises: ValueError: If the sharding strategy type is unsupported.
    """
    RANGE = "range"

    def to_class(self, num_shards: int):
        if self == ShardingStrategyType.RANGE:
            from deltacat.storage.rivulet.shard.range_shard import RangeShardingStrategy
            return RangeShardingStrategy(num_shards)
        else:
            raise ValueError(f"Unsupported sharding strategy type: {self}")

class ShardingStrategy(ABC):
    """ Abstract base class for defining a sharding strategy.

    A sharding strategy determines how the dataset is divided into shards.

    param: num_shards: The number of shards to create.
    method: shards: Abstract method to generate shards based on the strategy.
    """
    def __init__(self, num_shards: int):
        self.num_shards = num_shards

    @abstractmethod
    def shards(self, metastore: DatasetMetastore) -> Iterable[Shard]:
        pass
