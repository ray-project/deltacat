from __future__ import annotations
from enum import Enum
from typing import Generic, Optional, TypeVar, Protocol, Union, Iterable, Set, Tuple
from deltacat.storage.model.shard import Shard, ShardingStrategy
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore, ManifestAccessor
from deltacat.storage.rivulet.reader.query_expression import QueryExpression

T = TypeVar("T", bound=Union[int, float, str])

class RangeShard(Shard, Generic[T]):
    """ Represents a range-based shard with start and end keys.

    param: query: A QueryExpression object defining the range of the shard.
    """
    def __init__(self, metastore: DatasetMetastore, start: T, end: T):
        super().__init__(metastore)
        self.query = QueryExpression[T]().with_range(start, end)

    def __repr__(self) -> str:
        return f"Shard(type=range, start={self.query.min_key}, end={self.query.max_key})"

class RangeShardingStrategy(ShardingStrategy, Generic[T]):
    """ Implements a sharding strategy to divide a range of keys into shards.

    method: shards: Generates a list of RangeShard objects based on the global range.
    private_method: _gen_range: Computes the global minimum and maximum keys from manifests.
    private_method: _get_ranges: Computes shard ranges using a range generator.
    """
    def shards(self, metastore: DatasetMetastore) -> Iterable[Shard]:
        """ Divides the global range of keys into evenly sized shards.

        param: metastore: The dataset metastore providing access to manifests.
        returns: A list of RangeShard objects representing the divided range.
        """
        manifests = metastore.generate_manifests()
        global_min, global_max = self.__get_max_interval(set(manifests))

        if global_min is None or global_max is None:
            return []

        if global_min == global_max:
            return [RangeShard(metastore, global_min, global_max)]

        # determine range generation type
        range_generator = RangeGeneratorType.from_key(key=global_min)

        shards = []
        local_min = global_min
        for idx in range(self.num_shards):
            # Compute the max key for the current shard
            is_last_shard = idx == self.num_shards - 1
            local_max = global_max if is_last_shard else range_generator.interpolate(global_min, global_max, idx + 1, self.num_shards)

            shards.append(RangeShard(metastore, local_min, local_max))

            # Update the min key for the next shard
            local_min = range_generator.next_key(local_max)
            if local_min > global_max:
                break
        return shards

    def __get_max_interval(self, manifests: Set[ManifestAccessor]) -> Tuple[Optional[T], Optional[T]]:
        """ Computes the global minimum and maximum keys from the dataset manifests.

        param: manifests: A set of ManifestAccessor objects representing the dataset.
        returns: A tuple containing the global minimum and maximum keys, or None if no data is found.
        """
        global_min, global_max = None, None

        for manifest in manifests:
            for table in manifest.generate_sstables():
                if global_min is None or table.min_key < global_min:
                    global_min = table.min_key
                if global_max is None or table.max_key > global_max:
                    global_max = table.max_key

        print(f"Global min: {global_min}, Global max: {global_max}")
        return global_min, global_max


class RangeGeneratorType(Enum):
    """
    Enum to describe the type of range generator based on the key type.

    type: NUMERIC: For numeric ranges (int or float).
    type: STRING: For string ranges.
    method: from_key(key): Determines and returns the appropriate range generator
                           based on the type of the given key.
    raises: ValueError: If the key type is unsupported.
    """
    NUMERIC = "numeric" # int or float
    STRING = "string"

    @staticmethod
    def from_key(key) -> RangeGenerator:
        if isinstance(key, (int, float)):
            return NumericRangeGenerator()
        elif isinstance(key, str):
            return StringRangeGenerator()
        else:
            raise ValueError("Unsupported type for range generator determination.")


class RangeGenerator(Protocol[T]):
    """ Protocol for range generation classes.

    method: interpolate: Computes an intermediate value between two bounds.
    method: next_key: Computes the next value in the range.
    """
    def interpolate(self, start: T, end: T, step: int, total_steps: int) -> T:
        ...

    def next_key(self, key: T) -> T:
        ...


class NumericRangeGenerator(Generic[T]):
    """
    Generates ranges for integer or float types.
    """
    def interpolate(self, start: Union[int, float], end: Union[int, float], step: int, total_steps: int) -> Union[int, float]:
        """
        Calculates an intermediate value between `start` and `end` for the given `step` out of `total_steps`.

        This method uses linear interpolation to determine the value based on the range between `start`
        and `end`.

        param: start (int | float): Start of the range.
        param: end (int | float): End of the range.
        param: step (int): Current step in the interpolation process.
        param: total_steps (int): Total number of steps for interpolation.
        returns: int | float: Interpolated value for the given step.
        """
        return start + (end - start) * step // total_steps

    def next_key(self, key: Union[int, float]) -> Union[int, float]:
        """
        Computes the next key in a numeric sequence by incrementing `key` by 1.

        param: key (int | float): Current key value.
        returns: int | float: The next key in the sequence.
        """
        return key + 1


class StringRangeGenerator(Generic[T]):
    """
    Generates ranges for string types.
    """
    def interpolate(self, start: str, end: str, step: int, total_steps: int) -> str:
        """
        Calculates an intermediate string between `start` and `end` for the given `step` out of `total_steps`.

        This method treats the UTF-8 byte representations of `start` and `end` as numerical values
        and uses linear interpolation to compute the intermediate string.

        param: start (str): Start string.
        param: end (str): End string.
        param: step (int): Current step in the interpolation process.
        param: total_steps (int): Total steps for interpolation.
        returns: str: A string representation of `start` and `end` ranges.
        """
        max_length = max(len(start), len(end))
        start_bytes = start.ljust(max_length, "\0").encode("utf-8")
        end_bytes = end.ljust(max_length, "\0").encode("utf-8")

        interpolated_bytes = bytes(
            start_b + (end_b - start_b) * step // total_steps
            for start_b, end_b in zip(start_bytes, end_bytes)
        )
        return interpolated_bytes.decode("utf-8").strip("\0")

    def next_key(self, key: str) -> str:
        """
        If `key` is non-empty, increments the last character in the string by 1
        (based on its Unicode code point). If `key` is empty, returns "a".

        param: key (str): The current key.
        returns: str: The next key in the sequence.
        """
        return chr(ord(key[-1]) + 1) if key else "a"
