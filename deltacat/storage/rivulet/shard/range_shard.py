from __future__ import annotations
from typing import Generic, List, Union, Iterable
from deltacat.storage.model.shard import T, Shard, ShardingStrategy
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore


class RangeShard(Shard, Generic[T]):
    """
    Represents a range-based shard with minimum and maximum keys.

    param: min_key: The minimum key for the shard.
    param: max_key: The maximum key for the shard.
    """

    def __init__(self, min_key: T, max_key: T):
        self.min_key = min_key
        self.max_key = max_key

    def __repr__(self) -> str:
        return f"Shard(type=range, min_key={self.min_key}, max_key={self.max_key})"

    @staticmethod
    def split(
        global_min: Union[int, str], global_max: Union[int, str], num_shards: int
    ) -> List[RangeShard]:
        """
        Splits a range into `num_shards` shards.
        Currently supports splitting ranges of integers and strings.

        Note: If global_min == global_max or num_shards <= 1, a single shard is returned,
              num_shards is ignored.

        :param global_min: The minimum key for the entire range (int or str).
        :param global_max: The maximum key for the entire range (int or str).
        :param num_shards: The number of shards to create.
        :return: A list of RangeShard objects.
        """
        if global_min == global_max or num_shards <= 1:
            return [RangeShard(global_min, global_max)]

        # Determine which interpolation function to use based on the type of min/max
        if isinstance(global_min, int) and isinstance(global_max, int):
            interpolate = RangeShard._interpolate_numeric
        elif isinstance(global_min, str) and isinstance(global_max, str):
            interpolate = RangeShard._interpolate_str
        else:
            raise ValueError(
                "Unsupported combination of types for global_min and global_max."
            )

        shards: List[RangeShard] = []
        for i in range(num_shards):
            start = interpolate(global_min, global_max, i, num_shards)
            end = interpolate(global_min, global_max, i + 1, num_shards)

            if i > 0:
                if isinstance(start, int):
                    start = shards[-1].max_key + 1
                elif isinstance(start, int):
                    char_list = list(start)
                    char_list[-1] = chr(ord(char_list[-1]) + 1)
                    start = "".join(char_list)

            shards.append(RangeShard(start, end))

        return shards

    @staticmethod
    def _interpolate_numeric(start: int, end: int, step: int, total_steps: int) -> int:
        """
        Integer interpolation using integer (floor) division.

        param: start (int): The starting number.
        param: end (int): The ending number.
        param: step (int): The current step in the interpolation (0-based).
        param: total_steps (int): The total number of interpolation steps.

        returns: int: The interpolated integer.
        """
        return start + (end - start) * step // total_steps

    @staticmethod
    def _interpolate_str(start: str, end: str, step: int, total_steps: int) -> str:
        """
        Interpolates between two strings lexicographically.

        param: start (str): The starting string.
        param: end (str): The ending string.
        param: step (int): The current step in the interpolation (0-based).
        param: total_steps (int): The total number of interpolation steps.

        returns: str: The interpolated string.
        """
        max_len = max(len(start), len(end))

        # Pad strings to the same length with spaces (smallest lexicographical character).
        start = start.ljust(max_len, " ")
        end = end.ljust(max_len, " ")

        # Interpolate character by character based on ordinal values.
        interpolated_chars = [
            chr(round(ord(s) + (ord(e) - ord(s)) * step / total_steps))
            for s, e in zip(start, end)
        ]

        return "".join(interpolated_chars).rstrip()


class RangeShardingStrategy(ShardingStrategy, Generic[T]):
    """
    Implements a sharding strategy to divide a range of keys into shards.

    method: shards: Generates a list of RangeShard objects based on the global range.
    """

    def shards(
        self, num_shards: int, metastore: DatasetMetastore
    ) -> Iterable[RangeShard[T]]:
        """
        Divides the global range of keys into evenly sized shards.

        param: num_shards: The number of shards to divide the range into.
        param: metastore: The dataset metastore providing access to manifests.
        returns: A list of RangeShard objects representing the divided range.
        """
        min, max = metastore.get_min_max_keys()
        return RangeShard.split(min, max, num_shards)
