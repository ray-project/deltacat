import unittest
from unittest.mock import MagicMock

from deltacat.storage.model.shard import Shard, ShardingStrategy, ShardingStrategyType
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore


class TestShard(Shard):
    """Test implementation of the Shard abstract base class."""
    def __repr__(self):
        return "TestShard()"

class TestShardTests(unittest.TestCase):

    def test_shard_initialization(self):
        mock_metastore = MagicMock(spec=DatasetMetastore)
        shard = TestShard(mock_metastore)
        self.assertEqual(shard.metastore, mock_metastore)

    def test_shard_repr(self):
        mock_metastore = MagicMock(spec=DatasetMetastore)
        shard = TestShard(mock_metastore)
        self.assertEqual(repr(shard), "TestShard()")

class ShardingStrategyMock(ShardingStrategy):
    """Test implementation of the ShardingStrategy abstract base class."""
    def shards(self, metastore: DatasetMetastore):
        return [TestShard(metastore) for _ in range(self.num_shards)]

class ShardingStrategyTypeTests(unittest.TestCase):

    def test_range_sharding_strategy(self):
        num_shards = 3
        strategy_type = ShardingStrategyType.RANGE
        strategy = strategy_type.to_class(num_shards)

        self.assertEqual(strategy.num_shards, num_shards)
        self.assertIsInstance(strategy, ShardingStrategy)

    def test_unsupported_sharding_strategy(self):
        invalid_type = MagicMock()
        invalid_type.value = "unsupported"
        with self.assertRaises(ValueError):
            ShardingStrategyType.to_class(invalid_type, 2)

class ShardingStrategyTests(unittest.TestCase):

    def test_shards_generation(self):
        mock_metastore = MagicMock(spec=DatasetMetastore)
        num_shards = 5
        strategy = ShardingStrategyMock(num_shards)

        shards = list(strategy.shards(mock_metastore))
        self.assertEqual(len(shards), num_shards)
        for shard in shards:
            self.assertIsInstance(shard, TestShard)
            self.assertEqual(shard.metastore, mock_metastore)

if __name__ == "__main__":
    unittest.main()
