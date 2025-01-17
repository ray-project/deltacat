import unittest
from unittest.mock import MagicMock
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat.storage.rivulet.shard.range_shard import RangeShard, RangeShardingStrategy, NumericRangeGenerator, \
    StringRangeGenerator



class RangeShardTests(unittest.TestCase):

    def test_range_shard_initialization(self):
        mock_metastore = MagicMock(spec=DatasetMetastore)
        shard = RangeShard(mock_metastore, 0, 10)
        self.assertEqual(shard.query.min_key, 0)
        self.assertEqual(shard.query.max_key, 10)

    def test_range_shard_repr(self):
        mock_metastore = MagicMock(spec=DatasetMetastore)
        shard = RangeShard(mock_metastore, 5, 15)
        self.assertEqual(repr(shard), "Shard(type=range, start=5, end=15)")

class RangeShardingStrategyTests(unittest.TestCase):

    def test_shards_generation(self):
        mock_metastore = MagicMock(spec=DatasetMetastore)
        mock_metastore.generate_manifests.return_value = [
            MagicMock(generate_sstables=lambda: [
                MagicMock(min_key=0, max_key=50),
                MagicMock(min_key=60, max_key=100)
            ])
        ]
        strategy = RangeShardingStrategy(3)
        shards = list(strategy.shards(mock_metastore))

        self.assertEqual(len(shards), 3)
        self.assertIsInstance(shards[0], RangeShard)

    def test_empty_shards_on_no_range(self):
        mock_metastore = MagicMock(spec=DatasetMetastore)
        mock_metastore.generate_manifests.return_value = []
        strategy = RangeShardingStrategy(3)

        shards = list(strategy.shards(mock_metastore))
        self.assertEqual(len(shards), 0)

class RangeGeneratorTests(unittest.TestCase):

    def test_numeric_range_generator(self):
        generator = NumericRangeGenerator()

        # Regular range
        interpolated = generator.interpolate(0, 10, 1, 5)
        next_key = generator.next_key(5)
        self.assertEqual(interpolated, 2)
        self.assertEqual(next_key, 6)

        # Edge case: identical start and end
        interpolated = generator.interpolate(10, 10, 1, 5)
        self.assertEqual(interpolated, 10)

        # Edge case: very large range
        interpolated = generator.interpolate(0, 1e9, 1, 5)
        self.assertEqual(interpolated, 2e8)

    def test_string_range_generator(self):
        generator = StringRangeGenerator()

        # Regular range
        interpolated = generator.interpolate("a", "e", 1, 5)
        next_key = generator.next_key("a")
        self.assertEqual(interpolated, "b")
        self.assertEqual(next_key, "b")

        # Edge case: identical start and end
        interpolated = generator.interpolate("a", "a", 1, 5)
        self.assertEqual(interpolated, "a")

        # Edge case: single-character to multi-character range
        interpolated = generator.interpolate("a", "z", 25, 26)
        self.assertEqual(interpolated, "y")

        # Edge case: large string range
        interpolated = generator.interpolate("aaa", "zzz", 1, 3)
        self.assertTrue(interpolated.startswith("h"))
