import pytest

from deltacat.storage.model.shard import ShardingStrategy


def test_sharding_strategy_from_string_range():
    """
    Tests that from_string('range') returns an instance of RangeShardingStrategy.
    """
    from deltacat.storage.rivulet.shard.range_shard import RangeShardingStrategy

    strategy = ShardingStrategy.from_string("range")
    assert isinstance(strategy, RangeShardingStrategy)


def test_sharding_strategy_from_string_invalid():
    """
    Tests that from_string(...) raises ValueError for an unknown strategy string.
    """
    with pytest.raises(ValueError) as exc_info:
        ShardingStrategy.from_string("unknown_strategy")
    assert "Unsupported sharding strategy type: unknown_strategy" in str(exc_info.value)
