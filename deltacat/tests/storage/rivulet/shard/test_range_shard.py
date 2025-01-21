import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.shard.range_shard import RangeShard, RangeShardingStrategy


@pytest.fixture
def sample_numeric_dataset(tmp_path):
    """
    Creates a small Parquet file with integer-based min/max keys and
    initializes a Dataset from it. Merge key is 'id' with values [1,2,3].
    So min_key=1, max_key=3.
    """
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    }
    table = pa.Table.from_pydict(data)
    parquet_file = tmp_path / "numeric_data.parquet"
    pq.write_table(table, parquet_file)

    ds = Dataset.from_parquet(
        name="numeric_dataset",
        file_uri=str(parquet_file),
        metadata_uri=tmp_path,
        merge_keys="id",
    )
    return ds


@pytest.fixture
def sample_string_dataset(tmp_path):
    """
    Creates a small Parquet file with a string-based merge key ('name')
    and initializes a Dataset from it. Merge key has values
    ['Alice', 'Bob', 'Charlie'] => min_key='Alice', max_key='Charlie'.
    """
    data = {
        "name": ["Alice", "Charlie", "Bob"],  # random order
        "value": [100, 200, 150],
    }
    table = pa.Table.from_pydict(data)
    parquet_file = tmp_path / "string_data.parquet"
    pq.write_table(table, parquet_file)

    ds = Dataset.from_parquet(
        name="string_dataset",
        file_uri=str(parquet_file),
        metadata_uri=tmp_path,
        merge_keys="name",
    )
    return ds


def test_range_shard_repr():
    shard = RangeShard(min_key=5, max_key=15)
    assert repr(shard) == "Shard(type=range, min_key=5, max_key=15)"


def test_range_shard_split_integers():
    shards = RangeShard.split(global_min=1, global_max=10, num_shards=2)
    assert len(shards) == 2

    assert shards[0].min_key == 1
    assert shards[0].max_key == 5
    assert shards[1].min_key == 6
    assert shards[1].max_key == 10


def test_range_shard_split_integers_single_shard():
    shards = RangeShard.split(global_min=1, global_max=10, num_shards=1)
    assert len(shards) == 1
    assert shards[0].min_key == 1
    assert shards[0].max_key == 10


def test_range_shard_split_integers_same_value():
    shards = RangeShard.split(global_min=5, global_max=5, num_shards=3)
    assert len(shards) == 1


def test_range_sharding_strategy_integers(sample_numeric_dataset):
    strategy = RangeShardingStrategy()
    shards = list(
        strategy.shards(num_shards=2, metastore=sample_numeric_dataset._metastore)
    )

    assert len(shards) == 2, "Expected 2 shards for dataset with keys [1,2,3]"

    shard1, shard2 = shards
    assert isinstance(shard1, RangeShard)
    assert isinstance(shard2, RangeShard)
    assert shard1.min_key == 1
    assert shard1.max_key == 2
    assert shard2.min_key == 3
    assert shard2.max_key == 3


def test_range_sharding_strategy_integers_single_shard(sample_numeric_dataset):
    strategy = RangeShardingStrategy()
    shards = list(
        strategy.shards(num_shards=1, metastore=sample_numeric_dataset._metastore)
    )
    assert len(shards) == 1
    shard = shards[0]
    assert shard.min_key == 1
    assert shard.max_key == 3


def test_range_sharding_strategy_strings(sample_string_dataset):
    strategy = RangeShardingStrategy()
    shards = list(
        strategy.shards(num_shards=2, metastore=sample_string_dataset._metastore)
    )

    assert len(shards) == 2, "Expected 2 shards for string-based dataset"
    shard1, shard2 = shards
    assert isinstance(shard1, RangeShard)
    assert isinstance(shard2, RangeShard)

    assert shard1.min_key == "Alice"
    assert shard1.max_key < "Charlie"

    assert shard2.min_key == shard1.max_key
    assert shard2.max_key == "Charlie"


def test_range_sharding_strategy_strings_single_shard(sample_string_dataset):
    strategy = RangeShardingStrategy()
    shards = list(
        strategy.shards(num_shards=1, metastore=sample_string_dataset._metastore)
    )

    assert len(shards) == 1

    shard = shards[0]
    assert shard.min_key == "Alice"
    assert shard.max_key == "Charlie"
