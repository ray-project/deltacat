import pytest
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet.shard.range_shard import RangeShard


@pytest.fixture
def sample_range_shard():
    return RangeShard(min_key=5, max_key=15)


@pytest.fixture
def sample_string_shard():
    return RangeShard(min_key="apple", max_key="zebra")


def test_with_key():
    query = QueryExpression[int]()
    query.with_key(5)
    assert query.min_key == 5
    assert query.max_key == 5
    with pytest.raises(ValueError):
        query.with_key(10)


def test_with_range():
    query = QueryExpression[int]()
    query.with_range(10, 5)
    assert query.min_key == 5
    assert query.max_key == 10
    with pytest.raises(ValueError):
        query.with_range(20, 25)


def test_matches_query():
    query = QueryExpression[int]()
    assert query.matches_query(5)
    assert query.matches_query(-999)
    query.with_range(10, 20)
    assert query.matches_query(15)
    assert not query.matches_query(25)
    assert not query.matches_query(5)


def test_below_query_range():
    query = QueryExpression[int]()
    assert not query.below_query_range(5)
    query.with_range(10, 20)
    assert query.below_query_range(5)
    assert not query.below_query_range(15)
    assert not query.below_query_range(25)


def test_with_shard_existing_query(sample_range_shard):
    query = QueryExpression[int]().with_range(10, 20)
    new_query = QueryExpression.with_shard(query, sample_range_shard)
    assert new_query.min_key == 5
    assert new_query.max_key == 20


def test_with_shard_none_shard():
    query = QueryExpression[int]().with_range(10, 20)
    result = QueryExpression.with_shard(query, None)
    assert result.min_key == 10
    assert result.max_key == 20


def test_with_shard_existing_query_string(sample_string_shard):
    query = QueryExpression[str]().with_range("banana", "yellow")
    new_query = QueryExpression.with_shard(query, sample_string_shard)
    assert new_query.min_key == "apple"
    assert new_query.max_key == "zebra"


def test_query_expression_string_matches():
    query = QueryExpression[str]().with_range("apple", "cat")
    assert query.matches_query("apple")
    assert query.matches_query("banana")
    assert not query.matches_query("dog")
