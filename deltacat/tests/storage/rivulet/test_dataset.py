import pytest
import pyarrow as pa

from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.glob_path import GlobPath
from deltacat.storage.rivulet.reader.query_expression import QueryExpression


@pytest.fixture
def sample_schema():
    return Schema({
        'id': Datatype('int32'),
        'name': Datatype('string'),
        'age': Datatype('int32')
    }, primary_key='id')


@pytest.fixture
def sample_pydict():
    return {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }


def test_dataset_creation(tmp_path):
    """Test basic dataset creation"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)
    assert len(dataset.field_groups) == 0
    assert dataset._schema is None


def test_dataset_from_pydict(tmp_path, sample_pydict):
    """Test creating dataset from Python dictionary"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset.from_pydict(base_uri, sample_pydict, "id")

    assert dataset.schema is not None
    assert 'id' in dataset.schema
    assert 'name' in dataset.schema
    assert 'age' in dataset.schema


def test_dataset_new_field_group(tmp_path, sample_schema):
    """Test adding new field group to dataset"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)

    field_group = dataset.new_field_group(sample_schema)
    assert field_group in dataset.field_groups
    assert dataset.schema.primary_key.name == 'id'


def test_dataset_conflicting_field_groups(tmp_path, sample_schema):
    """Test adding field groups with conflicting fields"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)

    # Add first field group
    dataset.new_field_group(sample_schema)

    # Create conflicting schema with same field name
    conflicting_schema = Schema({
        'id': Datatype('int32'),
        'name': Datatype('string'),  # Conflicting field
    }, primary_key='id')

    # Should raise error due to conflicting 'name' field
    with pytest.raises(ValueError, match="Ambiguous field 'name' present in multiple field groups"):
        dataset.new_field_group(conflicting_schema)


def test_dataset_writer(tmp_path, sample_schema):
    """Test dataset writer creation"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)
    field_group = dataset.new_field_group(sample_schema)

    writer = dataset.writer(field_group)
    assert writer is not None


def test_dataset_scan(tmp_path, sample_schema):
    """Test dataset scanning"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)
    dataset.new_field_group(sample_schema)

    scan = dataset.scan(QueryExpression())
    assert scan is not None
    assert scan.dataset_schema == dataset.schema


def test_dataset_select_fields(tmp_path, sample_schema):
    """Test field selection"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)
    dataset.new_field_group(sample_schema)

    # Select subset of fields
    selected = dataset.select(['id', 'name'])
    assert selected is dataset  # Should return self for chaining


def test_dataset_different_primary_keys(tmp_path, sample_schema):
    """Test adding field groups with different primary keys"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)
    dataset.new_field_group(sample_schema)

    different_pk_schema = Schema({
        'user_id': Datatype('int32'),  # Different primary key
        'email': Datatype('string'),
    }, primary_key='user_id')

    with pytest.raises(ValueError, match="Field group .* must use dataset's primary key"):
        dataset.new_field_group(different_pk_schema)


def test_dataset_from_glob_path(tmp_path, sample_schema):
    """Test creating dataset from glob path"""
    base_uri = str(tmp_path / "test_dataset")
    glob_path = GlobPath("test/*.parquet")
    dataset = Dataset.from_glob_path(base_uri, glob_path, sample_schema)

    assert dataset.schema == sample_schema
    assert len(dataset.field_groups) == 1