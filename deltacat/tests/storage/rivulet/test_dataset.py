from typing import Dict

import pytest

import pyarrow as pa

from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.field_group import GlobPathFieldGroup
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.glob_path import GlobPath
from deltacat.storage.rivulet.reader.query_expression import QueryExpression


@pytest.fixture
def sample_schema():
    return Schema(
        {"id": Datatype("int32"), "name": Datatype("string"), "age": Datatype("int32")},
        primary_key="id",
    )

@pytest.fixture
def sample_pydict():
    return {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}

@pytest.fixture
def sample_extra_records_pydict():
    return {"id": [4, 5, 6], "name": ["Alice", "Bob", "McKenzie"], "age": [19, 30, 54]}

@pytest.fixture
def sample_schema_overlap_pydict():
    return {"id": [7, 8, 9], "name": ["Rahul", "Priya", "Matthew"], "zip": [48072, 91048, 49320]}

def write_parquet_file_from_dict(path: str, data: Dict):
    # Helper function to create temporary parquet files from dictionaries
    table = pa.Table.from_pydict(data)
    pa.parquet.write_table(table, path)



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
    assert "id" in dataset.schema
    assert "name" in dataset.schema
    assert "age" in dataset.schema


def test_dataset_new_field_group(tmp_path, sample_schema):
    """Test adding new field group to dataset"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)

    field_group = dataset.new_field_group(sample_schema)
    assert field_group in dataset.field_groups
    assert dataset.schema.primary_key.name == "id"


def test_dataset_conflicting_field_groups(tmp_path, sample_schema):
    """Test adding field groups with conflicting fields"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)

    # Add first field group
    dataset.new_field_group(sample_schema)

    # Create conflicting schema with same field name
    conflicting_schema = Schema(
        {
            "id": Datatype("int32"),
            "name": Datatype("string"),  # Conflicting field
        },
        primary_key="id",
    )

    # Should raise error due to conflicting 'name' field
    with pytest.raises(
        ValueError, match="Ambiguous field 'name' present in multiple field groups"
    ):
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
    selected = dataset.select(["id", "name"])
    assert selected is dataset  # Should return self for chaining


def test_dataset_different_primary_keys(tmp_path, sample_schema):
    """Test adding field groups with different primary keys"""
    base_uri = str(tmp_path / "test_dataset")
    dataset = Dataset(base_uri)
    dataset.new_field_group(sample_schema)

    different_pk_schema = Schema(
        {
            "user_id": Datatype("int32"),  # Different primary key
            "email": Datatype("string"),
        },
        primary_key="user_id",
    )

    with pytest.raises(
        ValueError, match="Field group .* must use dataset's primary key"
    ):
        dataset.new_field_group(different_pk_schema)


def test_dataset_from_glob_path(tmp_path, sample_schema):
    """Test creating dataset from glob path"""
    base_uri = str(tmp_path / "test_dataset")
    glob_path = GlobPath("test/*.parquet")
    dataset = Dataset.from_glob_path(base_uri, glob_path, sample_schema)

    assert dataset.schema == sample_schema
    assert len(dataset.field_groups) == 1


def test_from_parquet_single_file(tmp_path, sample_pydict):
    """Test creating Dataset from parquet file"""
    # First create a sample parquet file
    table = pa.Table.from_pydict(sample_pydict)

    # Write sample data to parquet
    parquet_file_path = tmp_path / "test.parquet"
    pa.parquet.write_table(table, str(parquet_file_path))

    # Test creating dataset from parquet
    dataset = Dataset.from_parquet(str(parquet_file_path), primary_key='id')

    # Verify the dataset
    assert len(dataset.field_groups) == 1
    assert isinstance(dataset.field_groups[0], GlobPathFieldGroup)
    assert dataset.schema.primary_key.name == 'id'
    assert len(dataset.schema) == 3
    # Note: we're auto-inferring the schema using pyarrow, not using the fixture's int32 schema, so it ends up as int64
    assert dataset.schema['id'].datatype == Datatype('int64')
    assert dataset.schema['name'].datatype == Datatype('string')
    assert dataset.schema['age'].datatype == Datatype('int64')

    # Test file not found
    with pytest.raises(FileNotFoundError):
        Dataset.from_parquet(str(tmp_path / "nonexistent.parquet"), primary_key='id')

def test_from_parquet_glob_path(tmp_path, sample_pydict, sample_extra_records_pydict):
    """Test creating Dataset from multiple parquet files"""
    # First create sample parquet files
    sample_pydict_path = tmp_path / "sample_pydict.parquet"
    sample_extra_records_pydict_path = tmp_path / "sample_extra_records_pydict.parquet"
    write_parquet_file_from_dict(sample_pydict_path, sample_pydict)
    write_parquet_file_from_dict(sample_extra_records_pydict_path, sample_extra_records_pydict)


    # Test creating dataset from parquet base path containing both files
    dataset = Dataset.from_parquet(str(tmp_path), primary_key='id')

    # Verify the dataset, both source files have the same schema, so we should just have one field_group
    assert len(dataset.field_groups) == 1
    assert isinstance(dataset.field_groups[0], GlobPathFieldGroup)
    assert dataset.schema.primary_key.name == 'id'
    assert len(dataset.schema) == 3
    # Note: we're auto-inferring the schema using pyarrow, not using the fixture's int32 schema, so it ends up as int64
    assert dataset.schema['id'].datatype == Datatype('int64')
    assert dataset.schema['name'].datatype == Datatype('string')
    assert dataset.schema['age'].datatype == Datatype('int64')

def test_from_parquet_glob_path_schema_modes(tmp_path, sample_pydict, sample_schema_overlap_pydict):
    """Test creating Dataset from multiple parquet files with different schema modes"""
    # First create sample parquet files
    sample_pydict_path = tmp_path / "sample_pydict.parquet"
    sample_schema_overlap_pydict_path = tmp_path / "sample_schema_overlap_pydict.parquet"
    write_parquet_file_from_dict(sample_pydict_path, sample_pydict)
    write_parquet_file_from_dict(sample_schema_overlap_pydict_path, sample_schema_overlap_pydict)

    # Test intersect schema mode (default)
    dataset_intersect = Dataset.from_parquet(str(tmp_path), primary_key='id', schema_mode='intersect')
    assert len(dataset_intersect.field_groups) == 1
    assert isinstance(dataset_intersect.field_groups[0], GlobPathFieldGroup)
    assert dataset_intersect.schema.primary_key.name == 'id'
    assert len(dataset_intersect.schema) == 2  # Only common columns: 'id' and 'name'
    assert 'id' in dataset_intersect.schema
    assert 'name' in dataset_intersect.schema
    assert 'age' not in dataset_intersect.schema
    assert 'zip' not in dataset_intersect.schema

    # Test union schema mode
    dataset_union = Dataset.from_parquet(str(tmp_path), primary_key='id', schema_mode='union')
    assert len(dataset_union.field_groups) == 1
    assert isinstance(dataset_union.field_groups[0], GlobPathFieldGroup)
    assert dataset_union.schema.primary_key.name == 'id'
    assert len(dataset_union.schema) == 4  # All columns: 'id', 'name', 'age', 'zip'
    assert dataset_union.schema['id'].datatype == Datatype('int64')
    assert dataset_union.schema['name'].datatype == Datatype('string')
    assert dataset_union.schema['age'].datatype == Datatype('int64')
    assert dataset_union.schema['zip'].datatype == Datatype('int64')
