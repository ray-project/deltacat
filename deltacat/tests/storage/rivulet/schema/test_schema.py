import pytest

import pyarrow as pa

from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.schema.datatype import Datatype


def test_schema_creation():
    """Test basic schema creation with valid fields"""
    fields = {
        'id': Datatype('int32'),
        'name': Datatype('string'),
        'age': Datatype('int32')
    }
    schema = Schema(fields, primary_key='id')

    assert len(schema) == 3
    assert schema['id'].datatype == Datatype('int32')
    assert schema.primary_key.name == 'id'


def test_schema_invalid_primary_key():
    """Test schema creation with invalid primary key"""
    fields = {
        'id': Datatype('int32'),
        'name': Datatype('string')
    }
    with pytest.raises(ValueError, match="Did not find primary key 'invalid_key' in Schema"):
        Schema(fields, primary_key='invalid_key')


def test_schema_field_operations():
    """Test adding and removing fields"""
    schema = Schema({'id': Datatype('int32')}, primary_key='id')

    # Add field
    schema.add_field('name', Datatype('string'))
    assert 'name' in schema
    assert schema['name'].datatype == Datatype('string')

    # Delete field
    del schema['name']
    assert 'name' not in schema

    # Try to delete primary key
    with pytest.raises(ValueError, match="Cannot delete the primary key field"):
        del schema['id']


def test_schema_to_pyarrow():
    """Test conversion to PyArrow schema"""
    fields = {
        'id': Datatype('int32'),
        'name': Datatype('string')
    }
    schema = Schema(fields, primary_key='id')
    pa_schema = schema.to_pyarrow_schema()

    assert pa_schema.names == ['id', 'name']
    assert str(pa_schema.types[0]) == 'int32'
    assert str(pa_schema.types[1]) == 'string'


def test_from_pyarrow_schema():
    """Test creating Schema from PyArrow schema"""
    # Create a PyArrow schema
    pa_schema = pa.schema([
        ('id', pa.int32()),
        ('name', pa.string()),
        ('age', pa.int32())
    ])

    # Convert to Rivulet Schema
    schema = Schema.from_pyarrow_schema(pa_schema, primary_key='id')

    # Verify conversion
    assert schema.primary_key.name == 'id'
    assert schema.primary_key.datatype == Datatype('int32')
    assert len(schema) == 3
    assert schema['name'].datatype == Datatype('string')
    assert schema['age'].datatype == Datatype('int32')

    # Test invalid primary key
    with pytest.raises(ValueError, match="Did not find primary key 'invalid_key' in Schema"):
        Schema.from_pyarrow_schema(pa_schema, primary_key='invalid_key')

def test_schema_json_serialization():
    """Test JSON serialization and deserialization"""
    original_schema = Schema({
        'id': Datatype('int32'),
        'name': Datatype('string')
    }, primary_key='id')

    # Convert to dict and back
    schema_dict = original_schema.__dict__()
    restored_schema = Schema.from_json(schema_dict)

    assert len(restored_schema) == len(original_schema)
    assert restored_schema.primary_key.name == original_schema.primary_key.name
    assert restored_schema['id'].datatype == original_schema['id'].datatype


def test_schema_filter():
    """Test schema field filtering"""
    schema = Schema({
        'id': Datatype('int32'),
        'name': Datatype('string'),
        'age': Datatype('int32')
    }, primary_key='id')

    schema.filter(['id', 'name'])
    assert len(schema) == 2
    assert 'age' not in schema
    assert 'id' in schema  # Primary key should remain

    # Try to filter out primary key
    with pytest.raises(ValueError, match="Schema filter must contain the primary key field"):
        schema.filter(['name'])