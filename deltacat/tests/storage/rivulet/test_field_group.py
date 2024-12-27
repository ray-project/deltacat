import pytest

from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.glob_path import GlobPath
from deltacat.storage.rivulet.field_group import (
    FieldGroup,
    GlobPathFieldGroup,
    PydictFieldGroup,
    FileSystemFieldGroup,
)


@pytest.fixture
def sample_schema():
    return Schema(
        {"id": Datatype("int32"), "name": Datatype("string"), "age": Datatype("int32")},
        primary_key="id",
    )


@pytest.fixture
def sample_data():
    return {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}


def test_glob_path_field_group(sample_schema):
    """Test GlobPathFieldGroup initialization and properties"""
    # Test with string path
    str_path = "/path/to/data/*.parquet"
    fg1 = GlobPathFieldGroup(str_path, sample_schema)
    assert isinstance(fg1._glob_path, GlobPath)
    assert str(fg1._glob_path.path) == str_path
    assert fg1.schema == sample_schema

    # Test with GlobPath
    glob_path = GlobPath("/path/to/other/*.parquet")
    fg2 = GlobPathFieldGroup(glob_path, sample_schema)
    assert isinstance(fg2._glob_path, GlobPath)
    assert fg2._glob_path == glob_path
    assert fg2.schema == sample_schema

    # Test string representation
    assert (
        str(fg1) == f"GlobPathFieldGroup(glob_path={str_path}, schema={sample_schema})"
    )


def test_pydict_field_group(sample_schema, sample_data):
    """Test PydictFieldGroup initialization and properties"""
    fg = PydictFieldGroup(sample_data, sample_schema)

    # Test schema property
    assert fg.schema == sample_schema

    # Test row data construction
    assert len(fg._row_data) == 3
    assert fg._row_data[1] == {"id": 1, "name": "Alice", "age": 25}
    assert fg._row_data[2] == {"id": 2, "name": "Bob", "age": 30}
    assert fg._row_data[3] == {"id": 3, "name": "Charlie", "age": 35}

    # Test empty data
    empty_fg = PydictFieldGroup({}, sample_schema)
    assert len(empty_fg._row_data) == 0

    # Test string representation
    assert str(fg) == f"DictFieldGroup(data={sample_data}, schema={sample_schema})"


def test_filesystem_field_group(sample_schema):
    """Test FileSystemFieldGroup initialization and properties"""
    fg1 = FileSystemFieldGroup(sample_schema)
    fg2 = FileSystemFieldGroup(sample_schema)

    # Test schema property
    assert fg1.schema == sample_schema

    # Test equality
    assert fg1 == fg2
    assert fg1 != "not a field group"

    # Test with different schema
    different_schema = Schema(
        {"id": Datatype("int32"), "email": Datatype("string")}, primary_key="id"
    )
    fg3 = FileSystemFieldGroup(different_schema)
    assert fg1 != fg3

    # Test string representation
    assert str(fg1) == f"FileSystemFieldGroup(schema={sample_schema})"


def test_field_group_protocol():
    """Test that all field group classes implement the FieldGroup protocol"""
    # Create instances to test
    sample_schema = Schema(
        {"id": Datatype("int32"), "name": Datatype("string")}, primary_key="id"
    )

    glob_fg = GlobPathFieldGroup("/test/*.parquet", sample_schema)
    pydict_fg = PydictFieldGroup({}, sample_schema)
    fs_fg = FileSystemFieldGroup(sample_schema)

    # Test instances instead of classes
    assert isinstance(glob_fg, FieldGroup)
    assert isinstance(pydict_fg, FieldGroup)
    assert isinstance(fs_fg, FieldGroup)
