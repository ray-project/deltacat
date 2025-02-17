import pytest
import pyarrow as pa

from deltacat.storage.model.schema import Schema, SchemaListMap, Field


@pytest.fixture
def schema_a():
    return Schema.of(
        [
            Field.of(
                field=pa.field("col1", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )


@pytest.fixture
def schema_b():
    return Schema.of(
        [
            Field.of(
                field=pa.field("col2", pa.string(), nullable=True),
                field_id=2,
                is_merge_key=False,
            )
        ]
    )


@pytest.fixture
def schema_c():
    return Schema.of(
        [
            Field.of(
                field=pa.field("col3", pa.float64(), nullable=False),
                field_id=3,
                is_merge_key=False,
            )
        ]
    )


@pytest.fixture
def named_schema():
    schema = Schema.of(
        [
            Field.of(
                field=pa.field("col_named", pa.int64(), nullable=True),
                field_id=4,
                is_merge_key=False,
            )
        ]
    )
    schema.name = "named_schema"
    return schema


def test_of_with_dict(schema_a, schema_b):
    input_dict = {"schema_a": schema_a, "schema_b": schema_b}
    smap = SchemaListMap.of(input_dict)

    assert isinstance(smap, SchemaListMap)
    assert list(smap.keys()) == ["schema_a", "schema_b"]
    assert smap["schema_a"].equivalent_to(schema_a)
    assert smap["schema_b"].equivalent_to(schema_b)


def test_of_with_list(schema_a, schema_b, schema_c):
    input_list = [schema_a, schema_b, schema_c]
    smap = SchemaListMap.of(input_list)

    expected_keys = ["1", "2", "3"]
    assert list(smap.keys()) == expected_keys
    assert smap.get_schemas() == [schema_a, schema_b, schema_c]


def test_of_invalid_input():
    with pytest.raises(ValueError):
        SchemaListMap.of(42)


def test_insert_default_name(schema_a):
    smap = SchemaListMap()
    smap.insert(None, schema_a)
    key = list(smap.keys())[0]
    assert key == "1"
    assert smap[key].equivalent_to(schema_a)


def test_insert_explicit_name(schema_a):
    smap = SchemaListMap()
    smap.insert("explicit", schema_a)
    assert "explicit" in smap
    assert smap["explicit"].equivalent_to(schema_a)


def test_insert_duplicate(schema_a):
    smap = SchemaListMap()
    smap.insert("dup", schema_a)
    with pytest.raises(ValueError):
        smap.insert("dup", schema_a)


def test_insert_same_schema_twice_with_none_key_named_schema(named_schema):
    smap = SchemaListMap()
    smap.insert(None, named_schema)
    smap.insert(None, named_schema)

    keys = list(smap.keys())
    assert len(keys) == 2
    assert keys[0] == "named_schema"
    assert keys[1] == "named_schema_1"
    assert smap[keys[0]].equivalent_to(named_schema)
    assert smap[keys[1]].equivalent_to(named_schema)


def test_insert_same_schema_twice_with_none_key_no_name(schema_a):
    smap = SchemaListMap()
    smap.insert(None, schema_a)
    smap.insert(None, schema_a)

    keys = list(smap.keys())
    assert len(keys) == 2
    assert keys[0] == "1"
    assert keys[1] == "2"
    assert smap[keys[0]].equivalent_to(schema_a)
    assert smap[keys[1]].equivalent_to(schema_a)


def test_update_success(schema_a, schema_b):
    smap = SchemaListMap()
    smap.insert("key", schema_a)
    smap.update("key", schema_b)

    assert smap["key"].equivalent_to(schema_b)


def test_update_not_exist(schema_a):
    smap = SchemaListMap()
    with pytest.raises(KeyError):
        smap.update("nonexistent", schema_a)


def test_delete_schema_success(schema_a):
    smap = SchemaListMap()
    smap.insert("key", schema_a)
    del smap["key"]
    assert "key" not in smap


def test_delete_schema_not_exist():
    smap = SchemaListMap()
    with pytest.raises(KeyError):
        del smap["nonexistent"]


def test_get_schemas_order(schema_a, schema_b, schema_c):
    smap = SchemaListMap()
    smap.insert("a", schema_a)
    smap.insert("b", schema_b)
    smap.insert("c", schema_c)

    assert smap.get_schemas() == [schema_a, schema_b, schema_c]


def test_equivalent_to_same(schema_a, schema_b):
    smap1 = SchemaListMap()
    smap1.insert("a", schema_a)
    smap1.insert("b", schema_b)

    smap2 = SchemaListMap()
    smap2.insert("a", schema_a)
    smap2.insert("b", schema_b)

    assert smap1.equivalent_to(smap2)
    assert smap2.equivalent_to(smap1)


def test_equivalent_to_different_keys(schema_a, schema_b):
    smap1 = SchemaListMap()
    smap1.insert("a", schema_a)
    smap1.insert("b", schema_b)

    smap2 = SchemaListMap()
    smap2.insert("x", schema_a)
    smap2.insert("b", schema_b)

    assert not smap1.equivalent_to(smap2)


def test_equivalent_to_different_order(schema_a, schema_b):
    smap1 = SchemaListMap()
    smap1.insert("a", schema_a)
    smap1.insert("b", schema_b)

    smap2 = SchemaListMap()
    smap2.insert("b", schema_b)
    smap2.insert("a", schema_a)

    assert not smap1.equivalent_to(smap2)


def test_equivalent_to_non_map(schema_a):
    smap = SchemaListMap()
    smap.insert("a", schema_a)
    assert not smap.equivalent_to("not a map")


def test_equivalent_schemas_different_instances():
    """
    Edge case: ensure equivalent schemas with different instances
    are considered equivalent.
    """
    schema1 = Schema.of(
        [
            Field.of(
                pa.field("col1", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )
    schema2 = Schema.of(
        [
            Field.of(
                pa.field("col1", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )

    smap1 = SchemaListMap()
    smap1.insert("key", schema1)

    smap2 = SchemaListMap()
    smap2.insert("key", schema2)

    assert smap1.equivalent_to(smap2)


def test_empty_schemamap():
    smap1 = SchemaListMap()
    smap2 = SchemaListMap()
    assert smap1.equivalent_to(smap2)
    assert smap1.get_schemas() == []
    assert len(smap1) == 0
