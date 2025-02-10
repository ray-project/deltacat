import pytest
import pyarrow as pa

from deltacat.storage.model.schema import Schema, SchemaMap, Field


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


def test_of_with_dict(schema_a, schema_b):
    input_dict = {"schema_a": schema_a, "schema_b": schema_b}
    smap = SchemaMap.of(input_dict)
    # Verify that we get a SchemaMap instance and keys are preserved.
    assert isinstance(smap, SchemaMap)
    assert list(smap.keys()) == ["schema_a", "schema_b"]
    # Compare the stored schemas using the added equivalent_to method.
    assert smap["schema_a"].equivalent_to(schema_a)
    assert smap["schema_b"].equivalent_to(schema_b)


def test_of_with_list(schema_a, schema_b, schema_c):
    input_list = [schema_a, schema_b, schema_c]
    smap = SchemaMap.of(input_list)
    # Since no explicit names were provided, default names should be generated.
    expected_keys = ["schema_1", "schema_2", "schema_3"]
    assert list(smap.keys()) == expected_keys
    # get_schemas should return the schemas in insertion order.
    assert smap.get_schemas() == [schema_a, schema_b, schema_c]


def test_of_invalid_input():
    with pytest.raises(ValueError):
        SchemaMap.of(42)


def test_insert_default_name(schema_a):
    smap = SchemaMap()
    smap.insert(None, schema_a)
    # Expect a default key to be generated.
    key = list(smap.keys())[0]
    assert key.startswith("default_schema_")
    assert smap[key].equivalent_to(schema_a)


def test_insert_explicit_name(schema_a):
    smap = SchemaMap()
    smap.insert("explicit", schema_a)
    assert "explicit" in smap
    assert smap["explicit"].equivalent_to(schema_a)


def test_insert_duplicate(schema_a):
    smap = SchemaMap()
    smap.insert("dup", schema_a)
    with pytest.raises(ValueError):
        smap.insert("dup", schema_a)


def test_update_schema_success(schema_a, schema_b):
    smap = SchemaMap()
    smap.insert("key", schema_a)
    smap.update_schema("key", schema_b)
    assert smap["key"].equivalent_to(schema_b)


def test_update_schema_not_exist(schema_a):
    smap = SchemaMap()
    with pytest.raises(KeyError):
        smap.update_schema("nonexistent", schema_a)


def test_delete_schema_success(schema_a):
    smap = SchemaMap()
    smap.insert("key", schema_a)
    smap.delete_schema("key")
    assert "key" not in smap


def test_delete_schema_not_exist():
    smap = SchemaMap()
    with pytest.raises(KeyError):
        smap.delete_schema("nonexistent")


def test_get_schemas_order(schema_a, schema_b, schema_c):
    smap = SchemaMap()
    smap.insert("a", schema_a)
    smap.insert("b", schema_b)
    smap.insert("c", schema_c)

    # get_schemas should return the schemas in the order they were inserted.
    assert smap.get_schemas() == [schema_a, schema_b, schema_c]


def test_equivalent_to_same(schema_a, schema_b):
    smap1 = SchemaMap()
    smap1.insert("a", schema_a)
    smap1.insert("b", schema_b)

    smap2 = SchemaMap()
    # Recreate equivalent schemas via Schema.of so that the internal dicts are similar.
    schema_a_clone = Schema.of(
        [
            Field.of(
                field=pa.field("col1", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )
    schema_b_clone = Schema.of(
        [
            Field.of(
                field=pa.field("col2", pa.string(), nullable=True),
                field_id=2,
                is_merge_key=False,
            )
        ]
    )
    smap2.insert("a", schema_a_clone)
    smap2.insert("b", schema_b_clone)

    assert smap1.equivalent_to(smap2)
    assert smap2.equivalent_to(smap1)


def test_equivalent_to_different_keys(schema_a, schema_b):
    smap1 = SchemaMap()
    smap1.insert("a", schema_a)
    smap1.insert("b", schema_b)

    smap2 = SchemaMap()
    smap2.insert("x", schema_a)
    smap2.insert("b", schema_b)

    assert not smap1.equivalent_to(smap2)


def test_equivalent_to_different_order(schema_a, schema_b):
    # Although the underlying schemas are equivalent,
    # insertion order matters for msgpack serialization/deserialization.
    smap1 = SchemaMap()
    smap1.insert("a", schema_a)
    smap1.insert("b", schema_b)

    smap2 = SchemaMap()
    smap2.insert("b", schema_b)
    smap2.insert("a", schema_a)

    assert not smap1.equivalent_to(smap2)


def test_equivalent_to_non_map(schema_a):
    smap = SchemaMap()
    smap.insert("a", schema_a)
    assert not smap.equivalent_to("not a map")
