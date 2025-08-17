import pytest
import pyarrow as pa

from deltacat.storage.model.schema import (
    Schema,
    Field,
    BASE_SCHEMA_NAME,
)


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
def schema_d():
    schema = Schema.of(
        [
            Field.of(
                field=pa.field("col_named", pa.int64(), nullable=True),
                field_id=4,
                is_merge_key=False,
            )
        ]
    )
    return schema


def test_of_with_dict(schema_a, schema_b):
    input_dict = {"schema_a": schema_a.arrow, "schema_b": schema_b.arrow}
    schema = Schema.of(input_dict)

    assert isinstance(schema, Schema)
    assert list(schema.subschemas.keys()) == ["schema_a", "schema_b"]
    assert list(schema.subschemas_to_field_ids.keys()) == ["schema_a", "schema_b"]
    assert schema.subschemas["schema_a"].equivalent_to(schema_a, True)
    assert schema.subschemas["schema_b"].equivalent_to(schema_b, True)


def test_of_invalid_input():
    with pytest.raises(ValueError):
        Schema.of(42)
    with pytest.raises(ValueError):
        Schema.of(["foo"])
    with pytest.raises(ValueError):
        Schema.of({"foo": [42]})


def test_insert_explicit_name(schema_a, schema_b):
    schema = Schema.of(schema_a.arrow)
    assert not schema.subschemas
    new_schema = schema.add_subschema("explicit", schema_b.arrow)
    assert not schema.subschemas
    assert "explicit" in new_schema.subschemas
    assert "explicit" in new_schema.subschemas_to_field_ids
    assert BASE_SCHEMA_NAME in new_schema.subschemas
    assert BASE_SCHEMA_NAME in new_schema.subschemas_to_field_ids
    assert new_schema.subschemas[BASE_SCHEMA_NAME].equivalent_to(schema_a)
    assert new_schema.subschemas["explicit"].equivalent_to(schema_b)


def test_insert_reserved_name_fails(schema_a, schema_b):
    schema = Schema.of(schema_a.arrow)
    with pytest.raises(ValueError):
        schema.add_subschema(BASE_SCHEMA_NAME, schema_b.arrow)


def test_insert_duplicate_schema_name_fails(schema_a, schema_b):
    schema = Schema.of({"dupe": schema_a.arrow})
    with pytest.raises(ValueError):
        schema.add_subschema("dupe", schema_b.arrow)


def test_insert_duplicate_field_name_fails(schema_a):
    schema = Schema.of(schema_a.arrow)
    with pytest.raises(ValueError):
        schema.add_subschema("dupe_field_name", schema_a.arrow)


def test_insert_autofill_field_id():
    schema1 = [
        Field.of(
            field=pa.field("col1", pa.int32(), nullable=False),
            is_merge_key=True,
        )
    ]
    schema2 = [
        Field.of(
            field=pa.field("col2", pa.int32(), nullable=False),
            is_merge_key=True,
        )
    ]
    schema = Schema.of(schema1)
    new_schema = schema.add_subschema("explicit", schema2)
    assert not schema.subschemas
    assert "explicit" in new_schema.subschemas
    assert "explicit" in new_schema.subschemas_to_field_ids
    assert BASE_SCHEMA_NAME in new_schema.subschemas
    assert BASE_SCHEMA_NAME in new_schema.subschemas_to_field_ids
    assert len(new_schema.subschemas[BASE_SCHEMA_NAME].fields) == 1
    assert len(new_schema.subschemas["explicit"].fields) == 1
    assert new_schema.subschemas[BASE_SCHEMA_NAME].fields[0].id == 0
    assert new_schema.subschemas["explicit"].fields[0].id == 1
    assert new_schema.subschemas[BASE_SCHEMA_NAME].equivalent_to(Schema.of(schema1))
    schema2_with_expected_field_id = [
        Field.of(
            field=pa.field("col2", pa.int32(), nullable=False),
            field_id=1,
            is_merge_key=True,
        )
    ]
    assert new_schema.subschemas["explicit"].equivalent_to(
        Schema.of(schema2_with_expected_field_id)
    )


def test_insert_duplicate_field_name_case_insensitive_fails():
    schema1 = Schema.of(
        [
            Field.of(
                field=pa.field("col1", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )
    schema2 = Schema.of(
        [
            Field.of(
                field=pa.field("COL1", pa.int32(), nullable=False),
                field_id=2,
                is_merge_key=True,
            )
        ]
    )
    schema = Schema.of(schema1.arrow)
    with pytest.raises(ValueError):
        schema.add_subschema("dupe_field_name_case_insensitive", schema2.arrow)


def test_insert_duplicate_field_id_fails():
    schema1 = Schema.of(
        [
            Field.of(
                field=pa.field("col1", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )
    schema2 = Schema.of(
        [
            Field.of(
                field=pa.field("col2", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )
    schema = Schema.of(schema1.arrow)
    with pytest.raises(ValueError):
        schema.add_subschema("dupe_field_id", schema2.arrow)


def test_update_success(schema_a, schema_b):
    schema = Schema.of({"key": schema_a.arrow})
    new_schema = schema.replace_subschema("key", schema_b.arrow)
    assert schema.subschemas["key"].equivalent_to(schema_a)
    assert new_schema.subschemas["key"].equivalent_to(schema_b)


def test_update_not_exist(schema_a):
    schema = Schema.of({"key": schema_a.arrow})
    with pytest.raises(ValueError):
        schema.replace_subschema("nonexistent", schema_a.arrow)


def test_delete_schema_success(schema_a, schema_b):
    schema = Schema.of({"key1": schema_a.arrow, "key2": schema_b.arrow})
    new_schema = schema.delete_subschema("key1")
    assert "key1" in schema.subschemas
    assert "key2" in schema.subschemas
    assert "key1" not in new_schema.subschemas
    assert "key2" in new_schema.subschemas
    assert "key1" in schema.subschemas_to_field_ids
    assert "key2" in schema.subschemas_to_field_ids
    assert "key1" not in new_schema.subschemas_to_field_ids
    assert "key2" in new_schema.subschemas_to_field_ids


def test_delete_only_schema_fails(schema_a):
    schema = Schema.of({"key": schema_a.arrow})
    with pytest.raises(ValueError):
        schema.delete_subschema("key")


def test_delete_schema_not_exist(schema_a):
    schema = Schema.of({"key": schema_a.arrow})
    with pytest.raises(ValueError):
        schema.delete_subschema("nonexistent")


def test_get_schemas_order(schema_a, schema_b, schema_c):
    schema = Schema.of({"a": schema_a.arrow, "b": schema_b.arrow, "c": schema_c.arrow})
    assert list(schema.subschemas.keys()) == ["a", "b", "c"]
    assert list(schema.subschemas.values()) == [schema_a, schema_b, schema_c]
    schema = Schema.of({"a": schema_a.arrow})
    schema = schema.add_subschema("b", schema_b.arrow)
    assert list(schema.subschemas.keys()) == ["a", "b"]
    assert list(schema.subschemas.values()) == [schema_a, schema_b]
    schema = schema.add_subschema("c", schema_c.arrow)
    assert list(schema.subschemas.keys()) == ["a", "b", "c"]
    assert list(schema.subschemas.values()) == [schema_a, schema_b, schema_c]


def test_equivalent_to_same(schema_a, schema_b):
    schema1 = Schema.of({"a": schema_a.arrow, "b": schema_b.arrow})
    schema2 = Schema.of({"a": schema_a.arrow, "b": schema_b.arrow})
    assert schema1.equivalent_to(schema2)
    assert schema2.equivalent_to(schema1)


def test_equivalent_to_different_subschema_names(schema_a, schema_b):
    schema1 = Schema.of({"a": schema_a.arrow, "b": schema_b.arrow})
    schema2 = Schema.of({"x": schema_a.arrow, "b": schema_b.arrow})
    assert schema1.equivalent_to(schema2)
    assert not schema1.equivalent_to(schema2, True)


def test_not_equivalent_to_different_subschema_order(schema_a, schema_b):
    schema1 = Schema.of({"a": schema_a.arrow, "b": schema_b.arrow})
    schema2 = Schema.of({"b": schema_b.arrow, "a": schema_a.arrow})
    assert not schema1.equivalent_to(schema2)


def test_equivalent_to_non_schema(schema_a):
    schema = Schema.of({"a": schema_a.arrow})
    assert not schema.equivalent_to("not a schema")


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
    schema1 = Schema.of({"key": schema1.arrow})
    schema2 = Schema.of({"key": schema2.arrow})
    assert schema1.equivalent_to(schema2)


def test_empty_schema_fails():
    with pytest.raises(ValueError):
        Schema.of({})
    with pytest.raises(ValueError):
        Schema.of([])
