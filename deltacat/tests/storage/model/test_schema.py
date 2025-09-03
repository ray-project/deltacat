import pytest
import pyarrow as pa
from deltacat.exceptions import SchemaValidationError

from deltacat.storage.model.schema import (
    Schema,
    Field,
    BASE_SCHEMA_NAME,
    SchemaConsistencyType,
    SchemaUpdate,
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


def test_schema_type_promotion_edge_cases():
    """Test edge cases for type promotion with SchemaConsistencyType.NONE."""
    # Test 1: Same type - no promotion
    field_int32 = Field.of(
        pa.field("test", pa.int32()), consistency_type=SchemaConsistencyType.NONE
    )
    data_int32 = pa.array([1, 2, 3], type=pa.int32())
    promoted_data, was_promoted = field_int32.promote_type_if_needed(data_int32)
    assert not was_promoted, "Same type should not trigger promotion"
    assert promoted_data.type == pa.int32(), "Data type should remain int32"

    # Test 2: int32 to int64 promotion
    data_int64 = pa.array([2147483648], type=pa.int64())  # Value requiring int64
    promoted_data, was_promoted = field_int32.promote_type_if_needed(data_int64)
    assert was_promoted, "int32 field should promote to int64"
    assert promoted_data.type == pa.int64(), "Promoted data should be int64"

    # Test 3: Nullability preservation
    field_nullable = Field.of(
        pa.field("test", pa.int32(), nullable=True),
        consistency_type=SchemaConsistencyType.NONE,
    )
    data_with_null = pa.array([1, None, 3], type=pa.int32())
    promoted_data, was_promoted = field_nullable.promote_type_if_needed(data_with_null)
    assert not was_promoted, "Same nullable type should not promote"

    # Test 4: Cross-type promotion (int to float)
    field_int = Field.of(
        pa.field("test", pa.int32()), consistency_type=SchemaConsistencyType.NONE
    )
    data_float = pa.array([1.5, 2.7], type=pa.float64())
    promoted_data, was_promoted = field_int.promote_type_if_needed(data_float)
    assert was_promoted, "int32 should promote to accommodate float64"
    assert pa.types.is_floating(
        promoted_data.type
    ), f"Should promote to float type, got {promoted_data.type}"


def test_schema_update_method(schema_a):
    """Test the Schema.update() convenience method."""
    # Test basic usage
    update = schema_a.update()
    assert isinstance(update, SchemaUpdate)
    assert update.base_schema == schema_a
    assert not update.allow_incompatible_changes

    # Test with allow_incompatible_changes=True
    update_permissive = schema_a.update(allow_incompatible_changes=True)
    assert isinstance(update_permissive, SchemaUpdate)
    assert update_permissive.base_schema == schema_a
    assert update_permissive.allow_incompatible_changes

    # Test method chaining with field addition
    new_field = Field.of(pa.field("name", pa.string(), nullable=True), field_id=4)
    updated_schema = schema_a.update().add_field(new_field).apply()

    assert len(updated_schema.fields) == 2
    assert updated_schema.field("col1") == schema_a.field(
        "col1"
    )  # Original field preserved
    added_field = updated_schema.field("name")
    assert added_field.arrow.name == "name"
    assert added_field.arrow.type == pa.string()
    assert added_field.id == 2  # requested field_id of 4 is ignored and auto-assigned


def test_default_value_type_promotion():
    """Test that default values are correctly cast when field types are promoted."""

    # Test 1: Unit-level default value casting
    # Create a field with int32 type and default values
    original_field = Field.of(
        pa.field("test_field", pa.int32()),
        past_default=42,
        future_default=100,
        consistency_type=SchemaConsistencyType.NONE,
    )

    # Test casting to int64
    promoted_past = original_field._cast_default_to_promoted_type(42, pa.int64())
    promoted_future = original_field._cast_default_to_promoted_type(100, pa.int64())
    assert promoted_past == 42
    assert promoted_future == 100

    # Test casting to float64
    promoted_past_float = original_field._cast_default_to_promoted_type(
        42, pa.float64()
    )
    promoted_future_float = original_field._cast_default_to_promoted_type(
        100, pa.float64()
    )
    assert promoted_past_float == 42.0
    assert promoted_future_float == 100.0

    # Test casting to string
    promoted_past_str = original_field._cast_default_to_promoted_type(42, pa.string())
    promoted_future_str = original_field._cast_default_to_promoted_type(
        100, pa.string()
    )
    assert promoted_past_str == "42"
    assert promoted_future_str == "100"

    # Test 2: Test that the default casting logic works correctly
    # Test with None values (should return None)
    none_result = original_field._cast_default_to_promoted_type(None, pa.string())
    assert none_result is None, "None default should remain None"

    # Test error handling - incompatible cast should raise SchemaValidationError
    with pytest.raises(SchemaValidationError):
        original_field._cast_default_to_promoted_type("not_a_number", pa.int64())

    # Test with a complex type
    complex_field = Field.of(
        pa.field("complex", pa.list_(pa.int32())),
        consistency_type=SchemaConsistencyType.NONE,
    )
    with pytest.raises(SchemaValidationError):
        complex_field._cast_default_to_promoted_type(42, pa.list_(pa.string()))


def test_default_value_backfill_with_promotion():
    """Test that default values are correctly backfilled when types are promoted."""

    # Test the interaction between default value casting and binary promotion
    # This represents a common scenario where defaults need to be promoted to binary
    field_with_defaults = Field.of(
        pa.field("test_field", pa.int32()),
        past_default=42,
        future_default=100,
        consistency_type=SchemaConsistencyType.NONE,
    )

    # Test promotion to string (a common "catch-all" type in type promotion)
    string_past = field_with_defaults._cast_default_to_promoted_type(42, pa.string())
    string_future = field_with_defaults._cast_default_to_promoted_type(100, pa.string())

    assert string_past == "42", f"Expected '42', got {string_past}"
    assert string_future == "100", f"Expected '100', got {string_future}"

    # Also test floats to string
    float_field = Field.of(
        pa.field("float_field", pa.float32()),
        past_default=3.14159,
        future_default=2.71828,
        consistency_type=SchemaConsistencyType.NONE,
    )

    string_past = float_field._cast_default_to_promoted_type(3.14159, pa.string())
    string_future = float_field._cast_default_to_promoted_type(2.71828, pa.string())

    assert string_past == "3.14159", f"Expected '3.14159', got {string_past}"
    assert string_future == "2.71828", f"Expected '2.71828', got {string_future}"

    # Test that None defaults are handled correctly
    none_field = Field.of(
        pa.field("none_field", pa.int32()),
        past_default=None,
        future_default=42,
        consistency_type=SchemaConsistencyType.NONE,
    )

    none_past = none_field._cast_default_to_promoted_type(None, pa.string())
    valid_future = none_field._cast_default_to_promoted_type(42, pa.string())

    assert none_past is None, f"None should remain None, got {none_past}"
    assert valid_future == "42", f"Expected '42', got {valid_future}"
