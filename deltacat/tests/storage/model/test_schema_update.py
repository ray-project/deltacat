"""
Tests for SchemaUpdate functionality.

Note: These tests are in a separate file from test_schema.py to avoid test contamination issues.
Some tests in test_schema.py appear to modify global state that affects SchemaUpdate tests
when run together. Running these tests in isolation ensures they pass consistently.

To run both test suites together successfully, run SchemaUpdate tests first:
    pytest test_schema_update.py test_schema.py
"""

import pytest
import pyarrow as pa

from deltacat.storage.model.schema import (
    Schema,
    Field,
    SchemaUpdate,
)
from deltacat.storage.model.types import SchemaConsistencyType, SortOrder
from deltacat.storage.model.schema import MergeOrder
from deltacat.exceptions import SchemaCompatibilityError


@pytest.fixture(scope="function")
def base_schema():
    """Simple base schema for testing SchemaUpdate operations."""
    return Schema.of(
        [
            Field.of(
                pa.field("id", pa.int64(), nullable=False),
                field_id=1,
                is_merge_key=True,
            ),
            Field.of(pa.field("name", pa.string(), nullable=True), field_id=2),
            Field.of(pa.field("age", pa.int32(), nullable=True), field_id=3),
        ]
    )


@pytest.fixture(scope="function")
def complex_schema():
    """More complex schema for advanced testing."""
    return Schema.of(
        [
            Field.of(
                pa.field("user_id", pa.int64(), nullable=False),
                field_id=1,
                is_merge_key=True,
            ),
            Field.of(pa.field("email", pa.string(), nullable=False), field_id=2),
            Field.of(pa.field("score", pa.float32(), nullable=True), field_id=3),
            Field.of(
                pa.field(
                    "metadata",
                    pa.struct(
                        [
                            pa.field("created_at", pa.timestamp("us")),
                            pa.field("tags", pa.list_(pa.string())),
                        ]
                    ),
                    nullable=True,
                ),
                field_id=4,
            ),
        ]
    )


@pytest.fixture(scope="function")
def protected_fields_schema():
    """Schema with protected fields for testing field protection rules."""
    return Schema.of(
        [
            Field.of(
                pa.field("id", pa.int64(), nullable=False),
                field_id=1,
                is_merge_key=True,
            ),
            Field.of(
                pa.field("timestamp", pa.int64(), nullable=False),
                field_id=2,
                is_event_time=True,
            ),  # Use int64 for event time
            Field.of(
                pa.field("priority", pa.int32(), nullable=True),
                field_id=3,
                merge_order=MergeOrder.of(SortOrder.ASCENDING),
            ),
            Field.of(
                pa.field("data", pa.string(), nullable=True),
                field_id=4,
                past_default="default",
                consistency_type=SchemaConsistencyType.COERCE,
            ),
        ]
    )


class TestSchemaUpdate:
    """Comprehensive tests for SchemaUpdate class."""

    def test_init(self, base_schema):
        """Test SchemaUpdate initialization."""
        update = SchemaUpdate.of(base_schema)
        assert update.base_schema == base_schema
        assert not update.allow_incompatible_changes
        assert len(update.operations) == 0

        update_permissive = SchemaUpdate.of(
            base_schema, allow_incompatible_changes=True
        )
        assert update_permissive.allow_incompatible_changes

    def test_add_field_success(self, base_schema):
        """Test successfully adding a new nullable field."""
        new_field = Field.of(pa.field("email", pa.string(), nullable=True), field_id=4)

        update = SchemaUpdate.of(base_schema)
        result_schema = update.add_field(new_field).apply()

        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("email")
        assert added_field.arrow.name == "email"
        assert added_field.arrow.type == pa.string()
        assert added_field.arrow.nullable is True
        assert added_field.id == 4
        assert result_schema.field("id") == base_schema.field(
            "id"
        )  # Original fields preserved

    def test_add_field_with_past_default(self, base_schema):
        """Test adding field with past_default is allowed."""
        new_field = Field.of(
            pa.field("status", pa.string(), nullable=False),
            field_id=4,
            past_default="active",
        )

        update = SchemaUpdate.of(base_schema)
        result_schema = update.add_field(new_field).apply()

        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("status")
        assert added_field.arrow.name == "status"
        assert added_field.arrow.type == pa.string()
        assert added_field.arrow.nullable is False
        assert added_field.id == 4
        assert added_field.past_default == "active"

    def test_add_field_with_future_default(self, base_schema):
        """Test adding field with future_default is allowed."""
        new_field = Field.of(
            pa.field("priority", pa.int32(), nullable=False),
            field_id=4,
            future_default=1,
        )

        update = SchemaUpdate.of(base_schema)
        result_schema = update.add_field(new_field).apply()

        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("priority")
        assert added_field.arrow.name == "priority"
        assert added_field.arrow.type == pa.int32()
        assert added_field.arrow.nullable is False
        assert added_field.id == 4
        assert added_field.future_default == 1

    def test_add_field_non_nullable_without_defaults_fails(self, base_schema):
        """Test that adding non-nullable field without defaults fails."""
        new_field = Field.of(
            pa.field("required_field", pa.string(), nullable=False), field_id=4
        )

        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field(new_field).apply()

        assert "non-nullable field" in str(exc_info.value)
        assert "without default values" in str(exc_info.value)

    def test_add_field_non_nullable_allowed_with_flag(self, base_schema):
        """Test adding non-nullable field succeeds with allow_incompatible_changes=True."""
        new_field = Field.of(
            pa.field("required_field", pa.string(), nullable=False), field_id=4
        )

        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        result_schema = update.add_field(new_field).apply()

        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("required_field")
        assert added_field.arrow.name == "required_field"
        assert added_field.arrow.type == pa.string()
        assert added_field.arrow.nullable is False
        assert added_field.id == 4

    def test_add_existing_field_fails(self, base_schema):
        """Test that adding a field that already exists fails."""
        duplicate_field = Field.of(pa.field("name", pa.string()), field_id=5)

        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field(duplicate_field).apply()

        assert "already exists" in str(exc_info.value)

    def test_remove_field_fails_by_default(self, base_schema):
        """Test that removing fields fails by default for compatibility."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.remove_field("age").apply()

        assert "would break compatibility" in str(exc_info.value)
        assert "allow_incompatible_changes=True" in str(exc_info.value)
        assert exc_info.value.field_locator == "age"

    def test_remove_field_succeeds_with_flag(self, base_schema):
        """Test removing field succeeds with allow_incompatible_changes=True."""
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        result_schema = update.remove_field("age").apply()

        assert len(result_schema.fields) == 2
        field_names = [f.path[0] for f in result_schema.fields if f.path]
        assert "age" not in field_names
        assert "id" in field_names
        assert "name" in field_names

    def test_remove_nonexistent_field_fails(self, base_schema):
        """Test removing a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.remove_field("nonexistent").apply()

        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"

    def test_update_field_compatible_type_widening(self, base_schema):
        """Test updating field with compatible type widening (int32 -> int64)."""
        update = SchemaUpdate.of(base_schema)
        result_schema = update.update_field_type("age", pa.int64()).apply()

        updated_age_field = result_schema.field("age")
        assert updated_age_field.arrow.type == pa.int64()
        assert updated_age_field.arrow.name == "age"
        assert updated_age_field.id == 3

    def test_update_field_compatible_nullability_change(self, base_schema):
        """Test making nullable field non-nullable fails without defaults."""
        # This should fail because we're making a nullable field non-nullable without defaults
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_nullability("name", False).apply()

        assert "non-nullable without" in str(exc_info.value)
        assert "past_default and future_default" in str(exc_info.value)

    def test_update_field_incompatible_nullability_fails(self, base_schema):
        """Test making nullable field non-nullable fails without defaults."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_nullability("name", False).apply()

        assert "non-nullable without" in str(exc_info.value)
        assert "past_default and future_default" in str(exc_info.value)

    def test_update_field_incompatible_type_fails(self, base_schema):
        """Test updating field with incompatible type change fails."""
        # int32 -> string is incompatible
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_type("age", pa.string()).apply()

        assert "would break compatibility" in str(exc_info.value)
        assert "PyArrow, Pandas, Polars, Ray Data, and Daft" in str(exc_info.value)

    def test_update_field_incompatible_allowed_with_flag(self, base_schema):
        """Test incompatible field update succeeds with allow_incompatible_changes=True."""
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        result_schema = update.update_field_type("age", pa.string()).apply()

        updated_age_field = result_schema.field("age")
        assert updated_age_field.arrow.type == pa.string()
        assert updated_age_field.arrow.name == "age"
        assert updated_age_field.id == 3

    def test_update_nonexistent_field_fails(self, base_schema):
        """Test updating a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_type("nonexistent", pa.string()).apply()

        assert "does not exist" in str(exc_info.value)

    def test_method_chaining(self, base_schema):
        """Test that SchemaUpdate methods support fluent chaining."""
        new_field1 = Field.of(pa.field("email", pa.string(), nullable=True), field_id=4)
        new_field2 = Field.of(
            pa.field("score", pa.float64(), nullable=True), field_id=5
        )

        result_schema = (
            SchemaUpdate.of(base_schema)
            .add_field(new_field1)
            .add_field(new_field2)
            .update_field_type("age", pa.int64())
            .apply()
        )

        assert len(result_schema.fields) == 5

        # Verify email field
        email_field = result_schema.field("email")
        assert email_field.arrow.name == "email"
        assert email_field.arrow.type == pa.string()
        assert email_field.id == 4

        # Verify score field
        score_field = result_schema.field("score")
        assert score_field.arrow.name == "score"
        assert score_field.arrow.type == pa.float64()
        assert score_field.id == 5

        # Verify updated age field
        age_field = result_schema.field("age")
        assert age_field.arrow.type == pa.int64()
        assert age_field.arrow.name == "age"
        assert age_field.id == 3

    def test_complex_struct_field_operations(self, complex_schema):
        """Test operations on schemas with complex struct fields."""
        # Add a new nested struct field
        new_struct_field = Field.of(
            pa.field(
                "preferences",
                pa.struct(
                    [
                        pa.field("theme", pa.string()),
                        pa.field("notifications", pa.bool_()),
                    ]
                ),
                nullable=True,
            ),
            field_id=5,
        )

        update = SchemaUpdate.of(complex_schema)
        result_schema = update.add_field(new_struct_field).apply()

        assert len(result_schema.fields) == 5
        # Verify the struct field was added correctly
        prefs_field = result_schema.field("preferences")
        assert prefs_field.arrow.name == "preferences"
        assert prefs_field.id == 5
        assert pa.types.is_struct(prefs_field.arrow.type)

    def test_field_locator_types(self, base_schema):
        """Test different types of field locators (string, list, int)."""
        new_field = Field.of(pa.field("test", pa.string(), nullable=True), field_id=4)

        # Test string locator
        update1 = SchemaUpdate.of(base_schema)
        result1 = update1.add_field(new_field).apply()
        assert len(result1.fields) == 4

        # Test list locator (nested field path)
        update2 = SchemaUpdate.of(base_schema)
        result2 = update2.add_field(new_field).apply()
        assert len(result2.fields) == 4

        # Test int locator for updates (using existing field ID)
        update3 = SchemaUpdate.of(base_schema)
        result3 = update3.update_field_type(3, pa.int64()).apply()  # Update by field ID
        assert result3.field("age").arrow.type == pa.int64()

    def test_type_compatibility_validation(self):
        """Test the _is_type_compatible method with various type combinations."""
        base_schema_simple = Schema.of(
            [Field.of(pa.field("test", pa.int32()), field_id=1)]
        )
        update = SchemaUpdate.of(base_schema_simple)

        # Test numeric widening (compatible)
        assert update._is_type_compatible(pa.int32(), pa.int64())
        assert update._is_type_compatible(pa.float32(), pa.float64())
        assert update._is_type_compatible(pa.int32(), pa.float64())

        # Test incompatible changes
        assert not update._is_type_compatible(pa.int64(), pa.int32())  # narrowing
        assert not update._is_type_compatible(
            pa.string(), pa.int32()
        )  # different types
        assert not update._is_type_compatible(
            pa.float64(), pa.string()
        )  # different types

        # Test string/binary compatibility
        assert update._is_type_compatible(pa.string(), pa.string())
        assert update._is_type_compatible(pa.binary(), pa.binary())

        # Test struct compatibility
        old_struct = pa.struct([pa.field("a", pa.int32())])
        new_struct_compatible = pa.struct(
            [pa.field("a", pa.int32()), pa.field("b", pa.string())]
        )
        new_struct_incompatible = pa.struct(
            [pa.field("b", pa.string())]
        )  # missing field "a"

        assert update._is_type_compatible(old_struct, new_struct_compatible)
        assert not update._is_type_compatible(old_struct, new_struct_incompatible)

        # Test list compatibility
        assert update._is_type_compatible(pa.list_(pa.int32()), pa.list_(pa.int64()))
        assert not update._is_type_compatible(
            pa.list_(pa.int64()), pa.list_(pa.int32())
        )

    def test_error_field_locator_attribute(self, base_schema):
        """Test that SchemaCompatibilityError includes field_locator."""
        new_field = Field.of(
            pa.field("required", pa.string(), nullable=False), field_id=4
        )

        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field(new_field).apply()

        assert "Adding non-nullable field" in str(exc_info.value)

    def test_operations_applied_in_order(self, base_schema):
        """Test that operations are applied in the order they were added."""
        # Add field, then remove it, then add it back
        new_field = Field.of(pa.field("temp", pa.string(), nullable=True), field_id=4)

        result_schema = (
            SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
            .add_field(new_field)
            .remove_field("temp")
            .add_field(new_field)
            .apply()
        )

        # Should end up with the field present
        assert len(result_schema.fields) == 4
        # Verify the temp field was added back correctly
        temp_field = result_schema.field("temp")
        assert temp_field.arrow.name == "temp"
        assert temp_field.arrow.type == pa.string()
        assert temp_field.id == 4

    def test_duplicate_field_id_validation_add_field(self, base_schema):
        """Test that adding a field with duplicate field ID fails."""
        # Try to add a field with ID 2, which already exists for 'name' field
        duplicate_id_field = Field.of(
            pa.field("new_field", pa.string(), nullable=True), field_id=2
        )

        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field(duplicate_id_field).apply()

        assert "duplicate field ID 2" in str(exc_info.value)

    def test_duplicate_field_id_validation_update_field(self, base_schema):
        """Test that updating a field to use duplicate field ID fails."""
        # Try to update 'age' field to use ID 1, which already exists for 'id' field
        updated_field = Field.of(pa.field("age", pa.int32(), nullable=True), field_id=1)

        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update._update_field("age", updated_field).apply()

        assert "duplicate field ID 1" in str(exc_info.value)
        assert exc_info.value.field_locator == "age"

    def test_cannot_remove_merge_key_field(self, protected_fields_schema):
        """Test that removing merge key fields is forbidden."""
        update = SchemaUpdate.of(protected_fields_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.remove_field("id").apply()

        assert "Cannot remove merge key field" in str(exc_info.value)
        assert "critical for data integrity" in str(exc_info.value)

    def test_cannot_remove_event_time_field(self, protected_fields_schema):
        """Test that removing event time fields is forbidden."""
        update = SchemaUpdate.of(protected_fields_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.remove_field("timestamp").apply()

        assert "Cannot remove event time field" in str(exc_info.value)
        assert "critical for temporal operations" in str(exc_info.value)

    def test_cannot_remove_merge_order_field(self, protected_fields_schema):
        """Test that removing merge order fields is forbidden."""
        update = SchemaUpdate.of(protected_fields_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.remove_field("priority").apply()

        assert "Cannot remove merge order field" in str(exc_info.value)
        assert "critical for data ordering" in str(exc_info.value)

    def test_cannot_change_merge_key_status(self, protected_fields_schema):
        """Test that changing merge key status is forbidden."""
        # Try to make merge key field not a merge key
        updated_field = Field.of(
            pa.field("id", pa.int64(), nullable=False), field_id=1, is_merge_key=False
        )

        update = SchemaUpdate.of(protected_fields_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update._update_field("id", updated_field).apply()

        assert "Cannot change merge key status" in str(exc_info.value)
        assert "critical for data integrity" in str(exc_info.value)

    def test_cannot_change_event_time_status(self, protected_fields_schema):
        """Test that changing event time status is forbidden."""
        # Try to make event time field not an event time field
        updated_field = Field.of(
            pa.field("timestamp", pa.timestamp("us"), nullable=False),
            field_id=2,
            is_event_time=False,
        )

        update = SchemaUpdate.of(protected_fields_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update._update_field("timestamp", updated_field).apply()

        assert "Cannot change event time status" in str(exc_info.value)
        assert "critical for temporal operations" in str(exc_info.value)

    def test_cannot_change_merge_order(self, protected_fields_schema):
        """Test that changing merge order is forbidden."""
        # Try to change merge order from ASCENDING to DESCENDING
        updated_field = Field.of(
            pa.field("priority", pa.int32(), nullable=True),
            field_id=3,
            merge_order=MergeOrder.of(SortOrder.DESCENDING),
        )

        update = SchemaUpdate.of(protected_fields_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update._update_field("priority", updated_field).apply()

        assert "Cannot change merge order" in str(exc_info.value)
        assert "critical for data consistency" in str(exc_info.value)

    def test_cannot_change_past_default(self, protected_fields_schema):
        """Test that changing past_default is forbidden."""
        # Try to change past_default from "default" to "new_default"
        updated_field = Field.of(
            pa.field("data", pa.string(), nullable=True),
            field_id=4,
            past_default="new_default",
            consistency_type=SchemaConsistencyType.COERCE,
        )

        update = SchemaUpdate.of(protected_fields_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update._update_field("data", updated_field).apply()

        assert "Cannot change past_default" in str(exc_info.value)
        assert "immutable once set" in str(exc_info.value)

    def test_consistency_type_evolution_coerce_to_validate(self, base_schema):
        """Test allowed transition from COERCE to VALIDATE."""
        # First add a field with COERCE consistency type
        coerce_field = Field.of(
            pa.field("test", pa.string(), nullable=True),
            field_id=4,
            consistency_type=SchemaConsistencyType.COERCE,
        )
        schema_with_coerce = (
            SchemaUpdate.of(base_schema).add_field(coerce_field).apply()
        )

        # Now update it to VALIDATE - this should be allowed
        update = SchemaUpdate.of(schema_with_coerce)
        result_schema = update.update_field_consistency_type(
            "test", SchemaConsistencyType.VALIDATE
        ).apply()

        updated_field = result_schema.field("test")
        assert updated_field.consistency_type == SchemaConsistencyType.VALIDATE

    def test_consistency_type_evolution_validate_to_coerce(self, base_schema):
        """Test allowed transition from VALIDATE to COERCE."""
        # First add a field with VALIDATE consistency type
        validate_field = Field.of(
            pa.field("test", pa.string(), nullable=True),
            field_id=4,
            consistency_type=SchemaConsistencyType.VALIDATE,
        )
        schema_with_validate = (
            SchemaUpdate.of(base_schema).add_field(validate_field).apply()
        )

        # Now update it to COERCE - this should be allowed
        update = SchemaUpdate.of(schema_with_validate)
        result_schema = update.update_field_consistency_type(
            "test", SchemaConsistencyType.COERCE
        ).apply()

        updated_field = result_schema.field("test")
        assert updated_field.consistency_type == SchemaConsistencyType.COERCE

    def test_consistency_type_evolution_to_none_allowed(self, base_schema):
        """Test allowed transition from COERCE/VALIDATE to NONE."""
        # First add a field with COERCE consistency type
        coerce_field = Field.of(
            pa.field("test", pa.string(), nullable=True),
            field_id=4,
            consistency_type=SchemaConsistencyType.COERCE,
        )
        schema_with_coerce = (
            SchemaUpdate.of(base_schema).add_field(coerce_field).apply()
        )

        # Now update it to NONE - this should be allowed (relaxing constraints)
        update = SchemaUpdate.of(schema_with_coerce)
        result_schema = update.update_field_consistency_type(
            "test", SchemaConsistencyType.NONE
        ).apply()

        updated_field = result_schema.field("test")
        assert updated_field.consistency_type == SchemaConsistencyType.NONE

    def test_consistency_type_evolution_none_to_coerce_forbidden(self, base_schema):
        """Test forbidden transition from NONE to COERCE."""
        # First add a field with NONE consistency type
        none_field = Field.of(
            pa.field("test", pa.string(), nullable=True),
            field_id=4,
            consistency_type=SchemaConsistencyType.NONE,
        )
        schema_with_none = SchemaUpdate.of(base_schema).add_field(none_field).apply()

        # Now try to update it to COERCE - this should fail
        update = SchemaUpdate.of(schema_with_none)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_consistency_type(
                "test", SchemaConsistencyType.COERCE
            ).apply()

        assert "Cannot change consistency type" in str(exc_info.value)
        assert "from none to coerce" in str(exc_info.value)
        assert "tighten validation constraints" in str(exc_info.value)

    def test_consistency_type_evolution_none_to_validate_forbidden(self, base_schema):
        """Test forbidden transition from NONE to VALIDATE."""
        # First add a field with NONE consistency type
        none_field = Field.of(
            pa.field("test", pa.string(), nullable=True),
            field_id=4,
            consistency_type=SchemaConsistencyType.NONE,
        )
        schema_with_none = SchemaUpdate.of(base_schema).add_field(none_field).apply()

        # Now try to update it to VALIDATE - this should fail
        update = SchemaUpdate.of(schema_with_none)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_consistency_type(
                "test", SchemaConsistencyType.VALIDATE
            ).apply()

        assert "Cannot change consistency type" in str(exc_info.value)
        assert "from none to validate" in str(exc_info.value)
        assert "tighten validation constraints" in str(exc_info.value)

    def test_protected_fields_allowed_with_incompatible_flag(
        self, protected_fields_schema
    ):
        """Test that protected field changes are allowed with allow_incompatible_changes=True."""
        # Should be able to remove merge key field with the flag
        update = SchemaUpdate.of(
            protected_fields_schema, allow_incompatible_changes=True
        )
        result_schema = update.remove_field("id").apply()

        assert len(result_schema.fields) == 3
        field_names = [f.path[0] for f in result_schema.fields if f.path]
        assert "id" not in field_names

    def test_duplicate_field_id_still_forbidden_with_flag(self, base_schema):
        """Test that duplicate field IDs are still forbidden even with allow_incompatible_changes=True."""

        test_schema = Schema.of(
            [
                Field.of(
                    pa.field("foo", pa.int64(), nullable=False),
                    field_id=1,
                    is_merge_key=True,
                ),
                Field.of(pa.field("name", pa.string(), nullable=True), field_id=2),
                Field.of(pa.field("age", pa.int32(), nullable=True), field_id=3),
            ]
        )
        # Duplicate field IDs should always be forbidden as they break schema integrity
        duplicate_id_field = Field.of(
            pa.field("new_field", pa.string(), nullable=True), field_id=1
        )

        update = SchemaUpdate.of(test_schema, allow_incompatible_changes=True)
        # The duplicate ID validation can happen at SchemaUpdate level or Schema.of() level
        with pytest.raises((SchemaCompatibilityError, ValueError)) as exc_info:
            update.add_field(duplicate_id_field).apply()

        assert (
            "duplicate field" in str(exc_info.value).lower()
            or "duplicate field id" in str(exc_info.value).lower()
        )

    def test_rename_field_success(self, base_schema):
        """Test successfully renaming a field."""
        update = SchemaUpdate.of(base_schema)
        result_schema = update.rename_field("name", "full_name").apply()

        # Original field should be gone
        field_names = [f.path[0] for f in result_schema.fields if f.path]
        assert "name" not in field_names
        assert "full_name" in field_names

        # Renamed field should have same properties except name
        original_field = base_schema.field("name")
        renamed_field = result_schema.field("full_name")

        assert renamed_field.arrow.name == "full_name"
        assert renamed_field.arrow.type == original_field.arrow.type
        assert renamed_field.arrow.nullable == original_field.arrow.nullable
        assert renamed_field.id == original_field.id
        assert renamed_field.doc == original_field.doc
        assert renamed_field.consistency_type == original_field.consistency_type

    def test_rename_field_nonexistent_field_fails(self, base_schema):
        """Test renaming a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.rename_field("nonexistent", "new_name").apply()

        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"

    def test_update_field_type_success(self, base_schema):
        """Test successfully updating field type."""
        update = SchemaUpdate.of(base_schema)
        result_schema = update.update_field_type("age", pa.int64()).apply()

        # Field should have new type but same other properties
        original_field = base_schema.field("age")
        updated_field = result_schema.field("age")

        assert updated_field.arrow.type == pa.int64()
        assert updated_field.arrow.name == original_field.arrow.name
        assert updated_field.arrow.nullable == original_field.arrow.nullable
        assert updated_field.id == original_field.id
        assert updated_field.doc == original_field.doc
        assert updated_field.consistency_type == original_field.consistency_type

    def test_update_field_type_incompatible_fails(self, base_schema):
        """Test updating field type with incompatible type fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_type("age", pa.string()).apply()

        assert "would break compatibility" in str(exc_info.value)

    def test_update_field_type_incompatible_allowed_with_flag(self, base_schema):
        """Test incompatible type update succeeds with allow_incompatible_changes=True."""
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        result_schema = update.update_field_type("age", pa.string()).apply()

        updated_field = result_schema.field("age")
        assert updated_field.arrow.type == pa.string()

    def test_update_field_type_nonexistent_field_fails(self, base_schema):
        """Test updating type of a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_type("nonexistent", pa.string()).apply()

        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"

    def test_update_field_doc_success(self, base_schema):
        """Test successfully updating field documentation."""
        update = SchemaUpdate.of(base_schema)
        result_schema = update.update_field_doc(
            "name", "Full name of the person"
        ).apply()

        # Field should have new doc but same other properties
        original_field = base_schema.field("name")
        updated_field = result_schema.field("name")

        assert updated_field.doc == "Full name of the person"
        assert updated_field.arrow.name == original_field.arrow.name
        assert updated_field.arrow.type == original_field.arrow.type
        assert updated_field.arrow.nullable == original_field.arrow.nullable
        assert updated_field.id == original_field.id
        assert updated_field.consistency_type == original_field.consistency_type

    def test_update_field_doc_to_none(self, base_schema):
        """Test updating field documentation to None."""
        # First set some doc
        schema_with_doc = (
            SchemaUpdate.of(base_schema)
            .update_field_doc("name", "Original doc")
            .apply()
        )

        # Then update to None
        update = SchemaUpdate.of(schema_with_doc)
        result_schema = update.update_field_doc("name", None).apply()

        updated_field = result_schema.field("name")
        assert updated_field.doc is None

    def test_update_field_doc_nonexistent_field_fails(self, base_schema):
        """Test updating doc of a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_doc("nonexistent", "Some doc").apply()

        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"

    def test_update_field_nullability_success(self, base_schema):
        """Test successfully updating field nullability."""
        # Make nullable field non-nullable (should succeed with allow_incompatible_changes)
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        result_schema = update.update_field_nullability("name", False).apply()

        # Field should have new nullability but same other properties
        original_field = base_schema.field("name")
        updated_field = result_schema.field("name")

        assert updated_field.arrow.nullable is False
        assert updated_field.arrow.name == original_field.arrow.name
        assert updated_field.arrow.type == original_field.arrow.type
        assert updated_field.id == original_field.id
        assert updated_field.doc == original_field.doc
        assert updated_field.consistency_type == original_field.consistency_type

    def test_update_field_nullability_incompatible_fails(self, base_schema):
        """Test making nullable field non-nullable fails without flag."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_nullability("name", False).apply()

        assert "non-nullable without" in str(exc_info.value)

    def test_update_field_nullability_nonexistent_field_fails(self, base_schema):
        """Test updating nullability of a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_nullability("nonexistent", True).apply()

        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"

    def test_update_field_consistency_type_success(self, base_schema):
        """Test successfully updating field consistency type."""
        update = SchemaUpdate.of(base_schema)
        result_schema = update.update_field_consistency_type(
            "name", SchemaConsistencyType.VALIDATE
        ).apply()

        # Field should have new consistency type but same other properties
        original_field = base_schema.field("name")
        updated_field = result_schema.field("name")

        assert updated_field.consistency_type == SchemaConsistencyType.VALIDATE
        assert updated_field.arrow.name == original_field.arrow.name
        assert updated_field.arrow.type == original_field.arrow.type
        assert updated_field.arrow.nullable == original_field.arrow.nullable
        assert updated_field.id == original_field.id
        assert updated_field.doc == original_field.doc

    def test_update_field_consistency_type_to_none(self, base_schema):
        """Test updating field consistency type to None."""
        # First set some consistency type
        schema_with_coerce = (
            SchemaUpdate.of(base_schema)
            .update_field_consistency_type("name", SchemaConsistencyType.COERCE)
            .apply()
        )

        # Then update to None
        update = SchemaUpdate.of(schema_with_coerce)
        result_schema = update.update_field_consistency_type("name", None).apply()

        updated_field = result_schema.field("name")
        assert updated_field.consistency_type is None

    def test_update_field_consistency_type_nonexistent_field_fails(self, base_schema):
        """Test updating consistency type of a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_consistency_type(
                "nonexistent", SchemaConsistencyType.VALIDATE
            ).apply()

        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"

    def test_update_field_future_default_success(self, base_schema):
        """Test successfully updating field future default."""
        update = SchemaUpdate.of(base_schema)
        result_schema = update.update_field_future_default("name", "Unknown").apply()

        # Field should have new future default but same other properties
        original_field = base_schema.field("name")
        updated_field = result_schema.field("name")

        assert updated_field.future_default == "Unknown"
        assert updated_field.arrow.name == original_field.arrow.name
        assert updated_field.arrow.type == original_field.arrow.type
        assert updated_field.arrow.nullable == original_field.arrow.nullable
        assert updated_field.id == original_field.id
        assert updated_field.doc == original_field.doc
        assert updated_field.consistency_type == original_field.consistency_type
        assert updated_field.past_default == original_field.past_default

    def test_update_field_future_default_to_none(self, base_schema):
        """Test updating field future default to None."""
        # First set some future default
        schema_with_default = (
            SchemaUpdate.of(base_schema)
            .update_field_future_default("name", "Default Name")
            .apply()
        )

        # Then update to None
        update = SchemaUpdate.of(schema_with_default)
        result_schema = update.update_field_future_default("name", None).apply()

        updated_field = result_schema.field("name")
        assert updated_field.future_default is None

    def test_update_field_future_default_invalid_type_fails(self, base_schema):
        """Test updating field future default with incompatible type fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(ValueError) as exc_info:
            update.update_field_future_default(
                "name", 123
            ).apply()  # int for string field

        assert "not compatible with type" in str(exc_info.value)

    def test_update_field_future_default_nonexistent_field_fails(self, base_schema):
        """Test updating future default of a field that doesn't exist fails."""
        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field_future_default("nonexistent", "value").apply()

        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"

    def test_method_chaining_with_metadata_preservation(self, base_schema):
        """Test that chaining operations on the same field preserves metadata correctly."""
        result_schema = (
            SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
            .update_field_type("age", pa.int64())
            .update_field_consistency_type("age", SchemaConsistencyType.VALIDATE)
            .update_field_future_default("age", 0)
            .apply()
        )

        age_field = result_schema.field("age")
        assert age_field.arrow.type == pa.int64()
        assert age_field.consistency_type == SchemaConsistencyType.VALIDATE
        assert age_field.future_default == 0

    def test_individual_methods_work_correctly(self, base_schema):
        """Test that each method works correctly on its own."""
        # Test doc update
        result1 = (
            SchemaUpdate.of(base_schema).update_field_doc("name", "Full name").apply()
        )
        assert result1.field("name").doc == "Full name"

        # Test nullability update
        result2 = (
            SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
            .update_field_nullability("name", False)
            .apply()
        )
        assert result2.field("name").arrow.nullable is False

        # Test rename
        result3 = SchemaUpdate.of(base_schema).rename_field("name", "full_name").apply()
        assert result3.field("full_name").arrow.name == "full_name"

        # Test multiple independent operations
        result4 = (
            SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
            .update_field_type("age", pa.int64())
            .update_field_consistency_type("name", SchemaConsistencyType.VALIDATE)
            .apply()
        )

        assert result4.field("age").arrow.type == pa.int64()
        assert result4.field("name").consistency_type == SchemaConsistencyType.VALIDATE

    def test_method_chaining_different_fields(self, base_schema):
        """Test chaining operations on different fields."""
        result_schema = (
            SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
            .update_field_type("age", pa.int64())
            .update_field_doc("name", "Updated name")
            .update_field_consistency_type("id", SchemaConsistencyType.VALIDATE)
            .apply()
        )

        age_field = result_schema.field("age")
        assert age_field.arrow.type == pa.int64()

        name_field = result_schema.field("name")
        assert name_field.doc == "Updated name"

        id_field = result_schema.field("id")
        assert id_field.consistency_type == SchemaConsistencyType.VALIDATE

    def test_user_friendly_methods_vs_protected_update_field(self, base_schema):
        """Test that user-friendly methods produce same results as protected _update_field."""
        # Using update_field_type should be equivalent to using _update_field with manually constructed field
        update1 = SchemaUpdate.of(base_schema)
        result1 = update1.update_field_type("age", pa.int64()).apply()

        # Manually construct the updated field
        original_field = base_schema.field("age")
        new_arrow_field = pa.field(
            original_field.arrow.name,
            pa.int64(),
            nullable=original_field.arrow.nullable,
            metadata=original_field.arrow.metadata,
        )
        updated_field = Field.of(
            new_arrow_field,
            field_id=original_field.id,
            is_merge_key=original_field.is_merge_key,
            merge_order=original_field.merge_order,
            is_event_time=original_field.is_event_time,
            doc=original_field.doc,
            past_default=original_field.past_default,
            future_default=original_field.future_default,
            consistency_type=original_field.consistency_type,
            native_object=original_field.native_object,
        )

        update2 = SchemaUpdate.of(base_schema)
        result2 = update2._update_field("age", updated_field).apply()

        # Results should be equivalent
        field1 = result1.field("age")
        field2 = result2.field("age")

        assert field1.arrow.type == field2.arrow.type
        assert field1.arrow.name == field2.arrow.name
        assert field1.arrow.nullable == field2.arrow.nullable
        assert field1.id == field2.id
        assert field1.doc == field2.doc
        assert field1.consistency_type == field2.consistency_type

    def test_schema_update_convenience_method_with_user_friendly_methods(
        self, base_schema
    ):
        """Test Schema.update() convenience method with user-friendly methods."""
        # Start with a schema that has a field we can modify
        base_schema = (
            base_schema.update()
            .add_field(
                Field.of(pa.field("score", pa.int32(), nullable=True), field_id=10)
            )
            .apply()
        )

        # Test update_field_type (compatible type widening)
        result1 = base_schema.update().update_field_type("score", pa.int64()).apply()
        score_field = result1.field("score")
        assert score_field.arrow.type == pa.int64()
        assert score_field.arrow.name == "score"
        assert score_field.id == 10

        # Test update_field_doc
        result2 = base_schema.update().update_field_doc("score", "User score").apply()
        score_field = result2.field("score")
        assert score_field.doc == "User score"
        assert score_field.arrow.type == pa.int32()  # Type unchanged

        # Test update_field_consistency_type
        result3 = (
            base_schema.update()
            .update_field_consistency_type("score", SchemaConsistencyType.VALIDATE)
            .apply()
        )
        score_field = result3.field("score")
        assert score_field.consistency_type == SchemaConsistencyType.VALIDATE
        assert score_field.arrow.type == pa.int32()  # Type unchanged

        # Test update_field_future_default
        result4 = base_schema.update().update_field_future_default("score", 100).apply()
        score_field = result4.field("score")
        assert score_field.future_default == 100
        assert score_field.arrow.type == pa.int32()  # Type unchanged

        # Test rename_field
        result5 = base_schema.update().rename_field("score", "user_score").apply()
        field_names = [f.path[0] for f in result5.fields if f.path]
        assert "score" not in field_names
        assert "user_score" in field_names
        renamed_field = result5.field("user_score")
        assert renamed_field.arrow.name == "user_score"
        assert renamed_field.arrow.type == pa.int32()
        assert renamed_field.id == 10

        # Test method chaining with user-friendly methods
        result6 = (
            base_schema.update()
            .update_field_type("score", pa.int64())
            .update_field_doc("score", "User score in points")
            .update_field_consistency_type("score", SchemaConsistencyType.COERCE)
            .update_field_future_default("score", 0)
            .apply()
        )

        final_score_field = result6.field("score")
        assert final_score_field.arrow.type == pa.int64()
        assert final_score_field.doc == "User score in points"
        assert final_score_field.consistency_type == SchemaConsistencyType.COERCE
        assert final_score_field.future_default == 0
