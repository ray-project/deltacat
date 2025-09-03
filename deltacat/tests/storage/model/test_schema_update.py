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
    MAX_FIELD_ID_EXCLUSIVE,
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

    def test_add_field_id_overflow_raises_error(self):
        """Adding a field when max_field_id is MAX-1 should overflow and error.

        Base schema has field IDs at 0 and MAX_FIELD_ID_EXCLUSIVE - 1. Adding a
        new field should attempt to auto-assign the next ID which overflows back
        to 0, causing a duplicate ID error.
        """
        base = Schema.of(
            [
                Field.of(
                    pa.field("id_max_minus_one", pa.int64(), nullable=True),
                    field_id=MAX_FIELD_ID_EXCLUSIVE - 1,
                ),
            ]
        )

        # Add a new nullable field (compatibility-wise OK). The ID is ignored
        # and will be auto-assigned, which should overflow and raise ValueError.
        update = SchemaUpdate.of(base)
        new_field = Field.of(pa.field("overflow", pa.int64(), nullable=True))

        with pytest.raises(SchemaCompatibilityError):
            update.add_field(new_field).apply()

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
        original_field_id = base_schema.field_id("name")
        result_schema = (
            base_schema.update()
            .update_field_doc("name", "first name")
            .update_field_doc("name", "last name")
            .update_field_doc("name", "middle name")
            .update_field_doc("name", "full name")
            .apply()
        )
        # Verify that the result reflects the last rename operation
        actual_field = result_schema.field(original_field_id)
        assert actual_field.arrow.name == "name"
        assert actual_field.doc == "full name"
        assert actual_field.arrow.type == pa.string()
        assert actual_field.id == original_field_id

    def test_duplicate_field_id_validation_add_field(self, base_schema):
        """Test that adding a field with duplicate field ID succeeds because IDs are auto-assigned.

        Note: This test behavior changed after implementing automatic field ID assignment.
        User-specified field IDs are now ignored to prevent conflicts.
        """
        # Try to add a field with ID 2, which already exists for 'name' field
        # This should succeed because the user-specified ID will be ignored
        duplicate_id_field = Field.of(
            pa.field("new_field", pa.string(), nullable=True), field_id=2
        )

        update = SchemaUpdate.of(base_schema)
        result_schema = update.add_field(duplicate_id_field).apply()

        # Field should be added with auto-assigned ID (4), not the conflicting ID (2)
        new_field = result_schema.field("new_field")
        assert new_field.id == 4  # Auto-assigned, not 2
        assert new_field.arrow.name == "new_field"

        # Original field with ID 2 should be unchanged
        assert result_schema.field("name").id == 2

    def test_duplicate_field_id_validation_update_field(self, base_schema):
        """Test that updating a field to use duplicate field ID fails."""
        # Try to update 'age' field to use ID 1, which already exists for 'id' field
        updated_field = Field.of(pa.field("age", pa.int32(), nullable=True), field_id=1)

        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update._update_field("age", updated_field).apply()

        assert "duplicate field ID 1" in str(exc_info.value)
        assert exc_info.value.field_locator == "age"

    def test_cannot_remove_all_fields(self, base_schema):
        """Test that removing all fields fails."""

        update = SchemaUpdate.of(base_schema, True)
        with pytest.raises(ValueError) as exc_info:
            update.remove_field("name").remove_field("age").remove_field("id").apply()

        assert "Schema must contain at least one field." in str(exc_info.value)

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
        """Test that duplicate field IDs are prevented through auto-assignment even with allow_incompatible_changes=True.

        Note: This test behavior changed after implementing automatic field ID assignment.
        Duplicate field ID conflicts can no longer occur because IDs are auto-assigned.
        """

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
        # User specifies conflicting field ID, but it will be ignored and auto-assigned
        duplicate_id_field = Field.of(
            pa.field("new_field", pa.string(), nullable=True), field_id=1
        )

        update = SchemaUpdate.of(test_schema, allow_incompatible_changes=True)
        result_schema = update.add_field(duplicate_id_field).apply()

        # Field should be added with auto-assigned ID (4), not the conflicting ID (1)
        new_field = result_schema.field("new_field")
        assert new_field.id == 4  # Auto-assigned, not 1
        assert new_field.arrow.name == "new_field"

        # Original field with ID 1 should be unchanged
        assert result_schema.field("foo").id == 1

        # Verify no duplicate field IDs in final schema
        field_ids = [field.id for field in result_schema.fields]
        assert len(field_ids) == len(
            set(field_ids)
        ), f"Duplicate field IDs found: {field_ids}"

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

    def test_schema_update_convenience_method(self, base_schema):
        """Test Schema.update() convenience method."""
        # Start with a schema that has a field we can modify
        base_schema = (
            base_schema.update()
            .add_field(Field.of(pa.field("score", pa.int32(), nullable=True)))
            .apply()
        )

        # Test update_field_type (compatible type widening)
        result1 = base_schema.update().update_field_type("score", pa.int64()).apply()
        score_field = result1.field("score")
        assert score_field.arrow.type == pa.int64()
        assert score_field.arrow.name == "score"
        assert score_field.id == 4  # Auto-assigned, not 10

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
        assert renamed_field.id == 4  # Retains original auto-assigne field ID

        # Test method chaining
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

    def test_add_multiple_fields_unique_field_ids(self, base_schema):
        """Test adding multiple fields in one SchemaUpdate gets unique, incremental field IDs."""
        # Create multiple new fields with different types to add simultaneously
        # Note: Field IDs specified here will be ignored and auto-assigned
        new_field1 = Field.of(
            pa.field("email", pa.string(), nullable=True),
            field_id=999,  # Will be ignored, auto-assigned to 4
        )
        new_field2 = Field.of(
            pa.field("score", pa.float64(), nullable=True),
            field_id=888,  # Will be ignored, auto-assigned to 5
        )
        new_field3 = Field.of(
            pa.field("active", pa.bool_(), nullable=False),
            field_id=777,  # Will be ignored, auto-assigned to 6
            past_default=True,
        )
        new_field4 = Field.of(
            pa.field("created_at", pa.timestamp("us"), nullable=True),
            field_id=666,  # Will be ignored, auto-assigned to 7
        )

        # Add all fields in a single SchemaUpdate operation
        update = SchemaUpdate.of(base_schema)
        result_schema = (
            update.add_field(new_field1)
            .add_field(new_field2)
            .add_field(new_field3)
            .add_field(new_field4)
            .apply()
        )

        # Verify all fields were added successfully
        assert len(result_schema.fields) == 7  # 3 original + 4 new

        # Verify each field has the expected unique field ID
        email_field = result_schema.field("email")
        assert email_field.id == 4
        assert email_field.arrow.name == "email"
        assert email_field.arrow.type == pa.string()

        score_field = result_schema.field("score")
        assert score_field.id == 5
        assert score_field.arrow.name == "score"
        assert score_field.arrow.type == pa.float64()

        active_field = result_schema.field("active")
        assert active_field.id == 6
        assert active_field.arrow.name == "active"
        assert active_field.arrow.type == pa.bool_()
        assert active_field.past_default is True

        created_at_field = result_schema.field("created_at")
        assert created_at_field.id == 7
        assert created_at_field.arrow.name == "created_at"
        assert pa.types.is_timestamp(created_at_field.arrow.type)

        # Verify original fields are preserved
        assert result_schema.field("id").id == 1
        assert result_schema.field("name").id == 2
        assert result_schema.field("age").id == 3

        # Verify no duplicate field IDs exist
        field_ids = [field.id for field in result_schema.fields]
        assert len(field_ids) == len(
            set(field_ids)
        ), f"Duplicate field IDs found: {field_ids}"

        # Verify field IDs are sequential starting from max_field_id + 1
        expected_ids = [1, 2, 3, 4, 5, 6, 7]
        assert sorted(field_ids) == expected_ids

    def test_conflicting_operations_add_then_remove_same_field(self, base_schema):
        """Test conflicting operations: adding a field then removing the same field should raise ValueError."""
        new_field = Field.of(pa.field("temp", pa.string(), nullable=True), field_id=4)

        # Add field then remove the same field - should raise ValueError for conflicting operations
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        with pytest.raises(ValueError) as exc_info:
            (
                update.add_field(new_field)
                .remove_field("temp")  # Conflicts with add operation
                .apply()
            )

        assert "Conflicting operations detected on field 'temp'" in str(exc_info.value)

    def test_conflicting_operations_remove_then_add_same_field(self, base_schema):
        """Test conflicting operations: removing a field then adding it back should raise ValueError."""
        # Remove existing field then add it back - should raise ValueError for conflicting operations
        replacement_field = Field.of(
            pa.field("age", pa.int32(), nullable=True), field_id=3
        )

        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        with pytest.raises(ValueError) as exc_info:
            (
                update.remove_field("age")  # Remove existing field
                .add_field(replacement_field)  # Conflicts with remove operation
                .apply()
            )

        assert "Conflicting operations detected on field 'age'" in str(exc_info.value)

    def test_conflicting_operations_update_then_remove_same_field(self, base_schema):
        """Test conflicting operations: updating a field then removing it should raise ValueError."""
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        with pytest.raises(ValueError) as exc_info:
            (
                update.update_field_type("age", pa.int64())  # Update field type
                .remove_field("age")  # Conflicts with update operation
                .apply()
            )

        assert "Conflicting operations detected on field 'age'" in str(exc_info.value)

    def test_conflicting_operations_remove_then_update_same_field_fails(
        self, base_schema
    ):
        """Test conflicting operations: removing a field then trying to update it fails during method chaining.

        Note: This fails with AttributeError during update_field_type() call because _get_existing_field
        returns None for the removed field. This happens before our conflict validation in apply().
        The conflict detection catches most cases, but this specific order triggers the old behavior.
        """
        update = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)

        # Remove field first, then try to update it - fails during update_field_type() call
        with pytest.raises(AttributeError) as exc_info:
            (
                update.remove_field("age").update_field_type(  # Remove field
                    "age", pa.int64()
                )  # Fails here due to _get_existing_field returning None
            )

        assert "NoneType" in str(exc_info.value)
        assert "arrow" in str(exc_info.value)

    def test_multiple_updates_same_field_allowed(self, base_schema):
        """Test multiple updates to the same field are allowed and applied cumulatively."""
        update = SchemaUpdate.of(base_schema)
        result_schema = (
            update.update_field_type("age", pa.int64())  # First update
            .update_field_doc("age", "Age in years")  # Second update - should work
            .update_field_consistency_type(
                "age", SchemaConsistencyType.VALIDATE
            )  # Third update
            .apply()
        )

        # All updates should be applied cumulatively
        updated_field = result_schema.field("age")
        assert updated_field.arrow.type == pa.int64()
        assert updated_field.doc == "Age in years"
        assert updated_field.consistency_type == SchemaConsistencyType.VALIDATE
        assert updated_field.id == 3  # ID should remain same

    def test_multiple_updates_only_are_allowed_explicitly(self, base_schema):
        """Test that ONLY multiple update operations on same field are allowed - this demonstrates the refined logic."""
        update = SchemaUpdate.of(base_schema)

        # Multiple update operations should work
        result_schema = (
            update.update_field_type("age", pa.int64())
            .update_field_doc("age", "Updated age field")
            .update_field_consistency_type("age", SchemaConsistencyType.COERCE)
            .update_field_future_default("age", 25)
            .apply()
        )

        # All updates should be applied cumulatively
        age_field = result_schema.field("age")
        assert age_field.arrow.type == pa.int64()
        assert age_field.doc == "Updated age field"
        assert age_field.consistency_type == SchemaConsistencyType.COERCE
        assert age_field.future_default == 25
        assert age_field.id == 3  # Original ID preserved

    def test_non_conflicting_operations_succeed(self, base_schema):
        """Test that non-conflicting operations on different fields succeed."""
        new_field = Field.of(
            pa.field("email", pa.string(), nullable=True), field_id=999
        )

        update = SchemaUpdate.of(base_schema)
        result_schema = (
            update.add_field(new_field)  # Add new field "email"
            .update_field_type("age", pa.int64())  # Update different field "age"
            .update_field_doc("name", "Full name")  # Update different field "name"
            .apply()
        )

        # All operations should succeed since they target different fields
        assert len(result_schema.fields) == 4  # 3 original + 1 new

        # Verify new field was added with auto-assigned ID
        email_field = result_schema.field("email")
        assert email_field.id == 4  # Auto-assigned, not 999
        assert email_field.arrow.name == "email"

        # Verify updates were applied
        age_field = result_schema.field("age")
        assert age_field.arrow.type == pa.int64()
        assert age_field.id == 3  # Original ID preserved

        name_field = result_schema.field("name")
        assert name_field.doc == "Full name"
        assert name_field.id == 2  # Original ID preserved

    def test_add_duplicate_field_name_fails(self, base_schema):
        """Test adding a field with a name that already exists should fail."""
        # Try to add a field with same name as existing field
        duplicate_field = Field.of(
            pa.field("name", pa.int32(), nullable=True), field_id=4
        )

        update = SchemaUpdate.of(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field(duplicate_field).apply()

        assert "already exists" in str(exc_info.value)

    def test_add_field_ignores_user_specified_field_id(self, base_schema):
        """Test that add_field operations ignore user-specified field IDs and auto-assign sequentially.

        This ensures field ID uniqueness and prevents users from accidentally creating
        conflicts by specifying existing field IDs.
        """
        # Try to add fields with conflicting field IDs (should be ignored)
        new_field1 = Field.of(
            pa.field("email", pa.string(), nullable=True),
            field_id=1,  # Intentionally conflicts with existing "id" field
        )
        new_field2 = Field.of(
            pa.field("score", pa.float64(), nullable=True),
            field_id=2,  # Intentionally conflicts with existing "name" field
        )
        new_field3 = Field.of(
            pa.field("active", pa.bool_(), nullable=True),
            field_id=999,  # High number that should be ignored
        )

        update = SchemaUpdate.of(base_schema)
        result_schema = (
            update.add_field(new_field1)
            .add_field(new_field2)
            .add_field(new_field3)
            .apply()
        )

        # New fields should get auto-assigned field IDs starting from max_field_id + 1
        assert len(result_schema.fields) == 6  # 3 original + 3 new

        # Verify original fields keep their IDs
        assert result_schema.field("id").id == 1
        assert result_schema.field("name").id == 2
        assert result_schema.field("age").id == 3

        # Verify new fields get sequential auto-assigned IDs (ignoring user input)
        email_field = result_schema.field("email")
        score_field = result_schema.field("score")
        active_field = result_schema.field("active")

        assert email_field.id == 4  # Not 1 (user-specified)
        assert score_field.id == 5  # Not 2 (user-specified)
        assert active_field.id == 6  # Not 999 (user-specified)

        # Verify no duplicate field IDs
        field_ids = [field.id for field in result_schema.fields]
        assert len(field_ids) == len(
            set(field_ids)
        ), f"Duplicate field IDs found: {field_ids}"

    def test_update_field_preserves_original_field_id(self, base_schema):
        """Test that update operations preserve the original field's ID regardless of user input.

        This ensures field ID stability during updates - the field ID should never change
        when updating an existing field's properties.
        """
        # Create a field update with a different field ID (should be ignored)
        update = SchemaUpdate.of(base_schema)

        # Update field type - the field ID in this context should be ignored
        result_schema = update.update_field_type("age", pa.int64()).apply()

        # Field ID should remain the same as original (3), not change
        updated_field = result_schema.field("age")
        original_field = base_schema.field("age")

        assert updated_field.id == original_field.id  # Should be 3
        assert updated_field.arrow.type == pa.int64()  # Type should be updated
        assert updated_field.arrow.name == "age"  # Name should stay same

        # All other fields should keep their original IDs too
        assert result_schema.field("id").id == 1
        assert result_schema.field("name").id == 2

    def test_mixed_add_update_field_id_management(self, base_schema):
        """Test field ID management with mixed add and update operations.

        Updates should preserve existing field IDs, while adds should get new sequential IDs.
        """
        # Add a field with conflicting ID, then update an existing field
        new_field = Field.of(
            pa.field("email", pa.string(), nullable=True),
            field_id=2,  # Same as "name" field - should be ignored
        )

        update = SchemaUpdate.of(base_schema)
        result_schema = (
            update.add_field(new_field)  # Should get field_id=4, not 2
            .update_field_type("age", pa.int64())  # Should keep field_id=3
            .update_field_doc("name", "Full name")  # Should keep field_id=2
            .apply()
        )

        # Verify field ID assignments
        assert result_schema.field("id").id == 1  # Original
        assert result_schema.field("name").id == 2  # Original, updated doc
        assert result_schema.field("age").id == 3  # Original, updated type
        assert result_schema.field("email").id == 4  # New field, auto-assigned

        # Verify updates were applied
        assert result_schema.field("age").arrow.type == pa.int64()
        assert result_schema.field("name").doc == "Full name"
        assert result_schema.field("email").arrow.name == "email"

        # Verify no duplicates
        field_ids = [field.id for field in result_schema.fields]
        assert len(field_ids) == len(set(field_ids))

    def test_field_id_auto_assignment_with_gaps(self):
        """Test that field ID auto-assignment handles gaps in existing field IDs correctly.

        If the schema has field IDs [1, 3, 7], new fields should start from 8.
        """
        # Create a schema with gaps in field IDs
        schema_with_gaps = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=3),  # Gap at 2
                Field.of(pa.field("score", pa.float32()), field_id=7),  # Gap at 4,5,6
            ]
        )

        new_field = Field.of(
            pa.field("email", pa.string(), nullable=True),
            field_id=999,  # Should be ignored, auto-assigned to 8
        )

        update = SchemaUpdate.of(schema_with_gaps)
        result_schema = update.add_field(new_field).apply()

        # New field should get max_field_id + 1 = 7 + 1 = 8
        email_field = result_schema.field("email")
        assert email_field.id == 8  # Not 999 or any of the existing gaps

        # Original fields should be unchanged
        assert result_schema.field("id").id == 1
        assert result_schema.field("name").id == 3
        assert result_schema.field("score").id == 7

    def test_field_id_never_reused_after_max_field_removal(self):
        """Test that field IDs are never reused, even when max field ID is removed and same-named field added back.

        This ensures field ID uniqueness over schema evolution history - a field with the same name
        but added after removal gets a new field ID, making it clear it's a different field.
        """
        # Create schema with fields having IDs 1, 2, 3
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=2),
                Field.of(
                    pa.field("score", pa.float32()), field_id=3
                ),  # This has max field ID
            ]
        )

        # Step 1: Remove the field with the max field ID (score, ID=3)
        update1 = SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
        schema_after_remove = update1.remove_field("score").apply()

        # Verify the field is removed
        assert len(schema_after_remove.fields) == 2
        field_names = [f.path[0] for f in schema_after_remove.fields if f.path]
        assert "score" not in field_names
        assert "id" in field_names
        assert "name" in field_names

        # Max field ID should still be 3 (based on original schema)
        assert schema_after_remove.max_field_id == 3

        # Step 2: Add a field back with the same name ("score") but different type
        new_score_field = Field.of(
            pa.field(
                "score", pa.int32(), nullable=True
            ),  # Different type than original
            field_id=999,  # Will be ignored, should get ID 4 (not reuse 3)
        )

        update2 = SchemaUpdate.of(schema_after_remove)
        schema_after_add = update2.add_field(new_score_field).apply()

        # Verify field is added back
        assert len(schema_after_add.fields) == 3
        restored_score_field = schema_after_add.field("score")

        # New field should get ID 4 (max_field_id + 1), NOT reuse ID 3
        assert (
            restored_score_field.id == 4
        )  # Should be 4, not 3 (the removed field's ID)
        assert restored_score_field.arrow.name == "score"
        assert (
            restored_score_field.arrow.type == pa.int32()
        )  # Different type than original

        # Original fields should keep their IDs
        assert schema_after_add.field("id").id == 1
        assert schema_after_add.field("name").id == 2

        # Verify no duplicate field IDs in final schema
        field_ids = [field.id for field in schema_after_add.fields]
        assert len(field_ids) == len(
            set(field_ids)
        ), f"Duplicate field IDs found: {field_ids}"
        assert sorted(field_ids) == [1, 2, 4]  # Field ID 3 is permanently "retired"

    def test_field_id_never_reused_multiple_removes_adds(self):
        """Test field ID non-reuse with multiple remove/add cycles.

        This tests that field IDs continue incrementing even through multiple
        remove and add operations, ensuring each field gets a truly unique ID.
        """
        # Start with schema having IDs 1, 2, 3
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=2),
                Field.of(pa.field("score", pa.float32()), field_id=3),
            ]
        )

        # Remove field with ID 3, add new field -> should get ID 4
        step1_schema = (
            SchemaUpdate.of(base_schema, allow_incompatible_changes=True)
            .remove_field("score")
            .add_field(Field.of(pa.field("email", pa.string()), field_id=999))
            .apply()
        )
        assert step1_schema.field("email").id == 4

        # Remove field with ID 2, add new field -> should get ID 5
        step2_schema = (
            SchemaUpdate.of(step1_schema, allow_incompatible_changes=True)
            .remove_field("name")
            .add_field(Field.of(pa.field("phone", pa.string()), field_id=888))
            .apply()
        )
        assert step2_schema.field("phone").id == 5

        # Add back "name" field -> should get ID 6 (not reuse 2)
        step3_schema = (
            SchemaUpdate.of(step2_schema)
            .add_field(Field.of(pa.field("name", pa.string()), field_id=777))
            .apply()
        )
        restored_name_field = step3_schema.field("name")
        assert restored_name_field.id == 6  # Not 2 (the original name field's ID)

        # Final schema should have fields with IDs [1, 4, 5, 6]
        # IDs 2 and 3 are permanently "retired"
        field_ids = [field.id for field in step3_schema.fields]
        assert sorted(field_ids) == [1, 4, 5, 6]

        # Verify field names in final schema
        field_names = [f.path[0] for f in step3_schema.fields if f.path]
        assert sorted(field_names) == ["email", "id", "name", "phone"]

    def test_schema_update_increments_id_by_one(self, base_schema):
        """Test that SchemaUpdate.apply() increments schema ID by exactly 1."""
        # Create a schema with a specific schema ID
        test_schema = Schema.of(
            [
                Field.of(
                    pa.field("id", pa.int64(), nullable=False),
                    field_id=1,
                    is_merge_key=True,
                ),
                Field.of(pa.field("name", pa.string(), nullable=True), field_id=2),
                Field.of(pa.field("age", pa.int32(), nullable=True), field_id=3),
            ],
            schema_id=5,  # Explicitly set schema ID to 5
        )

        # Verify base schema has the expected ID
        assert test_schema.id == 5

        # Apply a schema update (add a new field)
        new_field = Field.of(pa.field("email", pa.string(), nullable=True), field_id=4)
        updated_schema = SchemaUpdate.of(test_schema).add_field(new_field).apply()

        # Verify the updated schema has ID = base_schema.id + 1
        assert updated_schema.id == 6  # 5 + 1
        assert len(updated_schema.fields) == 4

    def test_schema_update_increments_id_from_zero(self):
        """Test that schema ID increments correctly when starting from 0."""
        # Create a schema with default schema ID (0)
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=2),
            ]
        )

        # Verify base schema has default ID of 0
        assert base_schema.id == 0

        # Apply a schema update
        updated_schema = (
            base_schema.update()
            .update_field_type("name", pa.string())
            .update_field_doc("name", "Full name")
            .apply()
        )

        # Verify the updated schema has ID = 0 + 1 = 1
        assert updated_schema.id == 1

    def test_multiple_schema_updates_increment_sequentially(self):
        """Test that multiple schema updates increment ID sequentially."""
        # Start with schema ID 10
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=2),
            ],
            schema_id=10,
        )

        assert base_schema.id == 10

        # First update: should go from 10 to 11
        schema_v11 = (
            base_schema.update()
            .add_field(Field.of(pa.field("age", pa.int32(), nullable=True)))
            .apply()
        )
        assert schema_v11.id == 11

        # Second update: should go from 11 to 12
        schema_v12 = (
            schema_v11.update()
            .add_field(Field.of(pa.field("email", pa.string(), nullable=True)))
            .apply()
        )
        assert schema_v12.id == 12

        # Third update: should go from 12 to 13
        schema_v13 = (
            schema_v12.update()
            .update_field_consistency_type("name", SchemaConsistencyType.VALIDATE)
            .apply()
        )
        assert schema_v13.id == 13

    def test_schema_update_different_operation_types_increment_id(self):
        """Test that different types of schema operations all increment schema ID."""
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1, is_merge_key=True),
                Field.of(pa.field("name", pa.string()), field_id=2),
                Field.of(pa.field("age", pa.int32()), field_id=3),
            ],
            schema_id=100,
        )

        # Test add field operation
        add_result = (
            base_schema.update()
            .add_field(Field.of(pa.field("email", pa.string(), nullable=True)))
            .apply()
        )
        assert add_result.id == 101

        # Test update field operation
        update_result = (
            base_schema.update().update_field_type("age", pa.int64()).apply()
        )
        assert update_result.id == 101

        # Test rename field operation
        rename_result = base_schema.update().rename_field("name", "full_name").apply()
        assert rename_result.id == 101

        # Test remove field operation (with incompatible changes allowed)
        remove_result = (
            base_schema.update(allow_incompatible_changes=True)
            .remove_field("age")
            .apply()
        )
        assert remove_result.id == 101

        # Test update field documentation
        doc_result = (
            base_schema.update().update_field_doc("name", "Person's full name").apply()
        )
        assert doc_result.id == 101

    def test_schema_update_chained_operations_increment_once(self):
        """Test that multiple chained operations in one update increment ID by 1, not per operation."""
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=2),
                Field.of(pa.field("age", pa.int32()), field_id=3),
            ],
            schema_id=50,
        )

        # Chain multiple operations in a single SchemaUpdate
        chained_result = (
            base_schema.update()
            .add_field(Field.of(pa.field("email", pa.string(), nullable=True)))
            .add_field(Field.of(pa.field("phone", pa.string(), nullable=True)))
            .update_field_type("age", pa.int64())
            .update_field_doc("name", "Full name")
            .rename_field("id", "user_id")
            .apply()
        )

        # Even with 5 operations, schema ID should only increment by 1
        assert chained_result.id == 51  # 50 + 1, not 50 + 5

    def test_schema_subschema_operations_increment_id(self):
        """Test that subschema operations (add/delete/replace) also increment schema ID by 1."""
        # Create a base schema
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=2),
            ],
            schema_id=20,
        )

        # Test add_subschema operation
        add_subschema_result = base_schema.add_subschema(
            "user_profile",
            [
                Field.of(pa.field("email", pa.string()), field_id=3),
                Field.of(pa.field("age", pa.int32()), field_id=4),
            ],
        )
        assert add_subschema_result.id == 21  # 20 + 1

        # Test replace_subschema operation
        schema_with_subschema = base_schema.add_subschema(
            "test_subschema", [Field.of(pa.field("temp", pa.string()), field_id=5)]
        )
        replace_result = schema_with_subschema.replace_subschema(
            "test_subschema", [Field.of(pa.field("replaced", pa.int32()), field_id=6)]
        )
        assert replace_result.id == 22  # 21 + 1

        # Test delete_subschema operation
        delete_result = schema_with_subschema.delete_subschema("test_subschema")
        assert delete_result.id == 22  # 21 + 1

    def test_schema_id_increment_with_high_values(self):
        """Test that schema ID increment works correctly with high values."""
        # Test with a high schema ID to ensure no overflow issues
        high_id = 999999
        base_schema = Schema.of(
            [Field.of(pa.field("id", pa.int64()), field_id=1)],
            schema_id=high_id,
        )

        updated_schema = (
            base_schema.update()
            .add_field(Field.of(pa.field("name", pa.string(), nullable=True)))
            .apply()
        )

        assert updated_schema.id == high_id + 1

    def test_schema_id_preserved_in_failed_updates(self):
        """Test that schema ID is not incremented when schema updates fail."""
        base_schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), field_id=1),
                Field.of(pa.field("name", pa.string()), field_id=2),
            ],
            schema_id=42,
        )

        # Try an operation that should fail (adding non-nullable field without defaults)
        with pytest.raises(Exception):  # Could be SchemaCompatibilityError or other
            base_schema.update().add_field(
                Field.of(pa.field("required_field", pa.string(), nullable=False))
            ).apply()

        # Original schema should still have the same ID
        assert base_schema.id == 42

        # A successful update should still increment correctly
        success_schema = (
            base_schema.update()
            .add_field(Field.of(pa.field("optional_field", pa.string(), nullable=True)))
            .apply()
        )
        assert success_schema.id == 43

    def test_schema_id_increment_consistency_across_update_methods(self):
        """Test that schema ID increments consistently regardless of how SchemaUpdate is created."""
        base_schema = Schema.of(
            [Field.of(pa.field("id", pa.int64()), field_id=1)],
            schema_id=77,
        )

        # Method 1: Using Schema.update()
        result1 = (
            base_schema.update()
            .add_field(Field.of(pa.field("field1", pa.string(), nullable=True)))
            .apply()
        )
        assert result1.id == 78

        # Method 2: Using SchemaUpdate.of()
        result2 = (
            SchemaUpdate.of(base_schema)
            .add_field(Field.of(pa.field("field2", pa.string(), nullable=True)))
            .apply()
        )
        assert result2.id == 78

        # Both methods should produce the same schema ID increment
        assert result1.id == result2.id
