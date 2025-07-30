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
from deltacat.exceptions import SchemaCompatibilityError


@pytest.fixture(scope="function")
def base_schema():
    """Simple base schema for testing SchemaUpdate operations."""
    return Schema.of([
        Field.of(pa.field("id", pa.int64(), nullable=False), field_id=1, is_merge_key=True),
        Field.of(pa.field("name", pa.string(), nullable=True), field_id=2),
        Field.of(pa.field("age", pa.int32(), nullable=True), field_id=3),
    ])


@pytest.fixture(scope="function")
def complex_schema():
    """More complex schema for advanced testing."""
    return Schema.of([
        Field.of(pa.field("user_id", pa.int64(), nullable=False), field_id=1, is_merge_key=True),
        Field.of(pa.field("email", pa.string(), nullable=False), field_id=2),
        Field.of(pa.field("score", pa.float32(), nullable=True), field_id=3),
        Field.of(pa.field("metadata", pa.struct([
            pa.field("created_at", pa.timestamp('us')),
            pa.field("tags", pa.list_(pa.string()))
        ]), nullable=True), field_id=4),
    ])


class TestSchemaUpdate:
    """Comprehensive tests for SchemaUpdate class."""
    
    def test_init(self, base_schema):
        """Test SchemaUpdate initialization."""
        update = SchemaUpdate(base_schema)
        assert update.base_schema == base_schema
        assert not update.allow_incompatible_changes
        assert len(update._operations) == 0
        
        update_permissive = SchemaUpdate(base_schema, allow_incompatible_changes=True)
        assert update_permissive.allow_incompatible_changes
    
    def test_add_field_success(self, base_schema):
        """Test successfully adding a new nullable field."""
        new_field = Field.of(pa.field("email", pa.string(), nullable=True), field_id=4)
        
        update = SchemaUpdate(base_schema)
        result_schema = update.add_field("email", new_field).build()
        
        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("email")
        assert added_field.arrow.name == "email"
        assert added_field.arrow.type == pa.string()
        assert added_field.arrow.nullable == True
        assert added_field.id == 4
        assert result_schema.field("id") == base_schema.field("id")  # Original fields preserved
    
    def test_add_field_with_past_default(self, base_schema):
        """Test adding field with past_default is allowed."""
        new_field = Field.of(
            pa.field("status", pa.string(), nullable=False), 
            field_id=4,
            past_default="active"
        )
        
        update = SchemaUpdate(base_schema)
        result_schema = update.add_field("status", new_field).build()
        
        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("status")
        assert added_field.arrow.name == "status"
        assert added_field.arrow.type == pa.string()
        assert added_field.arrow.nullable == False
        assert added_field.id == 4
        assert added_field.past_default == "active"
    
    def test_add_field_with_future_default(self, base_schema):
        """Test adding field with future_default is allowed."""
        new_field = Field.of(
            pa.field("priority", pa.int32(), nullable=False),
            field_id=4,
            future_default=1
        )
        
        update = SchemaUpdate(base_schema)
        result_schema = update.add_field("priority", new_field).build()
        
        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("priority")
        assert added_field.arrow.name == "priority"
        assert added_field.arrow.type == pa.int32()
        assert added_field.arrow.nullable == False
        assert added_field.id == 4
        assert added_field.future_default == 1
    
    def test_add_field_non_nullable_without_defaults_fails(self, base_schema):
        """Test that adding non-nullable field without defaults fails."""
        new_field = Field.of(pa.field("required_field", pa.string(), nullable=False), field_id=4)
        
        update = SchemaUpdate(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field("required_field", new_field).build()
        
        assert "non-nullable field" in str(exc_info.value)
        assert "without default values" in str(exc_info.value)
        assert exc_info.value.field_locator == "required_field"
    
    def test_add_field_non_nullable_allowed_with_flag(self, base_schema):
        """Test adding non-nullable field succeeds with allow_incompatible_changes=True."""
        new_field = Field.of(pa.field("required_field", pa.string(), nullable=False), field_id=4)
        
        update = SchemaUpdate(base_schema, allow_incompatible_changes=True)
        result_schema = update.add_field("required_field", new_field).build()
        
        assert len(result_schema.fields) == 4
        # Verify the field was added with correct properties
        added_field = result_schema.field("required_field")
        assert added_field.arrow.name == "required_field"
        assert added_field.arrow.type == pa.string()
        assert added_field.arrow.nullable == False
        assert added_field.id == 4
    
    def test_add_existing_field_fails(self, base_schema):
        """Test that adding a field that already exists fails."""
        duplicate_field = Field.of(pa.field("name", pa.string()), field_id=5)
        
        update = SchemaUpdate(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field("name", duplicate_field).build()
        
        assert "already exists" in str(exc_info.value)
        assert exc_info.value.field_locator == "name"
    
    def test_remove_field_fails_by_default(self, base_schema):
        """Test that removing fields fails by default for compatibility."""
        update = SchemaUpdate(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.remove_field("age").build()
        
        assert "would break compatibility" in str(exc_info.value)
        assert "allow_incompatible_changes=True" in str(exc_info.value)
        assert exc_info.value.field_locator == "age"
    
    def test_remove_field_succeeds_with_flag(self, base_schema):
        """Test removing field succeeds with allow_incompatible_changes=True."""
        update = SchemaUpdate(base_schema, allow_incompatible_changes=True)
        result_schema = update.remove_field("age").build()
        
        assert len(result_schema.fields) == 2
        field_names = [f.path[0] for f in result_schema.fields if f.path]
        assert "age" not in field_names
        assert "id" in field_names
        assert "name" in field_names
    
    def test_remove_nonexistent_field_fails(self, base_schema):
        """Test removing a field that doesn't exist fails."""
        update = SchemaUpdate(base_schema, allow_incompatible_changes=True)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.remove_field("nonexistent").build()
        
        assert "does not exist" in str(exc_info.value)
        assert exc_info.value.field_locator == "nonexistent"
    
    def test_update_field_compatible_type_widening(self, base_schema):
        """Test updating field with compatible type widening (int32 -> int64)."""
        updated_field = Field.of(pa.field("age", pa.int64(), nullable=True), field_id=3)
        
        update = SchemaUpdate(base_schema)
        result_schema = update.update_field("age", updated_field).build()
        
        updated_age_field = result_schema.field("age")
        assert updated_age_field.arrow.type == pa.int64()
        assert updated_age_field.arrow.name == "age"
        assert updated_age_field.id == 3
    
    def test_update_field_compatible_nullability_change(self, base_schema):
        """Test making nullable field non-nullable is allowed (nullable->non-nullable)."""
        # This should be allowed since we're making the constraint more restrictive
        # but not breaking existing data (nulls can be handled by defaults)
        updated_field = Field.of(
            pa.field("name", pa.string(), nullable=False), 
            field_id=2,
            past_default="unknown",
            future_default="unknown"
        )
        
        update = SchemaUpdate(base_schema)
        result_schema = update.update_field("name", updated_field).build()
        
        updated_name_field = result_schema.field("name")
        assert not updated_name_field.arrow.nullable
        assert updated_name_field.arrow.name == "name"
        assert updated_name_field.id == 2
        assert updated_name_field.past_default == "unknown"
        assert updated_name_field.future_default == "unknown"
    
    def test_update_field_incompatible_nullability_fails(self, base_schema):
        """Test making nullable field non-nullable fails without defaults."""
        updated_field = Field.of(pa.field("name", pa.string(), nullable=False), field_id=2)
        
        update = SchemaUpdate(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field("name", updated_field).build()
        
        assert "non-nullable without" in str(exc_info.value)
        assert "past_default and future_default" in str(exc_info.value)
    
    def test_update_field_incompatible_type_fails(self, base_schema):
        """Test updating field with incompatible type change fails."""
        # int32 -> string is incompatible
        updated_field = Field.of(pa.field("age", pa.string(), nullable=True), field_id=3)
        
        update = SchemaUpdate(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field("age", updated_field).build()
        
        assert "would break compatibility" in str(exc_info.value)
        assert "PyArrow, Pandas, Polars, Ray Data, and Daft" in str(exc_info.value)
    
    def test_update_field_incompatible_allowed_with_flag(self, base_schema):
        """Test incompatible field update succeeds with allow_incompatible_changes=True."""
        updated_field = Field.of(pa.field("age", pa.string(), nullable=True), field_id=3)
        
        update = SchemaUpdate(base_schema, allow_incompatible_changes=True)
        result_schema = update.update_field("age", updated_field).build()
        
        updated_age_field = result_schema.field("age")
        assert updated_age_field.arrow.type == pa.string()
        assert updated_age_field.arrow.name == "age"
        assert updated_age_field.id == 3
    
    def test_update_nonexistent_field_fails(self, base_schema):
        """Test updating a field that doesn't exist fails."""
        updated_field = Field.of(pa.field("nonexistent", pa.string()), field_id=99)
        
        update = SchemaUpdate(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.update_field("nonexistent", updated_field).build()
        
        assert "does not exist" in str(exc_info.value)
    
    def test_method_chaining(self, base_schema):
        """Test that SchemaUpdate methods support fluent chaining."""
        new_field1 = Field.of(pa.field("email", pa.string(), nullable=True), field_id=4)
        new_field2 = Field.of(pa.field("score", pa.float64(), nullable=True), field_id=5)
        updated_age = Field.of(pa.field("age", pa.int64(), nullable=True), field_id=3)
        
        result_schema = (SchemaUpdate(base_schema)
                        .add_field("email", new_field1)
                        .add_field("score", new_field2)
                        .update_field("age", updated_age)
                        .build())
        
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
            pa.field("preferences", pa.struct([
                pa.field("theme", pa.string()),
                pa.field("notifications", pa.bool_())
            ]), nullable=True),
            field_id=5
        )
        
        update = SchemaUpdate(complex_schema)
        result_schema = update.add_field("preferences", new_struct_field).build()
        
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
        update1 = SchemaUpdate(base_schema)
        result1 = update1.add_field("test", new_field).build()
        assert len(result1.fields) == 4
        
        # Test list locator (nested field path)
        update2 = SchemaUpdate(base_schema)
        result2 = update2.add_field(["test"], new_field).build()
        assert len(result2.fields) == 4
        
        # Test int locator for updates (using existing field ID)
        updated_field = Field.of(pa.field("age", pa.int64(), nullable=True), field_id=3)
        update3 = SchemaUpdate(base_schema)
        result3 = update3.update_field(3, updated_field).build()  # Update by field ID
        assert result3.field("age").arrow.type == pa.int64()
    
    def test_type_compatibility_validation(self):
        """Test the _is_type_compatible method with various type combinations."""
        base_schema_simple = Schema.of([
            Field.of(pa.field("test", pa.int32()), field_id=1)
        ])
        update = SchemaUpdate(base_schema_simple)
        
        # Test numeric widening (compatible)
        assert update._is_type_compatible(pa.int32(), pa.int64())
        assert update._is_type_compatible(pa.float32(), pa.float64())
        assert update._is_type_compatible(pa.int32(), pa.float64())
        
        # Test incompatible changes
        assert not update._is_type_compatible(pa.int64(), pa.int32())  # narrowing
        assert not update._is_type_compatible(pa.string(), pa.int32())  # different types
        assert not update._is_type_compatible(pa.float64(), pa.string())  # different types
        
        # Test string/binary compatibility
        assert update._is_type_compatible(pa.string(), pa.string())
        assert update._is_type_compatible(pa.binary(), pa.binary())
        
        # Test struct compatibility
        old_struct = pa.struct([pa.field("a", pa.int32())])
        new_struct_compatible = pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())])
        new_struct_incompatible = pa.struct([pa.field("b", pa.string())])  # missing field "a"
        
        assert update._is_type_compatible(old_struct, new_struct_compatible)
        assert not update._is_type_compatible(old_struct, new_struct_incompatible)
        
        # Test list compatibility
        assert update._is_type_compatible(pa.list_(pa.int32()), pa.list_(pa.int64()))
        assert not update._is_type_compatible(pa.list_(pa.int64()), pa.list_(pa.int32()))
    
    def test_error_field_locator_attribute(self, base_schema):
        """Test that SchemaCompatibilityError includes field_locator."""
        new_field = Field.of(pa.field("required", pa.string(), nullable=False), field_id=4)
        
        update = SchemaUpdate(base_schema)
        with pytest.raises(SchemaCompatibilityError) as exc_info:
            update.add_field("required", new_field).build()
        
        error = exc_info.value
        assert error.field_locator == "required"
        assert isinstance(error.field_locator, str)
    
    def test_operations_applied_in_order(self, base_schema):
        """Test that operations are applied in the order they were added."""
        # Add field, then remove it, then add it back
        new_field = Field.of(pa.field("temp", pa.string(), nullable=True), field_id=4)
        
        result_schema = (SchemaUpdate(base_schema, allow_incompatible_changes=True)
                        .add_field("temp", new_field)
                        .remove_field("temp")
                        .add_field("temp", new_field)
                        .build())
        
        # Should end up with the field present
        assert len(result_schema.fields) == 4
        # Verify the temp field was added back correctly
        temp_field = result_schema.field("temp")
        assert temp_field.arrow.name == "temp"
        assert temp_field.arrow.type == pa.string()
        assert temp_field.id == 4