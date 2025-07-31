# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
import copy

import msgpack
from typing import Optional, Any, Dict, Union, List, Callable, Tuple

import pyarrow as pa
from pyarrow import ArrowInvalid
import pandas as pd
import numpy as np

from deltacat.constants import BYTES_PER_KIBIBYTE
from deltacat.exceptions import SchemaCompatibilityError
from deltacat.storage.model.types import (
    SchemaConsistencyType,
    SortOrder,
    NullOrder,
    Dataset,
)
from deltacat.types.tables import (
    get_table_length,
    to_pyarrow,
    from_pyarrow,
    get_dataset_type,
)
from deltacat.types.media import DatasetType
from deltacat import logs


# PyArrow Field Metadata Key used to set the Field ID when writing to Parquet.
# See: https://arrow.apache.org/docs/cpp/parquet.html#parquet-field-id
PARQUET_FIELD_ID_KEY_NAME = b"PARQUET:field_id"

# PyArrow Field Metadata Key used to store field documentation.
FIELD_DOC_KEY_NAME = b"DELTACAT:doc"

# PyArrow Field Metadata Key used to identify the field as a merge key.
FIELD_MERGE_KEY_NAME = b"DELTACAT:merge_key"

# PyArrow Field Metadata Key used to identify the field as a merge order key.
FIELD_MERGE_ORDER_KEY_NAME = b"DELTACAT:merge_order"

# PyArrow Field Metadata Key used to identify the field as an event time.
FIELD_EVENT_TIME_KEY_NAME = b"DELTACAT:event_time"

# PyArrow Field Metadata Key used to store field past default values.
FIELD_PAST_DEFAULT_KEY_NAME = b"DELTACAT:past_default"

# PyArrow Field Metadata Key used to store field future default values.
FIELD_FUTURE_DEFAULT_KEY_NAME = b"DELTACAT:future_default"

# PyArrow Field Metadata Key used to store field schema consistency type.
FIELD_CONSISTENCY_TYPE_KEY_NAME = b"DELTACAT:consistency_type"

# PyArrow Schema Metadata Key used to store schema ID value.
SCHEMA_ID_KEY_NAME = b"DELTACAT:schema_id"

# PyArrow Schema Metadata Key used to store named subschemas
SUBSCHEMAS_KEY_NAME = b"DELTACAT:subschemas"

# Set max field ID to INT32.MAX_VALUE - 200 for backwards-compatibility with
# Apache Iceberg, which sets aside this range for reserved fields
MAX_FIELD_ID_EXCLUSIVE = 2147483447

# Default name assigned to the base, unnamed single schema when a new named
# subschema is first added.
BASE_SCHEMA_NAME = "_base"

SchemaId = int
SchemaName = str
FieldId = int
FieldName = str
NestedFieldName = List[str]
FieldLocator = Union[FieldName, NestedFieldName, FieldId]


class SchemaUpdateOperation(tuple):
    """
    Represents a single schema update operation (add, remove, or update field).
    
    This class inherits from tuple and stores:
    - operation: str ("add", "remove", "update")  
    - field_locator: FieldLocator (name, path, or ID)
    - field: Optional[Field] (the field data for add/update operations)
    """
    
    @staticmethod
    def add_field(field_locator: FieldLocator, field: 'Field') -> 'SchemaUpdateOperation':
        """Create an operation to add a new field."""
        return SchemaUpdateOperation(("add", field_locator, field))
    
    @staticmethod
    def remove_field(field_locator: FieldLocator) -> 'SchemaUpdateOperation':
        """Create an operation to remove an existing field.""" 
        return SchemaUpdateOperation(("remove", field_locator, None))
    
    @staticmethod
    def update_field(field_locator: FieldLocator, field: 'Field') -> 'SchemaUpdateOperation':
        """Create an operation to update an existing field."""
        return SchemaUpdateOperation(("update", field_locator, field))
    
    @property
    def operation(self) -> str:
        """The operation type: 'add', 'remove', or 'update'."""
        return self[0]
    
    @property
    def field_locator(self) -> FieldLocator:
        """The field locator (name, path, or ID)."""
        return self[1]
    
    @property
    def field(self) -> Optional['Field']:
        """The field data (None for remove operations)."""
        return self[2]


class SchemaUpdateOperations(List[SchemaUpdateOperation]):
    """
    A list of schema update operations that can be applied to a schema.
    
    This class inherits from List[SchemaUpdateOperation] and provides convenience
    methods for creating and managing schema update operations.
    """
    
    @staticmethod
    def of(operations: List[SchemaUpdateOperation]) -> 'SchemaUpdateOperations':
        """Create a SchemaUpdateOperations list from a list of operations."""
        typed_operations = SchemaUpdateOperations()
        for operation in operations:
            if operation is not None and not isinstance(operation, SchemaUpdateOperation):
                operation = SchemaUpdateOperation(operation)
            typed_operations.append(operation)
        return typed_operations
    
    def __getitem__(self, item):
        """Override to ensure items are properly typed as SchemaUpdateOperation."""
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, SchemaUpdateOperation):
            self[item] = val = SchemaUpdateOperation(val)
        return val


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class MergeOrder(tuple):
    @staticmethod
    def of(
        sort_order: SortOrder = SortOrder.ASCENDING,
        null_order: NullOrder = NullOrder.AT_END,
    ) -> MergeOrder:
        return MergeOrder(
            (
                sort_order,
                null_order,
            )
        )

    @property
    def sort_order(self) -> Optional[SortOrder]:
        return SortOrder(self[0])

    @property
    def null_order(self) -> Optional[NullOrder]:
        return NullOrder(self[1])


class Field(dict):
    @staticmethod
    def of(
        field: pa.Field,
        field_id: Optional[FieldId] = None,
        is_merge_key: Optional[bool] = None,
        merge_order: Optional[MergeOrder] = None,
        is_event_time: Optional[bool] = None,
        doc: Optional[str] = None,
        past_default: Optional[Any] = None,
        future_default: Optional[Any] = None,
        consistency_type: Optional[SchemaConsistencyType] = None,
        path: Optional[NestedFieldName] = None,
        native_object: Optional[Any] = None,
    ) -> Field:
        """
        Creates a DeltaCAT field from a PyArrow base field. The DeltaCAT
        field contains a copy of the base field, but ensures that the
        PyArrow Field's metadata is also populated with optional metadata
        like documentation or metadata used within the context of a parent
        schema like field ids, merge keys, and default values.

        Args:
            field (pa.Field): Arrow base field.

            field_id (Optional[FieldId]): Unique ID of the field within its
            parent schema, or None if this field has no parent schema. If not
            given, then the field ID will be derived from the Arrow base
            field's "PARQUET:field_id" metadata key.

            is_merge_key (Optional[bool]): True if this Field is used as a merge
            key within its parent schema, False or None if it is not a merge
            key or has no parent schema. If not given, this will be derived from
            the Arrow base field's "DELTACAT:merge_key" metadata key. Merge keys
            are the default keys used to find matching records for equality
            deletes, upserts, and other equality-key-based merge operations.
            Must be a non-floating-point primitive type.

            merge_order (Optional[MergeOrder]): Merge order for this field
            within its parent schema. None if it is not used for merge order or
            has no parent schema. If not given, this will be derived from
            the Arrow base field's "DELTACAT:merge_order" metadata key. Merge
            order is used to determine the record kept amongst all records
            with matching merge keys for equality deletes, upserts, and other
            equality-key-based merge operations. Must be a primitive type.

            is_event_time (Optional[bool]): True if this Field is used to derive
            event time within its parent schema, False or None if it is not used
            or has no parent schema. If not given, this will be derived from
            the Arrow base field's "DELTACAT:event_time" metadata key. Event
            times are used to determine a stream's data completeness watermark.
            Must be an integer, float, or date type.

            doc (Optional[str]): Documentation for this field or None if this
            field has no documentation. If not given, then docs will be derived
            from the Arrow base field's "DELTACAT:doc" metadata key.

            past_default (Optional[Any]): Past default values for records
            written to the parent schema before this field was appended,
            or None if this field has no parent schema. If not given, this will
            be derived from the Arrow base field's "DELTACAT:past_default"
            metadata key. Must be coercible to the field's base arrow type.

            future_default (Optional[Any]): Future default values for records
            that omit this field in the parent schema they're written to, or
            None if this field has no parent schema. If not given, this will
            be derived from the Arrow base field's "DELTACAT:future_default"
            metadata key. Must be coercible to the field's base arrow type.

            consistency_type (Optional[SchemaConsistencyType]): Schema
            consistency type for records written to this field within the
            context of a parent schema, or None if the field has no parent
            schema. If not given, this will be derived from the Arrow base
            field's "DELTACAT:consistency_type" metadata key.

            path (Optional[NestedFieldName]): Fully qualified path of this
            field within its parent schema. Any manually specified path will
            be overwritten when this field is added to a schema.

            native_object (Optional[Any]): The native object, if any, that this
            field was originally derived from.
        Returns:
            A new DeltaCAT Field.
        """
        final_field = Field._build(
            field=field,
            field_id=Field._field_id(field) if field_id is None else field_id,
            is_merge_key=Field._is_merge_key(field)
            if is_merge_key is None
            else is_merge_key,
            merge_order=Field._merge_order(field)
            if merge_order is None
            else merge_order,
            is_event_time=Field._is_event_time(field)
            if is_event_time is None
            else is_event_time,
            doc=Field._doc(field) if doc is None else doc,
            past_default=Field._past_default(field)
            if past_default is None
            else past_default,
            future_default=Field._future_default(field)
            if future_default is None
            else future_default,
            consistency_type=Field._consistency_type(field)
            if consistency_type is None
            else consistency_type,
        )
        return Field(
            {
                "arrow": final_field,
                "path": copy.deepcopy(path),
                "nativeObject": native_object,
            }
        )

    @property
    def arrow(self) -> pa.Field:
        return self["arrow"]

    @property
    def id(self) -> Optional[FieldId]:
        return Field._field_id(self.arrow)

    @property
    def path(self) -> Optional[NestedFieldName]:
        return self.get("path")

    @property
    def is_merge_key(self) -> Optional[bool]:
        return Field._is_merge_key(self.arrow)

    @property
    def merge_order(self) -> Optional[MergeOrder]:
        return Field._merge_order(self.arrow)

    @property
    def is_event_time(self) -> Optional[bool]:
        return Field._is_event_time(self.arrow)

    @property
    def doc(self) -> Optional[str]:
        return Field._doc(self.arrow)

    @property
    def past_default(self) -> Optional[Any]:
        return Field._past_default(self.arrow)

    @property
    def future_default(self) -> Optional[Any]:
        return Field._future_default(self.arrow)

    @property
    def consistency_type(self) -> Optional[SchemaConsistencyType]:
        return Field._consistency_type(self.arrow)

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")

    @staticmethod
    def _field_id(field: pa.Field) -> Optional[FieldId]:
        field_id = None
        if field.metadata:
            bytes_val = field.metadata.get(PARQUET_FIELD_ID_KEY_NAME)
            field_id = int(bytes_val.decode()) if bytes_val else None
        return field_id

    @staticmethod
    def _doc(field: pa.Field) -> Optional[str]:
        doc = None
        if field.metadata:
            bytes_val = field.metadata.get(FIELD_DOC_KEY_NAME)
            doc = bytes_val.decode() if bytes_val else None
        return doc

    @staticmethod
    def _is_merge_key(field: pa.Field) -> Optional[bool]:
        is_merge_key = None
        if field.metadata:
            bytes_val = field.metadata.get(FIELD_MERGE_KEY_NAME)
            is_merge_key = bool(bytes_val.decode()) if bytes_val else None
        return is_merge_key

    @staticmethod
    def _merge_order(field: pa.Field) -> Optional[MergeOrder]:
        merge_order = None
        if field.metadata:
            bytes_val = field.metadata.get(FIELD_MERGE_ORDER_KEY_NAME)
            merge_order = msgpack.loads(bytes_val) if bytes_val else None
        return merge_order

    @staticmethod
    def _is_event_time(field: pa.Field) -> Optional[bool]:
        is_event_time = None
        if field.metadata:
            bytes_val = field.metadata.get(FIELD_EVENT_TIME_KEY_NAME)
            is_event_time = bool(bytes_val.decode()) if bytes_val else None
        return is_event_time

    @staticmethod
    def _past_default(field: pa.Field) -> Optional[Any]:
        default = None
        if field.metadata:
            bytes_val = field.metadata.get(FIELD_PAST_DEFAULT_KEY_NAME)
            default = msgpack.loads(bytes_val) if bytes_val else None
        return default

    @staticmethod
    def _future_default(field: pa.Field) -> Optional[Any]:
        default = None
        if field.metadata:
            bytes_val = field.metadata.get(FIELD_FUTURE_DEFAULT_KEY_NAME)
            default = msgpack.loads(bytes_val) if bytes_val else None
        return default

    @staticmethod
    def _consistency_type(field: pa.Field) -> Optional[SchemaConsistencyType]:
        t = None
        if field.metadata:
            bytes_val = field.metadata.get(FIELD_CONSISTENCY_TYPE_KEY_NAME)
            t = SchemaConsistencyType(bytes_val.decode()) if bytes_val else None
        return t

    @staticmethod
    def _validate_merge_key(field: pa.Field, consistency_type: Optional[SchemaConsistencyType] = None):
        # Note: large_strings were explicitly allowed for compatibility with PyIceberg Iceberg Schema to PyArrow converter
        if not (
            pa.types.is_string(field.type)
            or pa.types.is_primitive(field.type)
            or pa.types.is_large_string(field.type)
        ):
            raise ValueError(
                f"Merge key {field} must be a primitive type or large string."
            )
        
        # Merge key fields must have VALIDATE consistency type to prevent type promotion
        if consistency_type is not None and consistency_type != SchemaConsistencyType.VALIDATE:
            raise ValueError(
                f"Merge key field '{field.name}' must have VALIDATE consistency type, "
                f"got {consistency_type}. Type promotion is not allowed for merge keys."
            )
        
        if pa.types.is_floating(field.type):
            raise ValueError(f"Merge key {field} cannot be floating point.")

    @staticmethod
    def _validate_merge_order(field: pa.Field, consistency_type: Optional[SchemaConsistencyType] = None):
        if not pa.types.is_primitive(field.type):
            raise ValueError(f"Merge order {field} must be a primitive type.")
        
        # Merge order fields must have VALIDATE consistency type to prevent type promotion
        if consistency_type is not None and consistency_type != SchemaConsistencyType.VALIDATE:
            raise ValueError(
                f"Merge order field '{field.name}' must have VALIDATE consistency type, "
                f"got {consistency_type}. Type promotion is not allowed for merge order fields."
            )

    @staticmethod
    def _validate_event_time(field: pa.Field, consistency_type: Optional[SchemaConsistencyType] = None):
        if (
            not pa.types.is_integer(field.type)
            and not pa.types.is_floating(field.type)
            and not pa.types.is_date(field.type)
        ):
            raise ValueError(f"Event time {field} must be numeric or date type.")
        
        # Event time fields must have VALIDATE consistency type to prevent type promotion
        if consistency_type is not None and consistency_type != SchemaConsistencyType.VALIDATE:
            raise ValueError(
                f"Event time field '{field.name}' must have VALIDATE consistency type, "
                f"got {consistency_type}. Type promotion is not allowed for event time fields."
            )

    @staticmethod
    def _validate_default(
        default: Optional[Any],
        field: pa.Field,
    ) -> pa.Scalar:
        try:
            return pa.scalar(default, field.type)
        except ArrowInvalid:
            raise ValueError(
                f"Cannot treat default value `{default}` as type"
                f"`{field.type}` for field: {field}"
            )

    @staticmethod
    def _build(
        field: pa.Field,
        field_id: Optional[int],
        is_merge_key: Optional[bool],
        merge_order: Optional[MergeOrder],
        is_event_time: Optional[bool],
        doc: Optional[str],
        past_default: Optional[Any],
        future_default: Optional[Any],
        consistency_type: Optional[SchemaConsistencyType],
    ) -> pa.Field:
        # Auto-set future_default to past_default if past_default exists but future_default doesn't
        if past_default is not None and future_default is None:
            future_default = past_default
        
        # Default critical columns (merge key, merge order, event time) to VALIDATE consistency type
        # to prevent type promotion which could break merge semantics
        if consistency_type is None and (is_merge_key or merge_order or is_event_time):
            consistency_type = SchemaConsistencyType.VALIDATE
        
        meta = {}
        if is_merge_key:
            Field._validate_merge_key(field, consistency_type)
            meta[FIELD_MERGE_KEY_NAME] = str(is_merge_key)
        if merge_order:
            Field._validate_merge_order(field, consistency_type)
            meta[FIELD_MERGE_ORDER_KEY_NAME] = msgpack.dumps(merge_order)
        if is_event_time:
            Field._validate_event_time(field, consistency_type)
            meta[FIELD_EVENT_TIME_KEY_NAME] = str(is_event_time)
        if past_default is not None:
            Field._validate_default(past_default, field)
            meta[FIELD_PAST_DEFAULT_KEY_NAME] = msgpack.dumps(past_default)
        if future_default is not None:
            Field._validate_default(future_default, field)
            meta[FIELD_FUTURE_DEFAULT_KEY_NAME] = msgpack.dumps(future_default)
        if field_id is not None:
            meta[PARQUET_FIELD_ID_KEY_NAME] = str(field_id)
        if doc is not None:
            meta[FIELD_DOC_KEY_NAME] = doc
        if consistency_type is not None:
            meta[FIELD_CONSISTENCY_TYPE_KEY_NAME] = consistency_type.value
        return pa.field(
            name=field.name,
            type=field.type,
            nullable=field.nullable,
            metadata=meta,
        )

    def validate(
        self, 
        column_data: pa.Array,
    ) -> None:
        """Validate that data in a column matches this field's type and constraints.
        
        Args:
            column_data: PyArrow Array containing the column data to validate
            
        Raises:
            ValueError: If data doesn't match field requirements.
        """
        # Check if the data type matches the field type
        if not column_data.type.equals(self.arrow.type):
            raise ValueError(
                f"Data type mismatch for field '{self.arrow.name}': "
                f"expected {self.arrow.type}, got {column_data.type}"
            )
    
    def coerce(
        self, 
        column_data: pa.Array,
    ) -> pa.Array:
        """Coerce data in a column to match this field's type.
        
        Args:
            column_data: PyArrow Array containing the column data to coerce
            
        Returns:
            pa.Array: Coerced data matching this field's type
            
        Raises:
            ValueError: If data cannot be coerced to the field type
        """
        try:
            return pa.compute.cast(column_data, self.arrow.type)
        except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
            raise ValueError(
                f"Cannot coerce data for field '{self.arrow.name}' "
                f"from {column_data.type} to {self.arrow.type}: {e}"
            )

    def promote_type_if_needed(
        self, 
        column_data: pa.Array,
    ) -> Tuple[pa.Array, bool]:
        """Promote field type to accommodate new data when consistency type is NONE.
        
        This method implements intelligent type widening for fields with SchemaConsistencyType.NONE:
        - Numeric types: int8 → int16 → int32 → int64 → float32 → float64 → decimal
        - Any numeric → string (most permissive)
        - Any type → binary (most permissive)
        - Maintains nullability and metadata
        
        Args:
            column_data: PyArrow Array containing the column data
            
        Returns:
            Tuple[pa.Array, bool]: (data, type_was_promoted)
                - data: Either original data or data cast to promoted type
                - type_was_promoted: True if field type should be updated
        """
        current_type = self.arrow.type
        data_type = column_data.type
        
        # Early return if types are already compatible
        if current_type.equals(data_type):
            return column_data, False
            
        # Find the promoted type that can accommodate both types
        promoted_type = self._find_promoted_type(current_type, data_type)
        if promoted_type is None:
            return column_data, False
            
        # Handle type coercion vs promotion
        if promoted_type.equals(current_type):
            return self._coerce_to_current_type(column_data, current_type)
        else:
            return self._promote_to_new_type(column_data, promoted_type)
    
    def _coerce_to_current_type(
        self, 
        column_data: pa.Array, 
        current_type: pa.DataType,
    ) -> Tuple[pa.Array, bool]:
        """Try to coerce data to current type without promoting the field type."""
        try:
            coerced_data = pa.compute.cast(column_data, current_type)
            return coerced_data, False
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
            return column_data, False
    
    def _promote_to_new_type(
        self, 
        column_data: pa.Array, 
        promoted_type: pa.DataType,
    ) -> Tuple[pa.Array, bool]:
        """Try to cast data to the promoted type."""
        try:
            promoted_data = pa.compute.cast(column_data, promoted_type)
            return promoted_data, True
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
            # Try multi-step conversion for binary types
            if pa.types.is_binary(promoted_type):
                return self._convert_to_binary_via_string(column_data, promoted_type)
            return column_data, False
    
    def _convert_to_binary_via_string(
        self, 
        column_data: pa.Array, 
        binary_type: pa.DataType,
    ) -> Tuple[pa.Array, bool]:
        """Try converting to binary via string intermediate step."""
        try:
            string_data = pa.compute.cast(column_data, pa.string())
            binary_data = pa.compute.cast(string_data, binary_type)
            return binary_data, True
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
            return column_data, False
    
    def _cast_default_to_promoted_type(
        self, 
        default_value: Any, 
        promoted_type: pa.DataType,
    ) -> Optional[Any]:
        """Cast a default value to match a promoted type.
        
        Args:
            default_value: The original default value
            promoted_type: The new promoted type
            
        Returns:
            The default value cast to the promoted type, or None if casting fails
        """
        if default_value is None:
            return None
            
        try:
            # Create a scalar with the original default value
            original_scalar = pa.scalar(default_value)
            # Cast to the promoted type
            promoted_scalar = pa.compute.cast(original_scalar, promoted_type)
            # Return the Python value
            return promoted_scalar.as_py()
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError, ValueError):
            # If direct casting fails, try via string for binary promotion
            if pa.types.is_binary(promoted_type):
                try:
                    # Convert to string first, then to binary
                    string_scalar = pa.compute.cast(pa.scalar(default_value), pa.string())
                    binary_scalar = pa.compute.cast(string_scalar, promoted_type)
                    return binary_scalar.as_py()
                except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError, ValueError):
                    pass
            
            # If all casting attempts fail, return None to remove the default
            return None
    
    def _find_promoted_type(
        self, 
        current_type: pa.DataType, 
        new_type: pa.DataType,
    ) -> Optional[pa.DataType]:
        """Find the most specific type that can accommodate both current and new types.
        
        Returns the promoted type or None if no promotion is possible.
        """
        # Try within-hierarchy promotions first (most specific)
        promoted_type = self._try_within_hierarchy_promotion(current_type, new_type)
        if promoted_type is not None:
            return promoted_type
            
        # Try cross-type promotions (more permissive)
        return self._try_cross_type_promotion(current_type, new_type)
    
    def _try_within_hierarchy_promotion(
        self, 
        current_type: pa.DataType, 
        new_type: pa.DataType,
    ) -> Optional[pa.DataType]:
        """Try to promote types within the same type hierarchy."""
        type_hierarchies = self._get_type_hierarchies()
        
        for hierarchy in type_hierarchies:
            current_idx = self._find_type_in_hierarchy(current_type, hierarchy)
            new_idx = self._find_type_in_hierarchy(new_type, hierarchy)
            
            if current_idx is not None and new_idx is not None:
                # Both types are in same hierarchy - promote to the higher one
                promoted_idx = max(current_idx, new_idx)
                promoted_type = hierarchy[promoted_idx]
                return self._maybe_make_nullable(promoted_type, current_type, new_type)
        
        return None
    
    def _try_cross_type_promotion(
        self, 
        current_type: pa.DataType, 
        new_type: pa.DataType,
    ) -> Optional[pa.DataType]:
        """Try cross-type promotions in order of specificity."""
        promotions = self._get_cross_type_promotions()
        
        for source_types, target_types in promotions:
            if self._types_compatible_with_promotion_rule(current_type, new_type, source_types):
                # Find the most specific target type that works for both
                for target_type in target_types:
                    if (self._can_cast_to(current_type, target_type) and 
                        self._can_cast_to(new_type, target_type)):
                        return self._maybe_make_nullable(target_type, current_type, new_type)
        
        return None
    
    def _get_type_hierarchies(
        self,
    ) -> List[List[pa.DataType]]:
        """Get ordered type hierarchies for within-hierarchy promotions."""
        return [
            # Integer types (in order of promotion)
            [pa.int8(), pa.int16(), pa.int32(), pa.int64()],
            [pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()],
            # Float types
            [pa.float32(), pa.float64()],
            # Decimal types (most specific numeric)
            [pa.decimal128(28, 10)],  # Use reasonable precision
            # String types (most permissive for text)
            [pa.string(), pa.large_string()],
            # Binary types (most permissive overall)
            [pa.binary(), pa.large_binary()],
        ]
    
    def _get_cross_type_promotions(
        self,
    ) -> List[Tuple]:
        """Get cross-type promotions ordered by specificity (most specific first)."""
        return [
            # Any integer can promote to float (most specific numeric promotion)
            ([pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()],
             [pa.float32(), pa.float64()]),
            # Any numeric can promote to decimal
            ([pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(),
              pa.float32(), pa.float64()],
             [pa.decimal128(28, 10)]),
            # Any type except binary can promote to string
            ('not_binary', [pa.string(), pa.large_string()]),
            # Any type can promote to binary (most permissive, checked last)
            (None, [pa.binary(), pa.large_binary()]),
        ]
    
    def _types_compatible_with_promotion_rule(
        self, 
        current_type: pa.DataType, 
        new_type: pa.DataType, 
        source_types,
    ) -> bool:
        """Check if types are compatible with a specific promotion rule."""
        if source_types is None:  # any_to_* cases
            return True
        elif source_types == 'not_binary':  # any type except binary
            # For string promotion, require both types to be non-binary
            return (not pa.types.is_binary(current_type) and 
                   not pa.types.is_binary(new_type))
        else:
            # Check if at least one type is compatible with source types
            current_compatible = any(self._types_compatible(current_type, src) for src in source_types)
            new_compatible = any(self._types_compatible(new_type, src) for src in source_types)
            return current_compatible or new_compatible
    
    def _find_type_in_hierarchy(
        self, 
        data_type: pa.DataType, 
        hierarchy: List[pa.DataType],
    ) -> Optional[int]:
        """Find the index of a compatible type in the hierarchy."""
        for i, hierarchy_type in enumerate(hierarchy):
            if self._types_compatible(data_type, hierarchy_type):
                return i
        return None
    
    def _types_compatible(
        self, 
        type1: pa.DataType, 
        type2: pa.DataType,
    ) -> bool:
        """Check if two types are compatible (ignoring nullability)."""
        # For complex types like list and dictionary, compare their value types
        base_type1 = type1.value_type if isinstance(type1, (pa.ListType, pa.DictionaryType)) else type1
        base_type2 = type2.value_type if isinstance(type2, (pa.ListType, pa.DictionaryType)) else type2
        return base_type1.equals(base_type2)
    
    def _can_cast_to(
        self, 
        from_type: pa.DataType, 
        to_type: pa.DataType,
    ) -> bool:
        """Check if a type can be cast to another type."""
        # Null can cast to anything
        if from_type == pa.null():
            return True
            
        # Special handling for binary types (most permissive)
        if pa.types.is_binary(to_type):
            return self._can_cast_to_binary(from_type)
            
        # Test casting with a minimal array
        return self._test_cast_with_sample_array(from_type, to_type)
    
    def _can_cast_to_binary(
        self, 
        from_type: pa.DataType,
    ) -> bool:
        """Check if a type can be cast to binary (directly or via string)."""
        # Binary and string can always cast to binary
        if pa.types.is_binary(from_type) or pa.types.is_string(from_type):
            return True
        # For other types, check if they can cast to string first
        return self._can_cast_to(from_type, pa.string())
    
    def _test_cast_with_sample_array(
        self, 
        from_type: pa.DataType, 
        to_type: pa.DataType,
    ) -> bool:
        """Test casting by creating a sample array and attempting the cast."""
        try:
            test_array = self._create_sample_array(from_type)
            if test_array is None:
                return False
            pa.compute.cast(test_array, to_type)
            return True
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
            return False
    
    def _create_sample_array(
        self, 
        data_type: pa.DataType,
    ) -> Optional[pa.Array]:
        """Create a minimal sample array for testing type casting."""
        if isinstance(data_type, (pa.ListType, pa.DictionaryType)):  # complex types
            return pa.array([None], type=data_type)
        
        # Create appropriate test value based on type
        if pa.types.is_integer(data_type):
            return pa.array([0], type=data_type)
        elif pa.types.is_floating(data_type):
            return pa.array([0.0], type=data_type)
        elif pa.types.is_string(data_type):
            return pa.array([""], type=data_type)
        elif pa.types.is_binary(data_type):
            return pa.array([b""], type=data_type)
        else:
            return None  # Unknown type
    
    def _maybe_make_nullable(
        self, 
        base_type: pa.DataType, 
        type1: pa.DataType, 
        type2: pa.DataType,
    ) -> pa.DataType:
        """Make the base type nullable if either input type is nullable."""
        # PyArrow doesn't have a direct way to check nullability at the type level
        # This method assumes the caller will handle nullability at the field level
        # For now, return the base type as-is since nullability is handled by pa.field()
        return base_type


SingleSchema = Union[List[Field], pa.Schema]
MultiSchema = Union[Dict[SchemaName, List[Field]], Dict[SchemaName, pa.Schema]]


class Schema(dict):
    @staticmethod
    def of(
        schema: Union[SingleSchema, MultiSchema],
        schema_id: Optional[SchemaId] = None,
        native_object: Optional[Any] = None,
    ) -> Schema:
        """
        Creates a DeltaCAT schema from either one or multiple Arrow base schemas
        or lists of DeltaCAT fields. All field names across all input schemas
        must be unique (case-insensitive). If a dict of named subschemas is
        given, then this DeltaCAT schema will be backed by a unified arrow
        schema created as a union of all input schemas in the natural iteration
        order of their dictionary keys. This unified schema saves all named
        subschema field mappings in its metadata to support DeltaCAT subschema
        retrieval by name after schema creation.

        Args:
            schema (Union[SingleSchema, MultiSchema]): For a single unnamed
            schema, either an Arrow base schema or list of DeltaCAT fields.
            If an Arrow base schema is given, then a copy of the base schema
            is made with each Arrow field populated with additional metadata.
            Field IDs, merge keys, docs, and default vals will be read from
            each Arrow field's metadata if they exist. Any field missing a
            field ID will be assigned a unique field ID, with assigned field
            IDs either starting from 0 or the max field ID + 1.
            For multiple named subschemas, a dictionary of schema names to an
            arrow base schema or list of DeltaCAT fields. These schemas will
            be copied into a unified Arrow schema representing a union of all
            of their fields in their natural iteration order. Any missing
            field IDs will be autoassigned starting from 0 or the max field ID
            + 1 across the natural iteration order of all  schemas first, and
            all fields second.
            All fields across all schemas must have unique names
            (case-insensitive).

            schema_id (SchemaId): Unique ID of schema within its parent table
            version. Defaults to 0.

            native_object (Optional[Any]): The native object, if any, that this
            schema was converted from.
        Returns:
            A new DeltaCAT Schema.
        """
        # normalize the input as a unified pyarrow schema
        # if the input included multiple subschemas, then also save a mapping
        # from each subschema to its unique field names
        schema, subschema_to_field_names = Schema._to_unified_pyarrow_schema(schema)
        # discover assigned field IDs in the given pyarrow schema
        field_ids_to_fields = {}
        schema_metadata = {}
        visitor_dict = {"maxFieldId": 0}
        # find and save the schema's max field ID in the visitor dictionary
        Schema._visit_fields(
            current=schema,
            visit=Schema._find_max_field_id,
            visitor_dict=visitor_dict,
        )
        max_field_id = visitor_dict["maxFieldId"]
        visitor_dict["fieldIdsToFields"] = field_ids_to_fields
        # populate map of field IDs to DeltaCAT fields w/ IDs, docs, etc.
        Schema._visit_fields(
            current=schema,
            visit=Schema._populate_fields,
            visitor_dict=visitor_dict,
        )
        if schema.metadata:
            schema_metadata.update(schema.metadata)
        # populate merge keys
        merge_keys = [
            field.id for field in field_ids_to_fields.values() if field.is_merge_key
        ]
        # create a new pyarrow schema with field ID, doc, etc. field metadata
        pyarrow_schema = pa.schema(
            fields=[field.arrow for field in field_ids_to_fields.values()],
        )
        # map subschema field names to IDs (for faster lookup and reduced size)
        subschema_to_field_ids = {
            schema_name: [
                Field.of(pyarrow_schema.field(field_name)).id
                for field_name in field_names
            ]
            for schema_name, field_names in subschema_to_field_names.items()
        }
        # create a final pyarrow schema with populated schema metadata
        if schema_id is not None:
            schema_metadata[SCHEMA_ID_KEY_NAME] = str(schema_id)
        if schema_metadata.get(SCHEMA_ID_KEY_NAME) is None:
            schema_metadata[SCHEMA_ID_KEY_NAME] = str(0)
        schema_metadata[SUBSCHEMAS_KEY_NAME] = msgpack.dumps(subschema_to_field_ids)
        final_schema = pyarrow_schema.with_metadata(schema_metadata)
        return Schema(
            {
                "arrow": final_schema,
                "mergeKeys": merge_keys or None,
                "fieldIdsToFields": field_ids_to_fields,
                "maxFieldId": max_field_id,
                "nativeObject": native_object,
            }
        )

    @staticmethod
    def deserialize(serialized: pa.Buffer) -> Schema:
        return Schema.of(schema=pa.ipc.read_schema(serialized))

    def serialize(self) -> pa.Buffer:
        return self.arrow.serialize()

    def equivalent_to(self, other: Schema, check_metadata: bool = False):
        if other is None:
            return False
        if not isinstance(other, dict):
            return False
        if not isinstance(other, Schema):
            other = Schema(other)
        return self.arrow.equals(
            other.arrow,
            check_metadata,
        )

    def add_subschema(
        self,
        name: SchemaName,
        schema: SingleSchema,
    ) -> Schema:
        subschemas = copy.copy(self.subschemas)
        if not subschemas:  # self is SingleSchema
            subschemas = {BASE_SCHEMA_NAME: self}
        subschemas = Schema._add_subschema(name, schema, subschemas)
        return Schema.of(
            schema=subschemas,
            schema_id=self.id + 1,
        )

    def delete_subschema(self, name: SchemaName) -> Schema:
        subschemas = copy.copy(self.subschemas)
        subschemas = self._del_subschema(name, subschemas)
        if not subschemas:
            raise ValueError(f"Deleting `{name}` would leave the schema empty.")
        subschemas = {name: val.arrow for name, val in subschemas.items()}
        return Schema.of(
            schema=subschemas,
            schema_id=self.id + 1,
        )

    def replace_subschema(
        self,
        name: SchemaName,
        schema: SingleSchema,
    ) -> Schema:
        subschemas = copy.copy(self.subschemas)
        subschemas = Schema._del_subschema(name, subschemas)
        subschemas = Schema._add_subschema(name, schema, subschemas)
        return Schema.of(
            schema=subschemas,
            schema_id=self.id + 1,
        )

    def update(
        self,
        allow_incompatible_changes: bool = False
    ) -> 'SchemaUpdate':
        """
        Create a SchemaUpdate instance for safely evolving this schema.
        
        This method provides a convenient way to create a SchemaUpdate for this schema
        without needing to call SchemaUpdate.of() directly.
        
        Args:
            allow_incompatible_changes: If True, allows changes that may break
                backward compatibility. If False (default), raises SchemaCompatibilityError
                for incompatible changes.
                
        Returns:
            A new SchemaUpdate instance configured for this schema
            
        Example:
            >>> schema = Schema.of([Field.of(pa.field("id", pa.int64()))])
            >>> new_field = Field.of(pa.field("name", pa.string()))
            >>> updated_schema = (schema.update()
            ...                         .add_field("name", new_field)
            ...                         .apply())
        """
        return SchemaUpdate.of(self, allow_incompatible_changes=allow_incompatible_changes)

    def field_id(self, name: Union[FieldName, NestedFieldName]) -> FieldId:
        return Schema._field_name_to_field_id(self.arrow, name)

    def field_name(self, field_id: FieldId) -> Union[FieldName, NestedFieldName]:
        field = self.field_ids_to_fields[field_id]
        if len(field.path) == 1:
            return field.arrow.name
        return field.path

    def field(self, field_locator: FieldLocator) -> Field:
        field_id = (
            field_locator
            if isinstance(field_locator, FieldId)
            else self.field_id(field_locator)
        )
        return self.field_ids_to_fields[field_id]

    def merge_order_sort_keys(self) -> Optional[List["SortKey"]]:
        """Extract sort keys from fields with merge_order defined, or use event_time as fallback.
        
        If explicit merge_order fields are defined, they take precedence.
        If no merge_order fields are defined but an event_time field exists, use event_time
        with DESCENDING merge_order (keep latest events by default).
        
        Note: The sort order is inverted because deduplication keeps the "last" record
        after sorting. To keep the record with the smallest merge_order value, we need
        to sort in DESCENDING order so that record appears last.
        
        Returns:
            List of SortKey objects constructed from fields with merge_order or event_time,
            or None if neither are defined.
        """
        # First priority: explicit merge_order fields
        fields_with_merge_order = self._get_fields_with_merge_order()
        if fields_with_merge_order:
            return self._create_sort_keys_from_merge_order_fields(fields_with_merge_order)
        
        # Second priority: event_time field as default merge_order key
        event_time_fields = self._get_event_time_fields()
        if event_time_fields:
            return self._create_sort_keys_from_event_time_fields(event_time_fields)
            
        return None

    def validate_and_coerce_table(
        self, 
        table: pa.Table, 
        schema_evolution_mode: Optional[str] = None,
        default_schema_consistency_type: Optional['SchemaConsistencyType'] = None
    ) -> Tuple[pa.Table, 'Schema']:
        """Validate and coerce a PyArrow table to match this schema's field types and constraints.
        
        This method now uses SchemaUpdate for safe schema evolution, ensuring all field
        protection rules and validation are applied consistently.
        
        Args:
            table: PyArrow Table to validate and coerce
            schema_evolution_mode: How to handle fields not in schema (MANUAL or AUTO)
            default_schema_consistency_type: Default consistency type for new fields in AUTO mode
            
        Returns:
            Tuple[pa.Table, Schema]: Table with data validated/coerced according to schema consistency types,
                                    and the (potentially updated) schema
            
        Raises:
            ValueError: If validation fails or coercion is not possible
            SchemaCompatibilityError: If schema evolution would break compatibility
        """
        if not self.field_ids_to_fields:
            # No fields defined in schema, return original table
            return table, self
        
        # Setup
        field_name_to_field = self._create_field_name_mapping()
        field_updates = {}  # field_name -> updated_field
        new_fields = {}     # field_name -> new_field
        new_columns = []
        new_schema_fields = []
        
        # Process each column in the table
        for column_name in table.column_names:
            column_data = table.column(column_name)
            
            processed_data, schema_field, field_update, new_field = self._process_existing_table_column(
                column_name, 
                column_data, 
                field_name_to_field, 
                schema_evolution_mode, 
                default_schema_consistency_type,
            )
            
            new_columns.append(processed_data)
            new_schema_fields.append(schema_field)
            
            if field_update:
                field_updates[column_name] = field_update
            if new_field:
                new_fields[column_name] = new_field
        
        # Add any missing fields from schema
        table_column_names = set(table.column_names)
        self._add_missing_schema_fields(table, table_column_names, new_columns, new_schema_fields)
        
        # Apply schema updates if any modifications were made
        updated_schema = self._apply_schema_updates(field_updates, new_fields)
        
        return pa.table(new_columns, schema=pa.schema(new_schema_fields)), updated_schema

    def coerce(
        self, 
        dataset: Union[pa.Table, pd.DataFrame, np.ndarray, Any],
    ) -> Union[pa.Table, pd.DataFrame, np.ndarray, Any]:
        """Coerce a dataset to match this schema using field type promotion.
        
        This method processes different dataset types and applies type promotion
        using the field's promote_type_if_needed method. It handles:
        - PyArrow Tables
        - Pandas DataFrames
        - NumPy arrays (1D and 2D)
        - Polars DataFrames (if available)
        - Daft DataFrames (if available)
        - Other types with to_arrow() method
        
        For each column, it:
        - Fields that exist in both dataset and schema: applies type promotion
        - Fields in dataset but not in schema: preserves as-is  
        - Fields in schema but not in dataset: adds with null values
        - Reorders columns to match schema order
        
        Args:
            dataset: Dataset to coerce to this schema
            
        Returns:
            Dataset of the same type, coerced to match this schema
            
        Raises:
            ValueError: If coercion fails
        """
        if not self.field_ids_to_fields:
            # No fields defined in schema, return original dataset
            return dataset
        
        # Convert dataset to PyArrow table for processing
        pa_table = to_pyarrow(dataset)

        
        # Process columns using field coercion
        coerced_columns, coerced_fields = self._coerce_table_columns(pa_table)
        
        # Reorder columns to match schema order
        reordered_columns, reordered_fields = self._reorder_columns_to_schema(
            coerced_columns, coerced_fields, pa_table
        )
        
        # Create new table with processed columns
        coerced_table = pa.table(reordered_columns, schema=pa.schema(reordered_fields))
        
        # Convert back to original dataset type
        return from_pyarrow(coerced_table, get_dataset_type(dataset))


    @property
    def fields(self) -> List[Field]:
        field_ids_to_fields = self.field_ids_to_fields
        return list(field_ids_to_fields.values())

    @property
    def merge_keys(self) -> Optional[List[FieldId]]:
        return self.get("mergeKeys")

    @property
    def field_ids_to_fields(self) -> Dict[FieldId, Field]:
        return self.get("fieldIdsToFields")

    @property
    def arrow(self) -> pa.Schema:
        return self["arrow"]

    @property
    def max_field_id(self) -> FieldId:
        return self["maxFieldId"]

    @property
    def id(self) -> SchemaId:
        return Schema._schema_id(self.arrow)

    @property
    def subschema(self, name: SchemaName) -> Optional[Schema]:
        subschemas = self.subschemas
        return subschemas.get(name) if subschemas else None

    @property
    def subschemas(self) -> Dict[SchemaName, Schema]:
        # return cached subschemas first if they exist
        subschemas = self.get("subschemas")
        if not subschemas:
            # retrieve any defined subschemas
            subschemas_to_field_ids = self.subschemas_to_field_ids
            # rebuild and return the subschema cache
            if subschemas_to_field_ids:
                subschemas = {
                    schema_name: Schema.of(
                        schema=pa.schema(
                            [self.field(field_id).arrow for field_id in field_ids]
                        ),
                        schema_id=self.id,
                        native_object=self.native_object,
                    )
                    for schema_name, field_ids in subschemas_to_field_ids.items()
                }
                self["subschemas"] = subschemas
        return subschemas or {}

    @property
    def subschema_field_ids(self, name: SchemaName) -> Optional[List[FieldId]]:
        return self.subschemas_to_field_ids.get(name)

    @property
    def subschemas_to_field_ids(self) -> Dict[SchemaName, List[FieldId]]:
        return Schema._subschemas(self.arrow)

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")

    @staticmethod
    def _schema_id(schema: pa.Schema) -> SchemaId:
        schema_id = None
        if schema.metadata:
            bytes_val = schema.metadata.get(SCHEMA_ID_KEY_NAME)
            schema_id = int(bytes_val.decode()) if bytes_val else None
        return schema_id

    @staticmethod
    def _subschemas(
        schema: pa.Schema,
    ) -> Dict[SchemaName, List[FieldId]]:
        subschemas = None
        if schema.metadata:
            bytes_val = schema.metadata.get(SUBSCHEMAS_KEY_NAME)
            subschemas = msgpack.loads(bytes_val) if bytes_val else None
        return subschemas

    @staticmethod
    def _field_name_to_field_id(
        schema: pa.Schema,
        name: Union[FieldName, NestedFieldName],
    ) -> FieldId:
        if isinstance(name, str):
            return Field.of(schema.field(name)).id
        if isinstance(name, List):
            if not len(name):
                raise ValueError(f"Nested field name `{name}` is empty.")
            field = schema
            for part in name:
                field = field[part]
            return Field.of(field).id
        raise ValueError(f"Unknown field name type: {type(name)}")

    @staticmethod
    def _visit_fields(
        current: Union[pa.Schema, pa.Field],
        visit: Callable,
        path: NestedFieldName = [],
        *args,
        **kwargs,
    ) -> None:
        """
        Recursively visit all fields in a PyArrow schema, including nested
        fields.

        Args:
            current (pa.Schema or pa.Field): The schema or field to visit.
            visit (callable): A function that visits the current field.
            path (NestedFieldName): The current path to the field.
            *args: Additional args to pass to the visit function.
            **kwargs: Additional keyword args to pass to the visit function.
        Returns:
            None
        """
        if isinstance(current, pa.Schema):
            for field in current:
                Schema._visit_fields(
                    field,
                    visit,
                    path,
                    *args,
                    **kwargs,
                )
        elif isinstance(current, pa.Field):
            path.append(current.name)
            visit(current, path, *args, **kwargs)
            if pa.types.is_nested(current.type):
                if isinstance(current, pa.StructType):
                    for field in current:
                        Schema._visit_fields(
                            field,
                            visit,
                            path,
                            *args,
                            **kwargs,
                        )
                elif isinstance(current, pa.ListType):
                    Schema._visit_fields(
                        current.value_field,
                        visit,
                        path,
                        *args,
                        **kwargs,
                    )
                elif isinstance(current, pa.MapType):
                    Schema._visit_fields(
                        current.key_field,
                        visit,
                        path,
                        *args,
                        **kwargs,
                    )
                    Schema._visit_fields(
                        current.item_field,
                        visit,
                        path,
                        *args,
                        **kwargs,
                    )
            path.pop()
        else:
            raise ValueError(f"Unexpected Schema Field Type: {type(current)}")

    @staticmethod
    def _find_max_field_id(
        field: pa.Field,
        path: NestedFieldName,
        visitor_dict: Dict[str, Any],
    ) -> None:
        max_field_id = max(
            visitor_dict.get("maxFieldId", 0),
            Field.of(field).id or 0,
        )
        visitor_dict["maxFieldId"] = max_field_id

    @staticmethod
    def _populate_fields(
        field: pa.Field,
        path: NestedFieldName,
        visitor_dict: Dict[str, Any],
    ) -> None:
        field_ids_to_fields = visitor_dict["fieldIdsToFields"]
        max_field_id = (
            visitor_dict["maxFieldId"] + len(field_ids_to_fields)
        ) % MAX_FIELD_ID_EXCLUSIVE
        dc_field = Field.of(field)
        if dc_field is not None and dc_field.id is not None:
            field_id = dc_field.id
        else:
            field_id = max_field_id

        if (dupe := field_ids_to_fields.get(field_id)) is not None:
            raise ValueError(
                f"Duplicate field id {field_id} for field: {field} "
                f"Already assigned to field: {dupe}"
            )
        field = Field.of(
            field=field,
            field_id=field_id,
            path=path,
        )
        field_ids_to_fields[field_id] = field

    @staticmethod
    def _get_lower_case_field_names(
        schema: SingleSchema,
    ) -> List[str]:
        if isinstance(schema, pa.Schema):
            return [name.lower() for name in schema.names]
        elif isinstance(schema, List):  # List[Field]
            names = [f.arrow.name.lower() for f in schema if isinstance(f, Field)]
            if len(names) == len(schema):
                return names  # all items in list are valid Field objects
        raise ValueError(f"Unsupported schema argument: {schema}")

    @staticmethod
    def _validate_schema_name(name: str) -> None:
        if not name:
            raise ValueError(f"Schema name cannot be empty.")
        if len(name) > BYTES_PER_KIBIBYTE:
            raise ValueError(
                f"Invalid schema name `{name}`. Schema names "
                f"cannot be greater than {BYTES_PER_KIBIBYTE} "
                f"characters."
            )

    @staticmethod
    def _validate_field_names(
        schema: Union[SingleSchema, MultiSchema],
    ) -> None:
        all_names = []
        if isinstance(schema, dict):  # MultiSchema
            for schema_name, val in schema.items():
                Schema._validate_schema_name(schema_name)
                all_names.extend(Schema._get_lower_case_field_names(val))
        else:  # SingleSchema
            all_names.extend(Schema._get_lower_case_field_names(schema))
        if not all_names:
            raise ValueError(f"Schema must contain at least one field.")
        name_set = set()
        dupes = []
        for name in all_names:
            dupes.append(name) if name in name_set else name_set.add(name)
        if dupes:
            raise ValueError(
                f"Expected all schema fields to have unique names "
                f"(case-insensitive), but found the following duplicates: "
                f"{dupes}"
            )

    @staticmethod
    def _to_pyarrow_schema(schema: SingleSchema) -> pa.Schema:
        if isinstance(schema, pa.Schema):
            return schema
        elif isinstance(schema, List):  # List[Field]
            return pa.schema(fields=[field.arrow for field in schema])
        else:
            raise ValueError(f"Unsupported schema base type: {schema}")

    @staticmethod
    def _to_unified_pyarrow_schema(
        schema: Union[SingleSchema, MultiSchema],
    ) -> Tuple[pa.Schema, Dict[SchemaName, List[FieldName]]]:
        # first, ensure all field names are valid and contain no duplicates
        Schema._validate_field_names(schema)
        # now union all schemas into a single schema
        subschema_to_field_names = {}
        if isinstance(schema, dict):  # MultiSchema
            all_schemas = []
            for schema_name, schema_val in schema.items():
                pyarow_schema = Schema._to_pyarrow_schema(schema_val)
                all_schemas.append(pyarow_schema)
                subschema_to_field_names[schema_name] = [
                    field.name for field in pyarow_schema
                ]
            return pa.unify_schemas(all_schemas), subschema_to_field_names
        return Schema._to_pyarrow_schema(schema), {}  # SingleSchema

    def _get_fields_with_merge_order(self) -> List['Field']:
        """Get all fields that have merge_order defined.
        
        Returns:
            List of fields with merge_order defined, or empty list if none
        """
        return [
            field for field in self.fields 
            if field.merge_order is not None
        ]
    
    def _create_sort_keys_from_merge_order_fields(self, fields_with_merge_order: List["Field"]) -> List["SortKey"]:
        """Create sort keys from fields with explicit merge_order.
        
        Args:
            fields_with_merge_order: List of fields with merge_order defined
            
        Returns:
            List of SortKey objects with inverted sort order for deduplication
        """
        from deltacat.storage.model.sort_key import SortKey
        
        sort_keys = []
        for field in fields_with_merge_order:
            merge_order = field.merge_order
            desired_sort_order = merge_order[0]
            
            # Invert the sort order because deduplication keeps the "last" record
            # ASCENDING merge_order (keep smallest) → DESCENDING sort (smallest appears last)
            # DESCENDING merge_order (keep largest) → ASCENDING sort (largest appears last)
            if desired_sort_order == SortOrder.ASCENDING:
                actual_sort_order = SortOrder.DESCENDING
            else:
                actual_sort_order = SortOrder.ASCENDING
                
            sort_key = SortKey.of(
                key=[field.arrow.name],
                sort_order=actual_sort_order,
                null_order=merge_order[1],  # NullOrder (AT_START/AT_END)
            )
            sort_keys.append(sort_key)
        return sort_keys
    
    def _get_event_time_fields(self) -> List['Field']:
        """Get all fields marked as event_time.
        
        Returns:
            List of event_time fields, or empty list if none
        """
        return [
            field for field in self.fields 
            if field.is_event_time
        ]
    
    def _create_sort_keys_from_event_time_fields(self, event_time_fields: List['Field']) -> List:
        """Create sort keys from event_time fields with default DESCENDING merge_order.
        
        Args:
            event_time_fields: List of event_time fields
            
        Returns:
            List of SortKey objects with ASCENDING sort order (inverted from DESCENDING merge_order)
        """ 
        from deltacat.storage.model.sort_key import SortKey
        
        sort_keys = []
        for field in event_time_fields:
            sort_key = SortKey.of(
                key=[field.arrow.name],
                sort_order=SortOrder.ASCENDING,  # Inverted: DESCENDING merge_order → ASCENDING sort
                null_order=NullOrder.AT_END,
            )
            sort_keys.append(sort_key)
        return sort_keys

    def _create_field_name_mapping(self) -> Dict[str, 'Field']:
        """Create a mapping from field names to Field objects."""
        field_name_to_field = {}
        for field in self.field_ids_to_fields.values():
            field_name_to_field[field.arrow.name] = field
        return field_name_to_field
    
    def _process_existing_table_column(
        self, 
        column_name: str,
        column_data: pa.Array,
        field_name_to_field: Dict[str, 'Field'],
        schema_evolution_mode: Optional[str],
        default_schema_consistency_type: Optional['SchemaConsistencyType']
    ) -> Tuple[pa.Array, pa.Field, Optional['Field'], Optional['Field']]:
        """Process a column that exists in the table.
        
        Returns:
            Tuple of (processed_column_data, schema_field, field_update, new_field)
        """
        if column_name in field_name_to_field:
            # Field exists in schema - validate/coerce according to consistency type
            field = field_name_to_field[column_name]
            
            if field.consistency_type == SchemaConsistencyType.VALIDATE:
                field.validate(column_data)
                return column_data, field.arrow, None, None
            elif field.consistency_type == SchemaConsistencyType.COERCE:
                coerced_data = field.coerce(column_data)
                return coerced_data, field.arrow, None, None
            else:
                # NONE or no consistency type - use type promotion
                return self._handle_type_promotion(column_name, column_data, field)
        else:
            # Field not in schema - handle based on evolution mode
            return self._handle_new_field(column_name, column_data, schema_evolution_mode, default_schema_consistency_type)
    
    def _handle_type_promotion(
        self, 
        column_name: str,
        column_data: pa.Array,
        field: 'Field'
    ) -> Tuple[pa.Array, pa.Field, Optional['Field'], Optional['Field']]:
        """Handle type promotion for a field with NONE consistency type."""
        promoted_data, type_was_promoted = field.promote_type_if_needed(column_data)
        
        if type_was_promoted:
            # Cast default values to match the promoted type
            promoted_past_default = field._cast_default_to_promoted_type(
                field.past_default, promoted_data.type
            ) if field.past_default is not None else None
            
            promoted_future_default = field._cast_default_to_promoted_type(
                field.future_default, promoted_data.type
            ) if field.future_default is not None else None
            
            # Create updated field with same properties but new type and cast defaults
            promoted_field = pa.field(
                field.arrow.name,
                promoted_data.type,
                nullable=field.arrow.nullable,
                metadata=field.arrow.metadata
            )
            
            updated_field = Field.of(
                promoted_field,
                field_id=field.id,
                is_merge_key=field.is_merge_key,
                merge_order=field.merge_order,
                is_event_time=field.is_event_time,
                doc=field.doc,
                past_default=promoted_past_default,
                future_default=promoted_future_default,
                consistency_type=field.consistency_type,
                path=field.path,
                native_object=field.native_object
            )
            
            return promoted_data, promoted_field, updated_field, None
        else:
            return promoted_data, field.arrow, None, None
    
    def _handle_new_field(
        self,
        column_name: str,
        column_data: pa.Array,
        schema_evolution_mode: Optional[str],
        default_schema_consistency_type: Optional['SchemaConsistencyType']
    ) -> Tuple[pa.Array, pa.Field, Optional['Field'], Optional['Field']]:
        """Handle a field that's not in the schema."""
        if schema_evolution_mode == "AUTO":
            # Create new field with default consistency type
            next_field_id = self.max_field_id + 1
            new_field = Field.of(
                pa.field(column_name, column_data.type, nullable=True),
                field_id=next_field_id,
                consistency_type=default_schema_consistency_type or SchemaConsistencyType.NONE
            )
            return column_data, new_field.arrow, None, new_field
        else:
            # MANUAL mode or not specified - raise error
            raise ValueError(f"Field '{column_name}' is not present in the schema")
    
    def _add_missing_schema_fields(
        self,
        table: pa.Table,
        table_column_names: set,
        new_columns: List[pa.Array],
        new_schema_fields: List[pa.Field]
    ) -> None:
        """Add columns for fields that exist in schema but not in table."""
        for field in self.field_ids_to_fields.values():
            if field.arrow.name not in table_column_names:
                consistency_type = field.consistency_type
                
                if consistency_type == SchemaConsistencyType.VALIDATE:
                    raise ValueError(f"Field '{field.arrow.name}' is required but not present in the data")
                elif consistency_type == SchemaConsistencyType.COERCE:
                    # Use future_default if available, otherwise check if nullable
                    if field.future_default is not None:
                        # Create column with future_default value
                        default_array = pa.array([field.future_default] * get_table_length(table), type=field.arrow.type)
                        new_columns.append(default_array)
                    elif field.arrow.nullable:
                        # Backfill with nulls if field is nullable
                        null_column = pa.nulls(get_table_length(table), type=field.arrow.type)
                        new_columns.append(null_column)
                    else:
                        # Field is not nullable and no future_default - error
                        raise ValueError(f"Field '{field.arrow.name}' is required (not nullable) but not present in the data and no future_default is set")
                    new_schema_fields.append(field.arrow)
                else:
                    # NONE or no consistency type - backfill with future_default or nulls
                    if field.future_default is not None:
                        default_array = pa.array([field.future_default] * get_table_length(table), type=field.arrow.type)
                        new_columns.append(default_array)
                    else:
                        null_column = pa.nulls(get_table_length(table), type=field.arrow.type)
                        new_columns.append(null_column)
                    new_schema_fields.append(field.arrow)
    
    def _apply_schema_updates(
        self,
        field_updates: Dict[str, 'Field'],
        new_fields: Dict[str, 'Field']
    ) -> 'Schema':
        """Apply collected schema updates and return the updated schema."""
        if not field_updates and not new_fields:
            return self
        
        # Initialize schema update with allow_incompatible_changes=True for type promotion
        schema_update = self.update(allow_incompatible_changes=True)
        
        # Apply field updates
        for field_name, updated_field in field_updates.items():
            schema_update = schema_update.update_field(field_name, updated_field)
        
        # Apply new fields
        for field_name, new_field in new_fields.items():
            schema_update = schema_update.add_field(field_name, new_field)
        
        # Apply all updates
        return schema_update.apply()

    def _process_existing_columns_for_coercion(
        self,
        pa_table: pa.Table,
        field_name_to_field: Dict[str, 'Field']
    ) -> Tuple[List[pa.Array], List[pa.Field]]:
        """Process columns that exist in the table for coercion.
        
        Args:
            pa_table: PyArrow table to process
            field_name_to_field: Mapping from field names to Field objects
            
        Returns:
            Tuple of (processed columns, corresponding fields)
        """
        new_columns = []
        new_schema_fields = []
        
        for column_name in pa_table.column_names:
            column_data = pa_table.column(column_name)
            
            if column_name in field_name_to_field:
                # Field exists in target schema - use promote_type_if_needed for coercion
                field = field_name_to_field[column_name]
                promoted_data, _ = field.promote_type_if_needed(column_data)
                new_columns.append(promoted_data)
                new_schema_fields.append(field.arrow)
            else:
                # Field not in target schema - preserve as-is
                new_columns.append(column_data)
                new_schema_fields.append(pa.field(column_name, column_data.type))
        
        return new_columns, new_schema_fields
    
    def _add_missing_fields_for_coercion(
        self,
        pa_table: pa.Table,
        field_name_to_field: Dict[str, 'Field'],
        existing_columns: List[pa.Array],
        existing_fields: List[pa.Field]
    ) -> Tuple[List[pa.Array], List[pa.Field]]:
        """Add columns for fields that exist in schema but not in table.
        
        Args:
            pa_table: Original PyArrow table
            field_name_to_field: Mapping from field names to Field objects
            existing_columns: Columns already processed
            existing_fields: Fields already processed
            
        Returns:
            Tuple of (all columns including added ones, all corresponding fields)
        """
        all_columns = existing_columns.copy()
        all_fields = existing_fields.copy()
        
        # Add any missing fields from target schema with null values or past_default values
        target_field_names = {field.arrow.name for field in self.field_ids_to_fields.values()}
        table_field_names = set(pa_table.column_names)
        
        for field_name in target_field_names - table_field_names:
            field = field_name_to_field[field_name]
            
            # Check if field has past_default value and use it instead of nulls
            if field.past_default is not None:
                # Create array filled with past_default value
                default_column = pa.array([field.past_default] * get_table_length(pa_table), type=field.arrow.type)
                all_columns.append(default_column)
            else:
                # Use null values as before
                null_column = pa.nulls(get_table_length(pa_table), type=field.arrow.type)
                all_columns.append(null_column)
            
            all_fields.append(field.arrow)
        
        return all_columns, all_fields

    def _coerce_table_columns(self, pa_table: pa.Table) -> Tuple[List[pa.Array], List[pa.Field]]:
        """Process table columns using field coercion and add missing fields.
        
        Args:
            pa_table: PyArrow table to process
            
        Returns:
            Tuple of (list of coerced columns, list of corresponding fields)
        """
        # Create mapping from field names to Field objects
        field_name_to_field = self._create_field_name_mapping()
        
        # Process existing columns in the table
        processed_columns, processed_fields = self._process_existing_columns_for_coercion(
            pa_table, field_name_to_field
        )
        
        # Add any missing fields from target schema
        all_columns, all_fields = self._add_missing_fields_for_coercion(
            pa_table, field_name_to_field, processed_columns, processed_fields
        )
        
        return all_columns, all_fields

    def _reorder_columns_to_schema(
        self, 
        columns: List[pa.Array], 
        fields: List[pa.Field], 
        original_table: pa.Table
    ) -> Tuple[List[pa.Array], List[pa.Field]]:
        """Reorder columns to match schema order, preserving extra fields.
        
        Args:
            columns: List of processed columns
            fields: List of corresponding field schemas
            original_table: Original table for field name ordering
            
        Returns:
            Tuple of (reordered columns, reordered fields)
        """
        # Reorder columns to match schema order
        reordered_columns = []
        reordered_fields = []
        schema_field_names = [field.arrow.name for field in self.field_ids_to_fields.values()]
        
        # Add schema fields in schema order
        for field_name in schema_field_names:
            for i, field in enumerate(fields):
                if field.name == field_name:
                    reordered_columns.append(columns[i])
                    reordered_fields.append(field)
                    break
        
        # Add any extra fields that aren't in schema (preserve original order)
        target_field_names = set(schema_field_names)
        table_field_names = set(original_table.column_names)
        extra_field_names = table_field_names - target_field_names
        
        for field_name in original_table.column_names:
            if field_name in extra_field_names:
                for i, field in enumerate(fields):
                    if field.name == field_name:
                        reordered_columns.append(columns[i])
                        reordered_fields.append(field)
                        break
        
        return reordered_columns, reordered_fields

    @staticmethod
    def _del_subschema(
        name: SchemaName,
        subschemas: Dict[SchemaName, Schema],
    ) -> Dict[SchemaName, Schema]:
        deleted_subschema = subschemas.pop(name, None)
        if deleted_subschema is None:
            raise ValueError(f"Subschema `{name}` does not exist.")
        return subschemas

    @staticmethod
    def _add_subschema(
        name: SchemaName,
        schema: SingleSchema,
        subschemas: Dict[SchemaName, Schema],
    ) -> Dict[SchemaName, Schema]:
        Schema._validate_schema_name(name)
        if name == BASE_SCHEMA_NAME:
            raise ValueError(
                f"Cannot add subschema with reserved name: {BASE_SCHEMA_NAME}"
            )
        if name in subschemas:
            raise ValueError(f"Subschema `{name}` already exists.")
        for key, val in subschemas.items():
            subschemas[key] = val.arrow
        subschemas[name] = schema
        return subschemas


class SchemaList(List[Schema]):
    @staticmethod
    def of(items: List[Schema]) -> SchemaList:
        typed_items = SchemaList()
        for item in items:
            if item is not None and not isinstance(item, Schema):
                item = Schema(item)
            typed_items.append(item)
        return typed_items

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, Schema):
            self[item] = val = Schema(val)
        return val


class SchemaUpdate(dict):
    """
    Provides safe schema evolution capabilities for DeltaCAT schemas.
    
    SchemaUpdate allows users to:
    1. Add new fields to a schema
    2. Remove existing fields from a schema  
    3. Update existing fields with compatible changes
    4. Validate schema compatibility to prevent breaking existing dataset consumers
    
    The class enforces backward compatibility by default to ensure that table
    consumer jobs writtten using PyArrow, Pandas, Polars, Ray Data, Daft, and other 
    dataset types continue to work after schema changes.
    
    Example:
        Using Schema.update():
        >>> schema = Schema.of([Field.of(pa.field("id", pa.int64()))])
        >>> new_field = Field.of(pa.field("name", pa.string()))
        >>> updated_schema = (schema.update()
        ...                         .add_field("name", new_field)
        ...                         .apply())

        Using SchemaUpdate.of():
        >>> schema = Schema.of([Field.of(pa.field("id", pa.int64()))])
        >>> update = SchemaUpdate.of(schema)
        >>> new_field = Field.of(pa.field("name", pa.string()))
        >>> updated_schema = update.add_field("name", new_field).apply()
    """
    
    @staticmethod
    def of(
        base_schema: Schema, 
        allow_incompatible_changes: bool = False
    ) -> 'SchemaUpdate':
        """
        Create a SchemaUpdate for the given base schema.
        
        Args:
            base_schema: The original schema to update
            allow_incompatible_changes: If True, allows changes that may break
                backward compatibility. If False (default), raises SchemaCompatibilityError
                for incompatible changes.
                
        Returns:
            A new SchemaUpdate instance
        """
        return SchemaUpdate._build(
            base_schema=base_schema,
            allow_incompatible_changes=allow_incompatible_changes,
            operations=SchemaUpdateOperations.of([])
        )
    
    @staticmethod
    def _build(
        base_schema: Schema,
        allow_incompatible_changes: bool,
        operations: SchemaUpdateOperations
    ) -> 'SchemaUpdate':
        """Internal builder method."""
        schema_update = SchemaUpdate()
        schema_update["baseSchema"] = base_schema
        schema_update["allowIncompatibleChanges"] = allow_incompatible_changes
        schema_update["operations"] = operations
        return schema_update
    
    @property
    def base_schema(self) -> Schema:
        """Get the base schema being updated."""
        return self["baseSchema"]
    
    @base_schema.setter  
    def base_schema(self, value: Schema) -> None:
        """Set the base schema being updated."""
        self["baseSchema"] = value
    
    @property
    def allow_incompatible_changes(self) -> bool:
        """Get whether incompatible changes are allowed."""
        return self["allowIncompatibleChanges"]
    
    @allow_incompatible_changes.setter
    def allow_incompatible_changes(self, value: bool) -> None:
        """Set whether incompatible changes are allowed."""
        self["allowIncompatibleChanges"] = value
    
    @property
    def operations(self) -> SchemaUpdateOperations:
        """Get the list of pending operations."""
        return self["operations"]
    
    @operations.setter
    def operations(self, value: SchemaUpdateOperations) -> None:
        """Set the list of pending operations."""
        self["operations"] = value
        
    def add_field(self, field_locator: FieldLocator, new_field: Field) -> 'SchemaUpdate':
        """
        Add a new field to the schema.
        
        Args:
            field_locator: Location identifier for the new field (name, nested path, or ID)
            new_field: The Field object to add
            
        Returns:
            Self for method chaining
            
        Raises:
            SchemaCompatibilityError: If field already exists or addition would break compatibility
        """
        self.operations.append(SchemaUpdateOperation.add_field(field_locator, new_field))
        return self
        
    def remove_field(self, field_locator: FieldLocator) -> 'SchemaUpdate':
        """
        Remove an existing field from the schema.
        
        Args:
            field_locator: Location identifier for the field to remove
            
        Returns:
            Self for method chaining
            
        Raises:
            SchemaCompatibilityError: If field doesn't exist or removal would break compatibility
        """
        self.operations.append(SchemaUpdateOperation.remove_field(field_locator))
        return self
        
    def update_field(self, field_locator: FieldLocator, updated_field: Field) -> 'SchemaUpdate':
        """
        Update an existing field with compatible changes.
        
        Args:
            field_locator: Location identifier for the field to update
            updated_field: The new Field object to replace the existing field
            
        Returns:
            Self for method chaining
            
        Raises:
            SchemaCompatibilityError: If field doesn't exist or update would break compatibility
        """
        self.operations.append(SchemaUpdateOperation.update_field(field_locator, updated_field))
        return self
        
    def apply(self) -> Schema:
        """
        Apply all pending operations and return the updated schema.
        
        Returns:
            New Schema object with all updates applied
            
        Raises:
            SchemaCompatibilityError: If any operation would break backward compatibility
                and allow_incompatible_changes is False
        """
        # Start with a copy of the base schema
        updated_fields = list(self.base_schema.fields)
        field_name_to_index = {field.path[0] if field.path else f"field_{field.id}": i 
                              for i, field in enumerate(updated_fields)}
        
        # Apply operations in order
        for operation in self.operations:
            if operation.operation == "add":
                self._apply_add_field(updated_fields, field_name_to_index, operation.field_locator, operation.field)
            elif operation.operation == "remove":
                self._apply_remove_field(updated_fields, field_name_to_index, operation.field_locator)
            elif operation.operation == "update":
                self._apply_update_field(updated_fields, field_name_to_index, operation.field_locator, operation.field)
        
        # Create new schema from updated fields with incremented schema ID
        return Schema.of(updated_fields, schema_id=self.base_schema.id + 1)
    
    def _apply_add_field(
        self, 
        fields: List[Field], 
        field_name_to_index: Dict[str, int],
        field_locator: FieldLocator, 
        new_field: Field
    ) -> None:
        """Apply add field operation with compatibility validation."""
        field_name = self._get_field_name(field_locator)
        
        # Check if field already exists
        if field_name in field_name_to_index:
            raise SchemaCompatibilityError(
                f"Field '{field_name}' already exists in schema",
                field_locator
            )
            
        # Validate compatibility for new field
        if not self.allow_incompatible_changes:
            self._validate_add_field_compatibility(new_field, field_locator)
            
        # Create a copy of the field with the correct path
        field_with_path = Field.of(
            new_field.arrow,
            field_id=new_field.id,
            is_merge_key=new_field.is_merge_key,
            merge_order=new_field.merge_order,
            is_event_time=new_field.is_event_time,
            doc=new_field.doc,
            past_default=new_field.past_default,
            future_default=new_field.future_default,
            consistency_type=new_field.consistency_type,
            path=[field_name],
            native_object=new_field.native_object
        )
        
        # Add the field
        fields.append(field_with_path)
        field_name_to_index[field_name] = len(fields) - 1
    
    def _apply_remove_field(
        self,
        fields: List[Field],
        field_name_to_index: Dict[str, int],
        field_locator: FieldLocator
    ) -> None:
        """Apply remove field operation with compatibility validation."""
        field_name = self._get_field_name(field_locator)
        
        # Check if field exists
        if field_name not in field_name_to_index:
            raise SchemaCompatibilityError(
                f"Field '{field_name}' does not exist in schema",
                field_locator
            )
            
        # Validate compatibility for field removal
        if not self.allow_incompatible_changes:
            field_index = field_name_to_index[field_name]
            self._validate_remove_field_compatibility(fields[field_index], field_locator)
            
        # Remove the field
        field_index = field_name_to_index[field_name]
        fields.pop(field_index)
        
        # Update indices
        del field_name_to_index[field_name]
        for name, index in field_name_to_index.items():
            if index > field_index:
                field_name_to_index[name] = index - 1
    
    def _apply_update_field(
        self,
        fields: List[Field],
        field_name_to_index: Dict[str, int],
        field_locator: FieldLocator,
        updated_field: Field
    ) -> None:
        """Apply update field operation with compatibility validation."""
        field_name = self._get_field_name(field_locator)
        
        # Check if field exists
        if field_name not in field_name_to_index:
            raise SchemaCompatibilityError(
                f"Field '{field_name}' does not exist in schema",
                field_locator
            )
            
        field_index = field_name_to_index[field_name]
        old_field = fields[field_index]
        
        # Validate compatibility for field update
        if not self.allow_incompatible_changes:
            self._validate_update_field_compatibility(old_field, updated_field, field_locator)
            
        # Create a copy of the updated field with the correct path  
        field_with_path = Field.of(
            updated_field.arrow,
            field_id=updated_field.id,
            is_merge_key=updated_field.is_merge_key,
            merge_order=updated_field.merge_order,
            is_event_time=updated_field.is_event_time,
            doc=updated_field.doc,
            past_default=updated_field.past_default,
            future_default=updated_field.future_default,
            consistency_type=updated_field.consistency_type,
            path=[field_name],
            native_object=updated_field.native_object
        )
        
        # Update the field
        fields[field_index] = field_with_path
    
    def _get_field_name(self, field_locator: FieldLocator) -> str:
        """Extract field name from various field locator types."""
        if isinstance(field_locator, str):
            return field_locator
        elif isinstance(field_locator, list):
            return field_locator[0] if field_locator else ""
        elif isinstance(field_locator, int):
            # For field ID, try to find the corresponding field
            try:
                field = self.base_schema.field(field_locator)
                return field.path[0] if field.path else f"field_{field_locator}"
            except:
                return f"field_{field_locator}"
        else:
            raise ValueError(f"Invalid field locator type: {type(field_locator)}")
    
    def _validate_add_field_compatibility(self, new_field: Field, field_locator: FieldLocator) -> None:
        """Validate that adding a new field won't break compatibility."""
        field_name = self._get_field_name(field_locator)
        arrow_field = new_field.arrow
        
        # Check for duplicate field IDs across all existing fields
        if new_field.id is not None:
            existing_field_ids = {f.id for f in self.base_schema.fields if f.id is not None}
            if new_field.id in existing_field_ids:
                raise SchemaCompatibilityError(
                    f"Cannot add field '{field_name}' with duplicate field ID {new_field.id}. "
                    f"Field IDs must be unique across all fields in the schema.",
                    field_locator
                )
        
        # Check if field is nullable or has default values
        is_nullable = arrow_field.nullable
        has_past_default = new_field.past_default is not None
        has_future_default = new_field.future_default is not None
        
        if not (is_nullable or has_past_default or has_future_default):
            raise SchemaCompatibilityError(
                f"Adding non-nullable field '{field_name}' without "
                f"default values would break compatibility with existing data",
                field_locator
            )
    
    def _validate_remove_field_compatibility(self, field: Field, field_locator: FieldLocator) -> None:
        """Validate that removing a field won't break compatibility."""
        field_name = self._get_field_name(field_locator)
        
        # Check for protected field types that should never be removed
        if field.is_merge_key:
            raise SchemaCompatibilityError(
                f"Cannot remove merge key field '{field_name}'. "
                f"Merge keys are critical for data integrity and cannot be removed.",
                field_locator
            )
            
        if field.merge_order is not None:
            raise SchemaCompatibilityError(
                f"Cannot remove merge order field '{field_name}'. "
                f"Fields with merge_order are critical for data ordering and cannot be removed.",
                field_locator
            )
            
        if field.is_event_time:
            raise SchemaCompatibilityError(
                f"Cannot remove event time field '{field_name}'. "
                f"Event time fields are critical for temporal operations and cannot be removed.",
                field_locator
            )
        
        # Removing fields generally breaks compatibility for consumers expecting them
        raise SchemaCompatibilityError(
            f"Removing field '{field_name}' would break compatibility with existing consumers. "
            f"Set allow_incompatible_changes=True to force removal.",
            field_locator
        )
    
    def _validate_update_field_compatibility(
        self, 
        old_field: Field, 
        new_field: Field, 
        field_locator: FieldLocator
    ) -> None:
        """Validate that updating a field won't break compatibility."""
        old_arrow = old_field.arrow
        new_arrow = new_field.arrow
        field_name = self._get_field_name(field_locator)
        
        # Protect critical field attributes that should never be changed
        if old_field.is_merge_key != new_field.is_merge_key:
            raise SchemaCompatibilityError(
                f"Cannot change merge key status for field '{field_name}'. "
                f"Merge key designation is critical for data integrity and cannot be modified.",
                field_locator
            )
            
        if old_field.merge_order != new_field.merge_order:
            raise SchemaCompatibilityError(
                f"Cannot change merge order for field '{field_name}'. "
                f"Merge order is critical for data consistency and cannot be modified.",
                field_locator
            )
            
        if old_field.is_event_time != new_field.is_event_time:
            raise SchemaCompatibilityError(
                f"Cannot change event time status for field '{field_name}'. "
                f"Event time designation is critical for temporal operations and cannot be modified.",
                field_locator
            )
        
        # Validate schema consistency type evolution rules
        self._validate_consistency_type_evolution(old_field, new_field, field_locator)
        
        # Protect past_default immutability
        if old_field.past_default != new_field.past_default:
            raise SchemaCompatibilityError(
                f"Cannot change past_default for field '{field_name}'. "
                f"The past_default value is immutable once set to maintain data consistency.",
                field_locator
            )
        
        # Check for duplicate field IDs (if field ID is being changed)
        if old_field.id != new_field.id and new_field.id is not None:
            existing_field_ids = {f.id for f in self.base_schema.fields if f.id is not None and f != old_field}
            if new_field.id in existing_field_ids:
                raise SchemaCompatibilityError(
                    f"Cannot update field '{field_name}' to use duplicate field ID {new_field.id}. "
                    f"Field IDs must be unique across all fields in the schema.",
                    field_locator
                )
        
        # Check data type compatibility
        if not self._is_type_compatible(old_arrow.type, new_arrow.type):
            raise SchemaCompatibilityError(
                f"Cannot change field '{field_name}' from {old_arrow.type} to {new_arrow.type}. "
                f"This change would break compatibility with PyArrow, Pandas, Polars, Ray Data, and Daft.",
                field_locator
            )
        
        # Check nullability - making a field non-nullable is incompatible
        if old_arrow.nullable and not new_arrow.nullable:
            # Only allow if we have past/future defaults to fill null values
            has_past_default = new_field.past_default is not None
            has_future_default = new_field.future_default is not None
            
            if not (has_past_default and has_future_default):
                raise SchemaCompatibilityError(
                    f"Cannot make nullable field '{field_name}' non-nullable without "
                    f"providing both past_default and future_default values",
                    field_locator
                )
    
    def _validate_consistency_type_evolution(
        self, 
        old_field: Field, 
        new_field: Field, 
        field_locator: FieldLocator
    ) -> None:
        """
        Validate schema consistency type evolution rules.
        
        Allowed transitions:
        - COERCE -> VALIDATE  
        - VALIDATE -> COERCE
        - COERCE -> NONE
        - VALIDATE -> NONE
        
        Forbidden transitions:
        - NONE -> COERCE
        - NONE -> VALIDATE
        """ 
        old_type = old_field.consistency_type
        new_type = new_field.consistency_type
        field_name = self._get_field_name(field_locator)
        
        # If types are the same, no validation needed
        if old_type == new_type:
            return
            
        # Handle None values (treat as no consistency type set)
        if old_type is None and new_type is None:
            return
            
        # Allow transitions from any type to NONE (relaxing constraints)
        if new_type == SchemaConsistencyType.NONE or new_type is None:
            return
            
        # Allow transitions between COERCE and VALIDATE (bidirectional)
        if (old_type in (SchemaConsistencyType.COERCE, SchemaConsistencyType.VALIDATE) and 
            new_type in (SchemaConsistencyType.COERCE, SchemaConsistencyType.VALIDATE)):
            return
            
        # Allow transitions from None to COERCE or VALIDATE (adding constraints)
        if old_type is None and new_type in (SchemaConsistencyType.COERCE, SchemaConsistencyType.VALIDATE):
            return
            
        # Forbid transitions from NONE to COERCE or VALIDATE (tightening constraints)
        if (old_type == SchemaConsistencyType.NONE and 
            new_type in (SchemaConsistencyType.COERCE, SchemaConsistencyType.VALIDATE)):
            raise SchemaCompatibilityError(
                f"Cannot change consistency type for field '{field_name}' from {old_type.value} to {new_type.value}. "
                f"Transitioning from NONE to {new_type.value} would tighten validation constraints "
                f"and potentially break existing data processing.",
                field_locator
            )
            
        # If we get here, it's an unexpected combination
        raise SchemaCompatibilityError(
            f"Invalid consistency type transition for field '{field_name}' from "
            f"{old_type.value if old_type else 'None'} to {new_type.value if new_type else 'None'}.",
            field_locator
        )
    
    def _is_type_compatible(self, old_type: pa.DataType, new_type: pa.DataType) -> bool:
        """
        Check if changing from old_type to new_type is backward compatible.
        
        Compatible changes include:
        - Same type
        - Widening numeric types (int32 -> int64, float32 -> float64)
        - Making string/binary types longer
        - Adding fields to struct types
        - Making list/map value types more permissive
        """
        # Same type is always compatible
        if old_type.equals(new_type):
            return True
            
        # Numeric type widening
        if pa.types.is_integer(old_type) and pa.types.is_integer(new_type):
            # Check bit width and signedness using string representation
            old_signed = 'int' in str(old_type) and 'uint' not in str(old_type)
            new_signed = 'int' in str(new_type) and 'uint' not in str(new_type)
            return new_type.bit_width >= old_type.bit_width and old_signed == new_signed
            
        if pa.types.is_floating(old_type) and pa.types.is_floating(new_type):
            return new_type.bit_width >= old_type.bit_width
            
        # Integer to float promotion
        if pa.types.is_integer(old_type) and pa.types.is_floating(new_type):
            return True
            
        # String/binary type compatibility
        if pa.types.is_string(old_type) and pa.types.is_string(new_type):
            return True
        if pa.types.is_binary(old_type) and pa.types.is_binary(new_type):
            return True
            
        # Struct type compatibility (new fields can be added)
        if pa.types.is_struct(old_type) and pa.types.is_struct(new_type):
            old_names = {field.name for field in old_type}
            new_names = {field.name for field in new_type}
            
            # All old fields must exist in new type
            if not old_names.issubset(new_names):
                return False
                
            # Check compatibility of common fields
            for old_field in old_type:
                new_field = new_type.field(old_field.name)
                if not self._is_type_compatible(old_field.type, new_field.type):
                    return False
                    
            return True
            
        # List type compatibility
        if pa.types.is_list(old_type) and pa.types.is_list(new_type):
            return self._is_type_compatible(old_type.value_type, new_type.value_type)
            
        # Map type compatibility  
        if pa.types.is_map(old_type) and pa.types.is_map(new_type):
            return (self._is_type_compatible(old_type.key_type, new_type.key_type) and
                    self._is_type_compatible(old_type.item_type, new_type.item_type))
        
        # Default: types are incompatible
        return False

