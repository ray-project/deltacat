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
from deltacat.storage.model.types import (
    SchemaConsistencyType,
    SortOrder,
    NullOrder,
)
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
    def _validate_merge_key(field: pa.Field):
        # Note: large_strings were explicitly allowed for compatibility with PyIceberg Iceberg Schema to PyArrow converter
        if not (
            pa.types.is_string(field.type)
            or pa.types.is_primitive(field.type)
            or pa.types.is_large_string(field.type)
        ):
            raise ValueError(
                f"Merge key {field} must be a primitive type or large string."
            )
        if pa.types.is_floating(field.type):
            raise ValueError(f"Merge key {field} cannot be floating point.")

    @staticmethod
    def _validate_merge_order(field: pa.Field):
        if not pa.types.is_primitive(field.type):
            raise ValueError(f"Merge order {field} must be a primitive type.")

    @staticmethod
    def _validate_event_time(field: pa.Field):
        if (
            not pa.types.is_integer(field.type)
            and not pa.types.is_floating(field.type)
            and not pa.types.is_date(field.type)
        ):
            raise ValueError(f"Event time {field} must be numeric or date type.")

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
        meta = {}
        if is_merge_key:
            Field._validate_merge_key(field)
            meta[FIELD_MERGE_KEY_NAME] = str(is_merge_key)
        if merge_order:
            Field._validate_merge_order(field)
            meta[FIELD_MERGE_ORDER_KEY_NAME] = msgpack.dumps(merge_order)
        if is_event_time:
            Field._validate_event_time(field)
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

    def validate(self, column_data: pa.Array) -> None:
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
    
    def coerce(self, column_data: pa.Array) -> pa.Array:
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

    def promote_type_if_needed(self, column_data: pa.Array) -> Tuple[pa.Array, bool]:
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
        
        # If types are already compatible, no promotion needed
        if current_type.equals(data_type):
            return column_data, False
            
        # Try to find a promoted type that can accommodate both current and new data
        promoted_type = self._find_promoted_type(current_type, data_type)
        
        if promoted_type is None:
            # No compatible promotion found - return original data
            return column_data, False
            
        if promoted_type.equals(current_type):
            # Promoted type is same as current - just coerce the data
            try:
                coerced_data = pa.compute.cast(column_data, current_type)
                return coerced_data, False
            except (pa.ArrowTypeError, pa.ArrowInvalid):
                # Coercion failed, return original data
                return column_data, False
        else:
            # Type needs to be promoted
            try:
                promoted_data = pa.compute.cast(column_data, promoted_type)
                return promoted_data, True
            except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
                # Direct cast failed - try multi-step conversion for binary
                if pa.types.is_binary(promoted_type):
                    try:
                        # Try converting via string intermediate step
                        string_data = pa.compute.cast(column_data, pa.string())
                        binary_data = pa.compute.cast(string_data, promoted_type)
                        return binary_data, True
                    except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
                        pass
                # Promotion failed, return original data
                return column_data, False
    
    def _find_promoted_type(self, current_type: pa.DataType, new_type: pa.DataType) -> Optional[pa.DataType]:
        """Find the most specific type that can accommodate both current and new types.
        
        Returns the promoted type or None if no promotion is possible.
        """
        # Handle nullability - promoted type should be nullable if either type is nullable
        make_nullable = current_type.value_type if hasattr(current_type, 'value_type') else None
        if hasattr(new_type, 'value_type'):
            make_nullable = True
        
        # Type promotion hierarchy
        type_hierarchy = [
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
        
        # Special cross-type promotions (ordered by specificity - most specific first)
        cross_type_promotions = {
            # Any integer can promote to float (most specific numeric promotion)
            'int_to_float': (
                [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()],
                [pa.float32(), pa.float64()]
            ),
            # Any numeric can promote to decimal
            'numeric_to_decimal': (
                [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(),
                 pa.float32(), pa.float64()],
                [pa.decimal128(28, 10)]
            ),
            # Any type except binary can promote to string
            'any_to_string': (
                'not_binary',  # Any type except binary
                [pa.string(), pa.large_string()]
            ),
            # Any type can promote to binary (most permissive, checked last)
            'any_to_binary': (
                None,  # Any type  
                [pa.binary(), pa.large_binary()]
            ),
        }
        
        # First check within-hierarchy promotions
        for hierarchy in type_hierarchy:
            current_idx = self._find_type_in_hierarchy(current_type, hierarchy)
            new_idx = self._find_type_in_hierarchy(new_type, hierarchy)
            
            if current_idx is not None and new_idx is not None:
                # Both types are in same hierarchy - promote to the higher one
                promoted_idx = max(current_idx, new_idx)
                promoted_type = hierarchy[promoted_idx]
                return self._maybe_make_nullable(promoted_type, current_type, new_type)
        
        # Check cross-type promotions
        for promotion_name, (source_types, target_types) in cross_type_promotions.items():
            if source_types is None:  # any_to_* cases
                current_compatible = True
                new_compatible = True
            elif source_types == 'not_binary':  # any type except binary
                current_compatible = not pa.types.is_binary(current_type)
                new_compatible = not pa.types.is_binary(new_type)
                # For string promotion, require both types to be non-binary
                if not (current_compatible and new_compatible):
                    continue
            else:
                current_compatible = any(self._types_compatible(current_type, src) for src in source_types)
                new_compatible = any(self._types_compatible(new_type, src) for src in source_types)
            
            if current_compatible or new_compatible:
                # At least one type can be promoted to target - use the most specific target
                for target_type in target_types:
                    if self._can_cast_to(current_type, target_type) and self._can_cast_to(new_type, target_type):
                        return self._maybe_make_nullable(target_type, current_type, new_type)
        
        # No promotion found
        return None
    
    def _find_type_in_hierarchy(self, data_type: pa.DataType, hierarchy: List[pa.DataType]) -> Optional[int]:
        """Find the index of a compatible type in the hierarchy."""
        for i, hierarchy_type in enumerate(hierarchy):
            if self._types_compatible(data_type, hierarchy_type):
                return i
        return None
    
    def _types_compatible(self, type1: pa.DataType, type2: pa.DataType) -> bool:
        """Check if two types are compatible (ignoring nullability)."""
        # Strip nullability for comparison
        base_type1 = type1.value_type if hasattr(type1, 'value_type') else type1
        base_type2 = type2.value_type if hasattr(type2, 'value_type') else type2
        return base_type1.equals(base_type2)
    
    def _can_cast_to(self, from_type: pa.DataType, to_type: pa.DataType) -> bool:
        """Check if a type can be cast to another type."""
        try:
            # Create a small test array to check if casting is possible
            if from_type == pa.null():
                return True  # null can cast to anything
            
            # Special case: any type can conceptually promote to binary via string
            if pa.types.is_binary(to_type):
                # For binary targets, check if we can cast to string first
                # (since string->binary is always possible)
                if not pa.types.is_binary(from_type) and not pa.types.is_string(from_type):
                    return self._can_cast_to(from_type, pa.string())
                # Binary and string can always cast to binary
                return True
            
            # Create a minimal test array with the source type
            if hasattr(from_type, 'value_type'):  # nullable type
                test_array = pa.array([None], type=from_type)
            else:
                # Create appropriate test value based on type
                if pa.types.is_integer(from_type):
                    test_array = pa.array([0], type=from_type)
                elif pa.types.is_floating(from_type):
                    test_array = pa.array([0.0], type=from_type)
                elif pa.types.is_string(from_type):
                    test_array = pa.array([""], type=from_type)
                elif pa.types.is_binary(from_type):
                    test_array = pa.array([b""], type=from_type)
                else:
                    return False  # Unknown type
                    
            pa.compute.cast(test_array, to_type)
            return True
        except (pa.ArrowTypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
            return False
    
    def _maybe_make_nullable(self, base_type: pa.DataType, type1: pa.DataType, type2: pa.DataType) -> pa.DataType:
        """Make the base type nullable if either input type is nullable."""
        type1_nullable = hasattr(type1, 'value_type') or type1 == pa.null()
        type2_nullable = hasattr(type2, 'value_type') or type2 == pa.null()
        
        if type1_nullable or type2_nullable:
            # Make nullable if not already
            if hasattr(base_type, 'value_type'):
                return base_type  # Already nullable
            else:
                return pa.field("", base_type, nullable=True).type
        else:
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

    def validate_and_coerce_table(
        self, 
        table: pa.Table, 
        schema_evolution_mode: Optional[str] = None,
        default_schema_consistency_type: Optional['SchemaConsistencyType'] = None
    ) -> Tuple[pa.Table, bool]:
        """Validate and coerce a PyArrow table to match this schema's field types and constraints.
        
        Args:
            table: PyArrow Table to validate and coerce
            schema_evolution_mode: How to handle fields not in schema (MANUAL or AUTO)
            default_schema_consistency_type: Default consistency type for new fields in AUTO mode
            
        Returns:
            Tuple[pa.Table, bool]: Table with data validated/coerced according to schema consistency types,
                                  and a boolean indicating if the schema was modified
            
        Raises:
            ValueError: If validation fails or coercion is not possible
        """
        if not self.field_ids_to_fields:
            # No fields defined in schema, return original table
            return table, False
        
        # Create a mapping from field names to Field objects
        # TODO(pdames): Ensure this works properly with subschemas.
        field_name_to_field = {}
        for field in self.field_ids_to_fields.values():
            field_name_to_field[field.arrow.name] = field
        
        # Track if schema was modified
        schema_modified = False
        
        # Process each column in the table
        new_columns = []
        new_schema_fields = []
        
        for column_name in table.column_names:
            column_data = table.column(column_name)
            
            if column_name in field_name_to_field:
                # Field exists in schema - validate/coerce according to consistency type
                field = field_name_to_field[column_name]
                
                if field.consistency_type == SchemaConsistencyType.VALIDATE:
                    field.validate(column_data)
                    new_columns.append(column_data)
                    new_schema_fields.append(field.arrow)
                elif field.consistency_type == SchemaConsistencyType.COERCE:
                    coerced_data = field.coerce(column_data)
                    new_columns.append(coerced_data)
                    new_schema_fields.append(field.arrow)
                else:
                    # NONE or no consistency type - use type promotion
                    promoted_data, type_was_promoted = field.promote_type_if_needed(column_data)
                    new_columns.append(promoted_data)
                    
                    if type_was_promoted:
                        # Update the field type in the schema
                        promoted_field = pa.field(
                            field.arrow.name,
                            promoted_data.type,
                            nullable=field.arrow.nullable,
                            metadata=field.arrow.metadata
                        )
                        new_schema_fields.append(promoted_field)
                        
                        # Update the field in our schema mapping
                        updated_field = Field.of(promoted_field, consistency_type=field.consistency_type)
                        # Find and update the field in the schema
                        for field_id, existing_field in self.field_ids_to_fields.items():
                            if existing_field.arrow.name == field.arrow.name:
                                self.field_ids_to_fields[field_id] = updated_field
                                schema_modified = True
                                break
                    else:
                        new_schema_fields.append(field.arrow)
            else:
                # Field not in schema - handle based on evolution mode
                if schema_evolution_mode == "AUTO":
                    # Add new field to schema with default consistency type
                    new_field = Field.of(
                        pa.field(column_name, column_data.type, nullable=True),
                        consistency_type=default_schema_consistency_type or SchemaConsistencyType.NONE
                    )
                    # Add the field to our schema
                    self.field_ids_to_fields[len(self.field_ids_to_fields)] = new_field
                    schema_modified = True  # Mark that schema was modified
                    # Process the column with the new field
                    new_columns.append(column_data)
                    new_schema_fields.append(new_field.arrow)
                else:
                    # MANUAL mode or not specified - raise error
                    raise ValueError(f"Field '{column_name}' is not present in the schema")
        
        # Add any missing fields from schema with appropriate handling
        table_column_names = set(table.column_names)
        for field in self.field_ids_to_fields.values():
            if field.arrow.name not in table_column_names:
                consistency_type = field.consistency_type
                
                if consistency_type == SchemaConsistencyType.VALIDATE:
                    raise ValueError(f"Field '{field.arrow.name}' is required but not present in the data")
                elif consistency_type == SchemaConsistencyType.COERCE:
                    # Use future_default if available, otherwise check if nullable
                    if field.future_default is not None:
                        # Create column with future_default value
                        default_array = pa.array([field.future_default] * len(table), type=field.arrow.type)
                        new_columns.append(default_array)
                    elif field.arrow.nullable:
                        # Backfill with nulls if field is nullable
                        null_column = pa.nulls(len(table), type=field.arrow.type)
                        new_columns.append(null_column)
                    else:
                        # Field is not nullable and no future_default - error
                        raise ValueError(f"Field '{field.arrow.name}' is required (not nullable) but not present in the data and no future_default is set")
                    new_schema_fields.append(field.arrow)
                else:
                    # NONE or no consistency type - backfill with future_default or nulls
                    if field.future_default is not None:
                        default_array = pa.array([field.future_default] * len(table), type=field.arrow.type)
                        new_columns.append(default_array)
                    else:
                        null_column = pa.nulls(len(table), type=field.arrow.type)
                        new_columns.append(null_column)
                    new_schema_fields.append(field.arrow)
        
        # If schema was modified, update the schema's arrow property
        if schema_modified:
            self["arrow"] = pa.schema(new_schema_fields)
        
        return pa.table(new_columns, schema=pa.schema(new_schema_fields)), schema_modified

    def coerce(self, dataset: Union[pa.Table, pd.DataFrame, np.ndarray, Any]) -> Union[pa.Table, pd.DataFrame, np.ndarray, Any]:
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
        pa_table, original_type = self._convert_dataset_to_pyarrow(dataset)
        if pa_table is None:
            # Unsupported type, return as-is
            return dataset
        
        # Process columns using field coercion
        coerced_columns, coerced_fields = self._coerce_table_columns(pa_table)
        
        # Reorder columns to match schema order
        reordered_columns, reordered_fields = self._reorder_columns_to_schema(
            coerced_columns, coerced_fields, pa_table
        )
        
        # Create new table with processed columns
        coerced_table = pa.table(reordered_columns, schema=pa.schema(reordered_fields))
        
        # Convert back to original dataset type
        return self._convert_pyarrow_to_dataset(coerced_table, original_type)

    def _convert_dataset_to_pyarrow(self, dataset: Any) -> Tuple[Optional[pa.Table], str]:
        """Convert various dataset types to PyArrow table and track original type.
        
        Args:
            dataset: Input dataset of various types
            
        Returns:
            Tuple of (PyArrow table, original type string) or (None, '') if unsupported
        """
        if isinstance(dataset, pa.Table):
            return dataset, 'pyarrow'
        elif isinstance(dataset, pd.DataFrame):
            return pa.Table.from_pandas(dataset), 'pandas'
        elif hasattr(dataset, 'to_arrow'):  # Polars, Daft
            pa_table = dataset.to_arrow()
            # Better type detection for datasets with to_arrow method
            dataset_module = type(dataset).__module__
            if 'polars' in dataset_module:
                return pa_table, 'polars'
            elif 'daft' in dataset_module:
                return pa_table, 'daft'
            else:
                return pa_table, type(dataset).__name__.lower()
        elif isinstance(dataset, np.ndarray):
            if dataset.ndim == 1:
                pa_table = pa.table([dataset], names=[f"column_0"])
            elif dataset.ndim == 2:
                column_names = [f"column_{i}" for i in range(dataset.shape[1])]
                pa_table = pa.table([dataset[:, i] for i in range(dataset.shape[1])], names=column_names)
            else:
                raise ValueError(f"NumPy arrays with {dataset.ndim} dimensions are not supported")
            return pa_table, 'numpy'
        else:
            return None, ''

    def _coerce_table_columns(self, pa_table: pa.Table) -> Tuple[List[pa.Array], List[pa.Field]]:
        """Process table columns using field coercion and add missing fields.
        
        Args:
            pa_table: PyArrow table to process
            
        Returns:
            Tuple of (list of coerced columns, list of corresponding fields)
        """
        # Create mapping from field names to Field objects
        field_name_to_field = {}
        for field in self.field_ids_to_fields.values():
            field_name_to_field[field.arrow.name] = field
        
        # Process each column using field methods
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
        
        # Add any missing fields from target schema with null values
        target_field_names = {field.arrow.name for field in self.field_ids_to_fields.values()}
        table_field_names = set(pa_table.column_names)
        for field_name in target_field_names - table_field_names:
            field = field_name_to_field[field_name]
            null_column = pa.nulls(len(pa_table), type=field.arrow.type)
            new_columns.append(null_column)
            new_schema_fields.append(field.arrow)
        
        return new_columns, new_schema_fields

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

    def _convert_pyarrow_to_dataset(self, pa_table: pa.Table, original_type: str) -> Any:
        """Convert PyArrow table back to the original dataset type.
        
        Args:
            pa_table: Processed PyArrow table
            original_type: Original dataset type identifier
            
        Returns:
            Dataset converted back to original type
        """
        if original_type == 'pyarrow':
            return pa_table
        elif original_type == 'pandas':
            return pa_table.to_pandas()
        elif original_type == 'polars':
            import polars as pl
            return pl.from_arrow(pa_table)
        elif original_type == 'daft':
            import daft
            return daft.from_arrow(pa_table)
        elif original_type == 'numpy':
            if pa_table.num_columns == 1:
                return pa_table.column(0).to_numpy()
            else:
                return pa_table.to_pandas().values
        else:
            return pa_table

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
