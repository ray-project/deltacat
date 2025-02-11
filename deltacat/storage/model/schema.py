# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import copy

import msgpack
from typing import Optional, Any, Dict, OrderedDict, Union, List, Callable

import pyarrow as pa
from pyarrow import ArrowInvalid

from deltacat.storage.model.types import (
    SchemaConsistencyType,
    SortOrder,
    NullOrder,
)

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

# Set max field ID to INT32.MAX_VALUE - 200 for backwards-compatibility with
# Apache Iceberg, which sets aside this range for reserved fields
MAX_FIELD_ID_EXCLUSIVE = 2147483447

SchemaId = int
FieldId = int
FieldName = str
NestedFieldName = List[str]
FieldLocator = Union[FieldName, NestedFieldName, FieldId]


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
        if not (pa.types.is_string(field.type) or pa.types.is_primitive(field.type)):
            raise ValueError(f"Merge key {field} must be a primitive type.")
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


class Schema(dict):
    @staticmethod
    def of(
        schema: Union[List[Field], pa.Schema],
        schema_id: SchemaId = 0,
        metadata: Optional[Dict[str, Any]] = None,
        native_object: Optional[Any] = None,
    ) -> Schema:
        """
        Creates a DeltaCAT schema from either an Arrow base schema or list
        of DeltaCAT fields.

        Args:
            schema (Union[List[Field], pa.Schema]): Arrow base schema or list
            of DeltaCAT fields. If an Arrow base schema is given, then a copy
            of the base schema is made with each Arrow field populated with
            additional metadata. Field IDs, merge keys, docs, and default vals
            will be read from each Arrow field's metadata if they exist. Any
            field missing a field ID will be assigned a unique field ID, with
            assigned field IDs either starting from 0 or the max field ID + 1.

            schema_id (SchemaId): Unique ID of schema within its parent table.

            metadata (Optional[Dict[str, Any]]): Optional metadata key/value
            pairs associated with this schema. Overwrites Arrow base schema
            metadata if present. All values must be coercible to bytes.

            native_object (Optional[Any]): The native object, if any, that this
            schema was converted from.
        Returns:
            A new DeltaCAT Schema.
        """
        # discover assigned field IDs in the given pyarrow schema
        field_ids_to_fields = {}
        final_metadata = metadata or {}
        if isinstance(schema, pa.Schema):
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
            if not final_metadata and schema.metadata:
                final_metadata.update(schema.metadata)
        elif isinstance(schema, List):
            # convert input fields to a pyarrow schema to populate field paths
            return Schema.of(
                schema=pa.schema(fields=[field.arrow for field in schema]),
                schema_id=schema_id,
                metadata=metadata,
                native_object=native_object,
            )
        else:
            raise ValueError(f"Unsupported schema base type: {schema}")
        if not field_ids_to_fields:
            raise ValueError(f"Schema must contain at least one field.")
        # populate merge keys
        merge_keys = [
            field.id for field in field_ids_to_fields.values() if field.is_merge_key
        ]
        # create a new pyarrow schema with field ID, doc, etc. field metadata
        final_metadata[SCHEMA_ID_KEY_NAME] = str(schema_id)
        final_schema = pa.schema(
            fields=[field.arrow for field in field_ids_to_fields.values()],
            metadata=final_metadata,
        )
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

    def equivalent_to(self, other):
        try:
            return self.serialize().to_pybytes() == other.serialize().to_pybytes()
        except Exception:
            return False

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
    def schema_id(self) -> SchemaId:
        return Schema._schema_id(self.arrow)

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
    def _field_name_to_field_id(
        schema: pa.Schema,
        name: Union[FieldName, NestedFieldName],
    ) -> FieldId:
        if isinstance(name, str):
            return Field.of(schema[name]).id
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
        _field = Field.of(field)
        field_id = _field.id if _field is not None else max_field_id
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


class SchemaMap(OrderedDict):
    """
    An OrderedDict wrapper for managing DeltaCAT Schema objects.

    method: of(item)
    method: __setitem__(key, value)
    method: update(iterable_or_mapping, **kwargs)
    method: __delitem__(key)
    method: get_schemas()
    method: equivalent_to(other)
    """

    @staticmethod
    def of(item: Union[Dict[str, Any], List[Any]]) -> SchemaMap:
        """
        Create a SchemaMap from a dictionary or a list of schema data.

        param: item (Union[Dict[str, Any], List[Any]]): A dict mapping names to schema data or a list of schema data.
        returns: SchemaMap instance containing the provided schemas.
        raises: ValueError if item is neither a dict nor a list.
        """
        mapping = SchemaMap()
        if isinstance(item, dict):
            for name, schema_data in item.items():
                mapping[name] = schema_data
        elif isinstance(item, list):
            for schema_data in item:
                mapping[None] = schema_data
        else:
            raise ValueError(f"Cannot create SchemaMap from {item}")
        return mapping

    def _generate_default_name(self, schema: Schema) -> str:
        """
        Generate a default unique name for the given schema.
        Uses the schema's own name attribute if available; otherwise, creates one based on the current number of entries.
        Ensures uniqueness by appending an incremental suffix if needed.

        param: schema (Schema): The schema for which to generate a name.
        returns: A unique default name (str) for the schema.
        raises: None.
        """
        candidate = getattr(schema, "name", None)
        if not candidate:
            candidate = f"{len(self) + 1}"
        base_candidate = candidate
        counter = 1
        while candidate in self:
            candidate = f"{base_candidate}_{counter}"
            counter += 1
        return candidate

    def __setitem__(
        self, key: Optional[str], value: Union[Schema, Dict[str, Any]]
    ) -> None:
        """
        Override __setitem__ to convert the value to a Schema, generate a default name if needed,
        and check for duplicates.

        param: key (Optional[str]): The desired key for the schema; if None or empty, a default name is generated.
        param: value (Union[Schema, Dict[str, Any]]): The schema or dict convertible to a Schema.
        returns: None.
        raises: ValueError if a schema with the given (or generated) key already exists.
        """
        schema_obj = value if isinstance(value, Schema) else Schema(value)

        if not key:
            key = self._generate_default_name(schema_obj)

        if key in self:
            raise ValueError(
                f"Schema with name '{key}' already exists in the SchemaMap."
            )

        super().__setitem__(key, schema_obj)

    def __delitem__(self, key: str) -> None:
        """
        Override __delitem__ to delete a schema.

        param: key (str): The key of the schema to delete.
        returns: None.
        raises: KeyError if the key is not present.
        """
        if key not in self:
            raise KeyError(f"Schema with name '{key}' does not exist.")

        super().__delitem__(key)

    def get_schemas(self) -> List[Schema]:
        """
        Retrieve all stored schemas as a list.

        param: None.
        returns: List[Schema] containing all schemas in insertion order.
        raises: None.
        """
        return list(self.values())

    def equivalent_to(self, other: SchemaMap) -> bool:
        """
        Compare this SchemaMap with another mapping for equivalence.

        param: other (Any): Another mapping (dict or SchemaMap) to compare against.
        returns: bool indicating whether both mappings have the same keys in the same order and equivalent Schema objects.
        raises: None.
        """
        if not isinstance(other, (dict, SchemaMap)):
            return False
        if len(self) != len(other):
            return False
        for (key_self, schema_self), (key_other, schema_other) in zip(
            self.items(), other.items()
        ):
            if key_self != key_other:
                return False
            if not schema_self.equivalent_to(schema_other):
                return False
        return True
