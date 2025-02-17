# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
import copy
import uuid

import msgpack
from typing import Optional, Any, Dict, OrderedDict, Union, List, Callable, Set, Tuple

import pyarrow as pa
from pyarrow import ArrowInvalid

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


SingleSchema = Union[List[Field], pa.Schema]
MultiSchema = Union[Dict[SchemaName, List[Field]], Dict[SchemaName, pa.Schema]]


class Schema(dict):
    @staticmethod
    def of(
        schema: Union[SingleSchema, MultiSchema],
        schema_id: SchemaId = 0,
        native_object: Optional[Any] = None,
    ) -> Schema:
        """
        Creates a DeltaCAT schema from either an Arrow base schema or list
        of DeltaCAT fields.

        Args:
            schema (Union[SingleSchema, MultiSchema]): For a single unnamed
            schema, either an Arrow base schema or list of DeltaCAT fields.
            If an Arrow base schema is given, then a copy of the base schema
            is made with each Arrow field populated with additional metadata.
            Field IDs, merge keys, docs, and default vals will be read from
            each Arrow field's metadata if they exist. Any field missing a
            field ID will be assigned a unique field ID, with assigned field
            IDs either starting from 0 or the max field ID + 1.
            For multiple named schemas, a dictionary of schema names to either
            an arrow base schema or list of DeltaCAT fields. The same rules for
            single unnamed schema creation apply to each named schema. Any field
            missing a field ID will be have a unique field ID assigned
            starting from 0 or the max field ID + 1 across the natural iteration
            order of all dictionary keys first, and all schema fields second.
            All fields across all schemas must have unique names
            (case-insensitive).

            schema_id (SchemaId): Unique ID of schema within its parent table.

            native_object (Optional[Any]): The native object, if any, that this
            schema was converted from.
        Returns:
            A new DeltaCAT Schema.
        """
        # normalize the input as a unified pyarrow schema
        # if the input included multiple subschemas, then also save a mapping
        # from subschema name to its unique field names
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
        # map subschema field names to IDs (lower storage and faster lookup)
        subschema_to_field_ids = {
            name: [Field.of(pyarrow_schema[name]).id for name in field_names]
            for name, field_names in subschema_to_field_names.items()
        }
        # create a final pyarrow schema with populated schema metadata
        schema_metadata[SCHEMA_ID_KEY_NAME] = str(schema_id)
        if subschema_to_field_ids:
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
    def id(self) -> SchemaId:
        return Schema._schema_id(self.arrow)

    @property
    def subschema(self, name: SchemaName) -> Optional[Schema]:
        subschemas = self.subschemas()
        return subschemas.get(name) if subschemas else None

    @property
    def subschemas(self) -> Optional[Dict[SchemaName, Schema]]:
        subschemas = self.get("subschemas")
        if subschemas is not None:
            return subschemas
        subschemas_to_field_ids = self.subschemas_to_field_ids()
        subschemas = (
            (
                {
                    schema_name: Schema.of(
                        schema=pa.schema(
                            [self.field(field_id).arrow for field_id in field_ids]
                        ),
                        schema_id=self.id,
                        native_object=self.native_object,
                    )
                }
                for schema_name, field_ids in subschemas_to_field_ids.items()
            )
            if subschemas_to_field_ids
            else None
        )
        self["subschemas"] = subschemas
        return subschemas

    @property
    def subschema_field_ids(self, name: SchemaName) -> Optional[List[FieldId]]:
        return (
            self.subschemas_to_field_ids.get(name)
            if self.subschemas_to_field_ids
            else None
        )

    @property
    def subschemas_to_field_ids(self) -> Optional[Dict[SchemaName, List[FieldId]]]:
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
    ) -> Optional[Dict[SchemaName, List[FieldId]]]:
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

    @staticmethod
    def _get_field_names(
        schema: SingleSchema,
    ) -> Set[str]:
        if isinstance(schema, pa.Schema):
            return set([name.lower() for name in schema.names])
        elif isinstance(schema, List):  # List[Field]
            return set([field.arrow.name.lower() for field in schema])
        else:
            raise ValueError(f"Unsupported schema base type: {schema}")

    @staticmethod
    def _validate_field_names(
        schema: Union[SingleSchema, MultiSchema],
    ) -> None:
        all_names = []
        if isinstance(schema, dict):  # MultiSchema
            for val in schema.values():
                all_names.append(Schema._get_field_names(val))
        else:  # SingleSchema
            all_names.append(Schema._get_field_names(schema))
        if not all_names:
            raise ValueError(f"Schema must contain at least one field.")
        duplicate_field_names = set.intersection(*all_names)
        if duplicate_field_names:
            raise ValueError(
                f"Expected all schema fields to have unique names "
                f"(case-insensitive), but found the following duplicates: "
                f"{duplicate_field_names}"
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


class SchemaListMap(OrderedDict[str, SchemaList]):
    """
    An OrderedDict wrapper for managing named lists of DeltaCAT schemas.
    Each named schema list in the map represents the schema associated with
    a subset of fields in a DeltaCAT dataset, and each schema in the list
    represents subsequent schema evolution events for that subset of fields.
    """

    @staticmethod
    def of(item: Union[Dict[str, SchemaList], List[SchemaList]]) -> SchemaListMap:
        """
        Create a SchemaListMap from a dictionary or a list of schema data.

        Supported Item Types:
        - If a dict, each key becomes the name of a schema, and each value
          should be either:
           (1) A pre-constructed SchemaList object, or
           (2) A dictionary/JSON-like structure that can be converted
               to a SchemaList (e.g., [{"fields": [...] }]).
        - If a list, each element is treated like the dict values above, but
          are given auto-generated unique names (e.g., UUIDs).

        param: item: A dict mapping names to schemas or a nested schema list.
        returns: SchemaListMap instance containing the provided schema lists.
        raises: ValueError if item is neither a dict nor a list.
        """
        mapping = SchemaListMap()
        if isinstance(item, dict):
            for name, schema_data in item.items():
                mapping[name] = schema_data
        elif isinstance(item, list):
            for schema_data in item:
                mapping[None] = schema_data
        else:
            raise ValueError(f"Cannot create SchemaListMap from {item}")
        return mapping

    @staticmethod
    def _generate_unique_key_name(self) -> str:
        """
        Generate a default unique name.

        returns: A unique default name string.
        """
        return str(uuid.uuid4())

    def __setitem__(
        self,
        key: Optional[str],
        value: SchemaList,
    ) -> None:
        """
        Override __setitem__ to convert the value to a SchemaList. Generates a
        default name if needed.
        Will overwrite any existing schema list with the same key, if called
        directly.
        param: key: The desired key for the schema list; if
        None or empty, a default name is generated.
        param: value: The schema list
        returns: None.
        raises: ValueError if a schema with the given (or generated) key
        already exists.
        """
        schema = value if isinstance(value, SchemaList) else SchemaList(value)

        if not key:
            key = self._generate_unique_key_name()
            logger.debug(f"No schema name provided. Using generated ID: {key}")

        super().__setitem__(key, schema)

    def append(self, key: Optional[str], value: Schema) -> None:
        """
        Appends a new schema to either a new or existing list associated with
        the given map key. If no key is given, then a new unique key will be
        assigned.

        :param key: The desired key; generates one if None.
        :param value: The schema list object.
        :raises ValueError: If the key already exists.
        """
        existing_schemas = self.get(key)
        if existing_schemas is None:
            self.__setitem__(key, SchemaList.of([value]))
        else:
            existing_schemas.append(value)

    def insert(self, key: Optional[str], value: SchemaList) -> None:
        """
        Insert a new schema list into the map.
        Raises an error if the key already exists.

        :param key: The desired key; generates one if None.
        :param value: The schema list object.
        :raises ValueError: If the key already exists.
        """
        if key in self:
            raise ValueError(f"Schema list with name '{key}' already exists.")

        self.__setitem__(key, value)

    def update(self, key: str, value: SchemaList) -> None:
        """
        Update an existing schema list by its key.
        Raises an error if the key does not exist.

        :param key: The key to update.
        :param value: The new schema list object
        :raises KeyError: If the key does not exist.
        """
        if key is not None and key not in self:
            raise KeyError(f"Schema list with name '{key}' does not exist.")

        self.__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        """
        Override __delitem__ to delete a schema.

        param: key (str): The key of the schema to delete.
        returns: None.
        raises: KeyError if the key is not present.
        """
        if key not in self:
            raise KeyError(f"Schema list with name '{key}' does not exist.")

        super().__delitem__(key)

    def equivalent_to(self, other: SchemaListMap) -> bool:
        """
        Compare this SchemaMap with another mapping for equivalence.

        param: other (Any): Another mapping (dict or SchemaMap) to compare against.
        returns: bool indicating whether both mappings have the same keys in the same order and equivalent Schema objects.
        raises: None.
        """
        for k, v in self.items():
            if k not in other:
                return False
            other_schema_list: SchemaList = other[k]
            if len(other_schema_list) != len(v):
                return False
            for i in range(len(other_schema_list)):
                other_schema = other_schema_list[i]
                schema = v[i]
                if not schema.equivalent_to(other_schema):
                    return False
        for k in other.keys():
            if k not in self:
                return False
        return True
