from __future__ import annotations

from dataclasses import dataclass
from typing import MutableMapping, Dict, Iterable, Tuple, Optional

import pyarrow as pa

from deltacat.storage.rivulet.schema.datatype import Datatype


@dataclass
class Field:
    name: str
    datatype: Datatype
    is_merge_key: bool = False

    def __dict__(self):
        # Enable field lookup by name
        return {"name": self.name}

    def __hash__(self):
        return hash((self.name, self.datatype, self.is_merge_key))


class Schema(MutableMapping[str, Field]):
    """
    A mutable mapping representing a schema for structured data, requiring at least one merge key field.

    TODO FUTURE ITERATIONS
    1. We may use Deltacat for schema
    2. We almost certainly want our schema system based on arrow types,
       since many libraries we are integrating with (e.g. daft) are
       interoperable with arrow schemas

    Attributes:
        name: The name of the schema (for storing in dict/map)
       _fields (dict): Maps field names to Field objects.

    Methods:
       from_pyarrow(pyarrow_schema: pa.Schema, primary_key: str) -> Schema:
           Creates a Schema instance from a PyArrow schema.

       __len__() -> int: Returns number of fields.
       __getitem__(key: str) -> Field: Gets field by name.
       __setitem__(key: str, value: Field | Datatype): Adds/updates field.
       __delitem__(key: str): Deletes field if not a merge key.
       __iter__(): Iterates over fields.

       add_field(field: Field): Adds a Field using its name as the key.
       to_pyarrow() -> pa.Schema:
           Converts schema to PyArrow format.

       keys(): Returns field names.
       values(): Returns Field objects.
       items(): Returns (name, Field) pairs.
    """

    def __init__(
        self,
        fields: Iterable[Tuple[str, Datatype] | Field] = None,
        merge_keys: Optional[Iterable[str]] = None,
    ):
        self._fields: Dict[str, Field] = {}
        merge_keys = merge_keys or {}
        if len(fields or []) == 0:
            if len(merge_keys) > 0:
                raise TypeError(
                    "It is invalid to specify merge keys when no fields are specified. Add fields or remove the merge keys."
                )
            return
        # Convert all input tuples to Field objects
        processed_fields = set()
        merge_key_field = None
        for field in fields:
            if isinstance(field, tuple):
                name, datatype = field
                processed_field = Field(
                    name=name, datatype=datatype, is_merge_key=(name in merge_keys)
                )
            elif isinstance(field, Field):
                processed_field = field
                name = field.name
                # Check if merge key status conflicts
                if len(merge_keys) > 0:
                    expected_merge_key_status = name in merge_keys
                    if processed_field.is_merge_key != expected_merge_key_status:
                        raise TypeError(
                            f"Merge key status conflict for field '{name}': "
                            f"Provided as merge key: {expected_merge_key_status}, "
                            f"Field's current status: {processed_field.is_merge_key}. "
                            f"Merge keys should only be defined if raw (name, Datatype) tuples are used."
                        )
            else:
                raise TypeError(f"Unexpected field type: {type(field)}")

            # Locate a merge key to add, and assemble list of fields
            if processed_field.is_merge_key:
                merge_key_field = processed_field
            else:
                processed_fields.add(processed_field)

        # if merge_key_field is None:
        #    raise ValueError("Schema must contain at least one merge key field.")
        # Add merge key first
        if merge_key_field:
            self.add_field(merge_key_field)
        # Add all other fields (including other merge keys)
        for field in processed_fields:
            self.add_field(field)

    @classmethod
    def from_pyarrow(
        cls, pyarrow_schema: pa.Schema, merge_keys: str | Iterable[str] = None
    ) -> Schema:
        """
        Create a Schema instance from a PyArrow schema.

        Args:
            pyarrow_schema: PyArrow Schema to convert
            merge_keys: The optional set of merge keys to add to the schema as it's being translated.
                        These keys must be present in the schema.

        Returns:
            Schema: New Schema instance

        Raises:
            ValueError: If primary_key is not found in schema
        """
        merge_keys = [merge_keys] if isinstance(merge_keys, str) else merge_keys
        fields = {}

        for field in pyarrow_schema:
            dtype = Datatype.from_pyarrow(field.type)
            fields[field.name] = Field(
                field.name, dtype, is_merge_key=(field.name in merge_keys)
            )

        # Validate that the defined merge_keys are present in the fields being added
        missing_keys = merge_keys - fields.keys()
        if missing_keys:
            raise ValueError(
                f"The following merge keys not found in the provided schema: {', '.join(missing_keys)}"
            )

        return cls(fields.values())

    @classmethod
    def merge_all(cls, schemas: Iterable[Schema]) -> Schema:
        """Merges a list of schemas into a new schema"""
        merged = cls({})
        for schema in schemas:
            merged.merge(schema)
        return merged

    def __getitem__(self, key: str):
        return self._fields[key]

    def __setitem__(self, key: str, value: Field | Datatype | Tuple[Datatype, bool]):
        # Create field from [str, Datatype, bool] where bool is merge_key
        if isinstance(value, Field):
            processed_field = value
        elif isinstance(value, Datatype):
            processed_field = Field(
                key, value
            )  # is_merge_key is always false in this case
        elif isinstance(value, tuple):
            (datatype, merge_key) = value
            processed_field = Field(key, datatype, merge_key)
        else:
            raise TypeError(
                "The field must be an instance of the Field class, Datatype, or Tuple[Datatype, bool], where bool is whether the field is a merge key."
            )
        processed_field: Field = processed_field
        # if len(self._fields) == 0 and not processed_field.is_merge_key:
        #    raise TypeError("The first field set on a Schema must be a merge key.")

        self._fields[processed_field.name] = processed_field

    def __delitem__(self, key: str):
        field = self._fields[key]
        if field.is_merge_key:
            raise ValueError("Cannot delete a merge key field")
        del self._fields[key]

    def __len__(self):
        return len(self._fields)

    def __iter__(self):
        return iter(self._fields.keys())

    def __hash__(self):
        return hash((frozenset(self._fields.items())))

    def __eq__(self, other):
        if isinstance(other, Schema):
            return (
                self._fields == other._fields
            )
        return False

    def add_field(self, field: Field):
        """Adds a Field object using its name as the key"""
        self[field.name] = field

    def merge(self, other: Schema) -> None:
        """Merges another schema's fields into the current schema."""
        if not other:
            return
        for name, field in other._fields.items():
            if name in self._fields:
                if self._fields[name] != field:
                    raise ValueError(
                        f"Field '{name}' already exists in the current schema with different definition"
                    )
            else:
                self.add_field(field)

    def to_pyarrow(self) -> pa.Schema:
        """
        Convert the Schema to a PyArrow schema.

        Returns:
            pyarrow.schema: A PyArrow schema representation of this Schema.
        """
        # TODO: Should we track merge_keys as it goes to/from pyarrow?
        fields = []
        for name, field in self._fields.items():
            fields.append(pa.field(name, field.datatype.to_pyarrow()))
        return pa.schema(fields)

    def keys(self):
        return self._fields.keys()

    def values(self):
        return self._fields.values()

    def items(self):
        return self._fields.items()
