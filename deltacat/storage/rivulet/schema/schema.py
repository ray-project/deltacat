from __future__ import annotations

from typing import MutableMapping, List, Dict
from dataclasses import dataclass
import pyarrow as pa

from deltacat.storage.rivulet.schema.datatype import Datatype


@dataclass
class Field:
    name: str
    datatype: Datatype

    def __dict__(self):
        return {"name": self.name, "datatype": self.datatype.type_name}


class Schema(MutableMapping[str, Field]):
    """
    TODO FUTURE ITERATIONS
    1. We may use Deltacat for schema
    2. We almost certainly want our schema system based on arrow types,
        since many libraries we are integration wtih (e.g. daft) are
        interoperable with arrow schemas

    A class representing a schema for structured data. The Schema class holds a collection of fields, each with a name and a datatype.

    In riv-connector libraries, we will add helpers to generate riv Schema from other Schema systems

    Attributes:
        fields (dict): A dictionary mapping field names to Field objects.

    Methods:
        add_field(name: str, field: Field | Datatype): Adds a new field to the schema.

        __len__(): Returns the number of fields in the schema.
        __getitem__(key): Allows accessing fields using square bracket notation.
        __repr__(): Returns a string representation of the schema.
        __iter__(): Allows iterating over the fields in the schema.
        keys(): Returns an iterator over the field names.
        values(): Returns an iterator over the Field objects.
        items(): Returns an iterator over (name, Field) pairs.
    """

    def __init__(self, fields: dict[str, Field | Datatype], primary_key: str):
        self.fields: dict[str, Field] = {}
        found_pk = False
        for name, field in fields.items():
            self.__setitem__(name, field)
            # Validate we have traversed primary key somewhere in schema
            if name == primary_key:
                found_pk = True
                self.primary_key: Field = self.fields[name]
        if not found_pk:
            raise ValueError(f"Did not find primary key '{primary_key}' in Schema")

    def add_field(self, name: str, field: Field | Datatype):
        if isinstance(field, Datatype):
            self.fields[name] = Field(name, field)
        elif isinstance(field, Field):
            self.fields[name] = field
        else:
            raise TypeError(
                "The field must be an instance of the Field class or a Datatype."
            )

    def filter(self, fields: List[str]):
        # Remove fields which are not in the filter list. Raise exception if primary key not present
        fields_to_remove = {
            field for field in self.fields.keys() if field not in fields
        }
        if self.primary_key.name in fields_to_remove:
            raise ValueError(
                f"Schema filter must contain the primary key field: '{self.primary_key.name}'"
            )
        for field_name in fields_to_remove:
            self.__delitem__(field_name)

    def to_pyarrow_schema(self) -> pa.Schema:
        """
        Convert the Schema to a PyArrow schema.

        Returns:
            pyarrow.schema: A PyArrow schema representation of this Schema.
        """
        fields = []
        for name, field in self.fields.items():
            fields.append(pa.field(name, field.datatype.to_pyarrow()))
        return pa.schema(fields)

    @classmethod
    def from_pyarrow_schema(
        cls, pyarrow_schema: pa.Schema, primary_key: str
    ) -> "Schema":
        """
        Create a Schema instance from a PyArrow schema.

        Args:
            pyarrow_schema: PyArrow Schema to convert
            primary_key: Name of the field to use as primary key

        Returns:
            Schema: New Schema instance

        Raises:
            ValueError: If primary_key is not found in schema
        """
        fields = {}

        for field in pyarrow_schema:
            dtype = Datatype.from_pyarrow(field.type)
            fields[field.name] = Field(field.name, dtype)

        return cls(fields, primary_key)

    def __dict__(self) -> Dict[str, Dict[str, str]]:
        """
        for json encoding
        """
        fields = [f.__dict__() for f in self.fields.values()]

        return {"fields": fields, "primary_key": self.primary_key.__dict__()}

    @classmethod
    def from_json(cls, json_data: Dict) -> "Schema":
        """
        Construct a Schema instance from JSON data.

        Raises:
            ValueError: If the JSON data is invalid or missing required fields.
        """
        if "fields" not in json_data or "primary_key" not in json_data:
            raise ValueError("Invalid JSON data: missing 'fields' or 'primary_key'")

        fields = {}
        for field_data in json_data["fields"]:
            if "name" not in field_data or "datatype" not in field_data:
                raise ValueError("Invalid field data: missing 'name' or 'datatype'")
            name = field_data["name"]
            datatype = Datatype(field_data["datatype"])
            fields[name] = Field(name, datatype)

        primary_key = json_data["primary_key"]["name"]
        return cls(fields, primary_key)

    def __len__(self):
        return len(self.fields)

    def __getitem__(self, key: str):
        return self.fields[key]

    def __setitem__(self, key: str, value: Field | Datatype):
        if isinstance(value, Datatype):
            self.fields[key] = Field(key, value)
        elif isinstance(value, Field):
            if value.name != key:
                raise ValueError(
                    f"Field name '{value.name}' does not match schema key '{key}'"
                )
            self.fields[key] = value
        else:
            raise TypeError(f"Invalid type for field '{key}': {type(value)}")

    def __delitem__(self, key: str):
        if key == self.primary_key.name:
            raise ValueError("Cannot delete the primary key field")
        del self.fields[key]

    def __repr__(self):
        field_reprs = [
            f"{name}: {field.datatype}" for name, field in self.fields.items()
        ]
        return (
            f"Schema({', '.join(field_reprs)}, primary_key='{self.primary_key.name}')"
        )

    def __iter__(self):
        return iter(self.fields.values())

    def keys(self):
        return self.fields.keys()

    def values(self):
        return self.fields.values()

    def items(self):
        return self.fields.items()

    def __hash__(self):
        # Create a frozenset of (name, datatype) tuples for all fields
        field_tuples = frozenset(
            (name, field.datatype) for name, field in self.fields.items()
        )

        # Combine the hash of the field_tuples with the hash of the primary key name
        return hash((field_tuples, self.primary_key.name))

    def __deepcopy__(self):
        """
        Create and return a deep copy of the Schema.
        """
        new_fields = {
            name: Field(field.name, field.datatype)
            for name, field in self.fields.items()
        }
        return Schema(new_fields, self.primary_key.name)
