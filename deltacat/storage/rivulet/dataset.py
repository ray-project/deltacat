from __future__ import annotations

import os
from typing import Dict, List, Optional, Tuple, Iterable

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.file_location_provider import FileLocationProvider
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat.storage.rivulet import Schema, Field
from .schema.schema import Datatype

from deltacat.storage.rivulet.reader.data_scan import DataScan
from deltacat.storage.rivulet.reader.dataset_reader import DatasetReader
from deltacat.storage.rivulet.reader.query_expression import QueryExpression

from deltacat.storage.rivulet.writer.dataset_writer import DatasetWriter
from deltacat.storage.rivulet.writer.memtable_dataset_writer import (
    MemtableDatasetWriter,
)

import pyarrow as pa


class FieldsAccessor:
    """Accessor class used to make it easy to do actions like dataset.fields['name'] to work with fields in the Dataset.
    All field mutation and access should come through this class, or through the public helper functions in the dataset
    class, e.g. 'add_fields()'. Do not use internal functions or directly access _schemas, as it may result in
    unexpected behavior, such as fields being removed from the dataset while still showing up in the schemas.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def __getitem__(self, field_name: str) -> Field:
        if field_name not in self.dataset.schemas["all"]:
            raise KeyError(f"Field '{field_name}' not found in dataset.")
        return self.dataset.schemas["all"][field_name]

    def __setitem__(self, field_name: str, field: Field):
        if not isinstance(field, Field):
            raise TypeError("Value must be a Field object")
        self.dataset.schemas["all"][field_name] = field

    def __delitem__(self, field_name: str):
        if field_name not in self.dataset.schemas["all"]:
            raise ValueError(f"Field '{field_name}' does not exist.")
        del self.dataset.schemas["all"][field_name]
        for schema in self.dataset._schemas.values():
            if field_name in schema:
                del schema[field_name]

    def __contains__(self, field_name: str) -> bool:
        """Allows 'field_name in dataset.fields' checks."""
        return field_name in self.dataset.schemas["all"]

    def __iter__(self):
        return iter(self.dataset.schemas["all"].items())

    def __len__(self):
        return len(self.dataset.schemas["all"])

    def __repr__(self):
        return f"Fields({list(self.dataset.schemas['all'].keys())})"

    def add(
        self,
        name: str,
        datatype: Datatype,
        *,
        schema_name: str = "default",
        is_merge_key: bool = False,
    ):
        """Simple helper to add a field when you don't have a Field object"""
        self.dataset.add_fields(
            fields=[(name, datatype)],
            schema_name=schema_name,
            merge_keys=[name] if is_merge_key else None,
        )


class SchemasAccessor:
    """Accessor class used to make it easy to do actions like dataset.schemas['all'] to work with schemas in the Dataset.
    All schema mutation and access should come through this class, or through the public helper functions in the dataset
    class, e.g. 'add_fields()'. Do not use internal functions or directly access _schemas, as it may result in
    unexpected behavior.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def __getitem__(self, name: str) -> Schema:
        if name not in self.dataset._schemas:
            raise KeyError(f"Schema '{name}' not found.")
        return self.dataset._schemas[name]

    def __setitem__(self, schema_name: str, field_names: List[str]):
        self.dataset._add_fields_to_schema(
            field_names=field_names, schema_name=schema_name
        )

    def __delitem__(self, schema_name: str):
        if schema_name not in self.dataset._schemas:
            raise ValueError(f"Schema '{schema_name}' does not exist.")
        if schema_name == "all":
            raise ValueError("Cannot remove the 'all' schema.")
        del self.dataset._schemas[schema_name]

    def __contains__(self, schema_name: str) -> bool:
        return schema_name in self.dataset._schemas

    def __iter__(self):
        return iter(self.dataset._schemas.keys())

    def __len__(self):
        return len(self.dataset._schemas)

    def __repr__(self):
        return f"SchemasAccessor({list(self.dataset._schemas.keys())})"


class Dataset:
    def __init__(
        self,
        *,
        dataset_name: str,
        metadata_uri: Optional[str] = None,
        schema: Optional[Schema] = None,
        schema_name: Optional[str] = None,
    ):
        """
        Create an empty Dataset w/ optional schema. This method is typically only used for small datasets that are manually created.
        Use the Dataset.from_*() to create a dataset from existing data.

        Args:
            dataset_name: Unique identifier for the dataset.
            metadata_uri: The directory to store the _metadata_folder ('.riv-meta-{dataset_name}') containing dataset metadata.
                          If not provided, we'll use the local directory.

        Private Attributes:
            _metadata_folder (str):
                The folder name where metadata for the dataset is kept. It will always be
                '.riv-meta-{dataset_name}', and be stored under `metadata_uri`.
            _schemas (dict[str, dict[str, Field]]):
                Maps a schemas by name (e.g., "default", "analytics"). This is how fields in the dataset are grouped and accessed.
            _file_store (FileStore):
                The FileStore used by the Dataset class for reading and writing metadata files.
            _location_provider (FileLocationProvider):
                Used to resolve file URIs within the `_file_store`.
            _metastore (DatasetMetastore):
                Uses the _file_store and _location_provider to manage metadata (schema, stats, file locations, manifests, etc.) for this Dataset.
        """
        if not dataset_name or not isinstance(dataset_name, str):
            raise ValueError("Name must be a non-empty string")

        self.dataset_name = dataset_name
        self._metadata_folder = f".riv-meta-{dataset_name}"
        if metadata_uri:
            self._metadata_path = os.path.join(metadata_uri, self._metadata_folder)
        else:
            self._metadata_path = self._metadata_folder

        # Map of schema_name -> {field_name -> Field}, with initial empty 'all' schema
        self._schemas: Dict[str, Schema] = {"all": Schema()}

        # Initialize metadata handling
        self._file_store = FileStore()
        self._location_provider = FileLocationProvider(
            self._metadata_path, self._file_store
        )
        self._metastore = DatasetMetastore(self._location_provider, self._file_store)

        # Initialize accessors
        self.fields = FieldsAccessor(self)
        self.schemas = SchemasAccessor(self)

        # Add any fields optionally provided to the provided schema_name
        if schema:
            self.add_schema(schema, schema_name=schema_name)

    @classmethod
    def from_parquet(
        cls,
        name: str,
        file_uri: str,
        merge_keys: str | Iterable[str],
        metadata_uri: Optional[str] = None,
        schema_mode: str = "union",
    ):
        """
        Create a Dataset from parquet files.

        Args:
            name: Unique identifier for the dataset.
            metadata_uri: Base URI for the dataset, where dataset metadata is stored. If not specified, will be placed in ${file_uri}/riv-meta
            file_uri: Path to parquet file(s)
            merge_keys: Fields to specify as merge keys for future 'zipper merge' operations on the dataset
            schema_mode: Schema combination mode. Options:
                - 'union': Use unified schema with all columns
                - 'intersect': Use only common columns across files

        Returns:
            Dataset: New dataset instance with the schema automatically inferred from the source parquet files
        """
        metadata_uri = metadata_uri or os.path.join(file_uri, "riv-meta")
        dataset = pa.dataset.dataset(file_uri)

        if schema_mode == "intersect":
            schemas = [pa.parquet.read_schema(f) for f in dataset.files]
            # Find common columns across all schemas
            common_columns = set(schemas[0].names)
            for schema in schemas[1:]:
                common_columns.intersection_update(schema.names)

            # Create a new schema with only common columns
            intersect_schema = pa.schema(
                [(name, schemas[0].field(name).type) for name in common_columns]
            )
            pyarrow_schema = intersect_schema
        else:
            schemas = [pa.parquet.read_schema(f) for f in dataset.files]
            pyarrow_schema = pa.unify_schemas(schemas)

        dataset_schema = Schema.from_pyarrow(pyarrow_schema, merge_keys)
        # TODO the file URI never gets stored/saved, do we need to do so?
        return cls(dataset_name=name, metadata_uri=metadata_uri, schema=dataset_schema)

    def _add_fields_to_schema(
        self,
        field_names: Iterable[str],
        schema_name: str,
    ) -> None:
        """
        An internal function to add fields to a new or existing schema (creating the schema if it doesn't exist).
        Note: This function will error if the fields do not exist (rather than add them).

        Args:
            field_names: List of field names to add to the schema.
            schema_name: Name of the schema.

        Raises:
            ValueError: If any field does not exist in the dataset.
        """

        # Input Validation
        # Ensure all fields exist
        for name in field_names:
            if name not in self.schemas["all"]:
                raise ValueError(f"Field '{name}' does not exist in the dataset.")

        # Begin adding schema/fields to the schema map, this must be completed as a transaction w/o error or the schemas will be
        # left in an undefined state.
        # TODO: This is not threadsafe

        # Create the empty schema if it doesn't exist
        if schema_name not in self._schemas:
            self._schemas[schema_name] = Schema()

        # Add the (existing) fields from the 'all' schema to the defined schema
        for name in field_names:
            self._schemas[schema_name].add_field(self.schemas["all"][name])

    def add_fields(
        self,
        fields: Iterable[Tuple[str, Datatype] | Field],
        schema_name: str = "default",
        merge_keys: Optional[Iterable[str]] = None,
    ) -> None:
        """
        Helper function to simultaneously add a set of new fields, put them under a new or existing schema,
        and add merge keys, all in a single function.

        This can also be done field by field using:
        * dataset.fields.add(name=.., datatype=.., ...)

        Or it can be done by using add_schema().

        Args:
            fields: List of tuples (name, datatype) or Field objects.
            schema_name: User defined name to give to the group of fields.
            merge_keys: Optional list of field names to set as merge keys.

        Raises:
            ValueError: If any field has the same name as an existing field.
        """
        if not fields:
            raise ValueError("No fields provided.")
        merge_keys = merge_keys or {}

        # Convert all input tuples to Field objects
        processed_fields = []
        field_names = set()

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
                if merge_keys is not None:
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

            processed_fields.append(processed_field)
            field_names.add(name)

        # Input Validation
        # Check that merge_keys defined are present in the fields being added
        if merge_keys:
            missing_keys = set(merge_keys) - field_names
            if missing_keys:
                raise ValueError(
                    f"The following merge keys not found in the provided fields: {', '.join(missing_keys)}"
                )

        # If this is a new schema it must have a merge key defined.
        # if schema_name not in self._schemas and merge_keys is None:
        #    raise ValueError(f"New schemas must have at least one merge_key defined.")

        # Add/update the schema
        self.add_schema(Schema(processed_fields), schema_name=schema_name)

    def add_schema(self, schema: Schema, schema_name: str = "default") -> None:
        """
        Merges the provided schema into the existing schema, or creates a new schema if it doesn't exist.
        Will also add all fields to the 'all' schema.

        Args:
            schema: The Schema to add or merge into the named dataset schema.
            schema_name: The name of the schema to update or create. Defaults to "default".

        Raises:
            ValueError: If fields in the provided schema conflict with existing fields in the dataset.
        """
        schema_name = schema_name or "default"

        # Check for any fields that already exist
        for field in schema.values():
            if field.name in self.schemas["all"]:
                raise ValueError(f"Field '{field.name}' already exists.")

        # Begin adding fields, this must be completed as a transaction w/o error or the field maps will be
        # left in an undefined state.
        # TODO: This is not threadsafe

        # Create schema if it doesn't exist
        if schema_name not in self._schemas:
            self._schemas[schema_name] = Schema()

        # Merge new schema into 'all' and provided schema_name
        self._schemas[schema_name].merge(schema)
        self._schemas["all"].merge(schema)

    def get_merge_keys(self) -> Iterable[str]:
        """Return a list of all merge keys."""
        return [
            field.name for field in self.schemas["all"].values() if field.is_merge_key
        ]

    def writer(
        self,
        schemas: Schema | Iterable[Schema] = None,
        file_format: str | None = None,
    ) -> DatasetWriter:
        """Create a new (stateful) writer using the schema at the conjunction of given schemas.

        Invoking this will register any unregistered schemas.

        :param schemas: one or more schemas that form the schema of the data being written.
                             If None, uses the dataset-level schema
        :param file_format Write data to this format. Options are [parquet, feather]. If not specified, library will choose
            based on schema
        :return: new dataset writer with a schema at the conjunction of the given schemas
        """
        # Listify single schema
        schemas = [schemas] if not isinstance(schemas, Iterable) else schemas
        # If no schema was specified, use 'all' schema
        if not schemas:
            schemas = self.schemas["all"]

        merged_schema = Schema.merge_all(schemas)
        return MemtableDatasetWriter(
            self._location_provider, merged_schema, file_format
        )

    def scan(
        self, query: QueryExpression = QueryExpression(), schema_name: str = "all"
    ) -> DataScan:
        dataset_reader = DatasetReader(self._metastore)
        return DataScan(self.schemas[schema_name], query, dataset_reader)
