from __future__ import annotations

import itertools
import os
from typing import Dict, List, Optional, Tuple, Iterable, Iterator

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.file_location_provider import FileLocationProvider
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat.storage.rivulet import Schema, Field
from deltacat.utils.export import export_dataset
from .schema.schema import Datatype

from deltacat.storage.rivulet.reader.data_scan import DataScan
from deltacat.storage.rivulet.reader.dataset_reader import DatasetReader
from deltacat.storage.rivulet.reader.query_expression import QueryExpression

from deltacat.storage.rivulet.writer.dataset_writer import DatasetWriter
from deltacat.storage.rivulet.writer.memtable_dataset_writer import (
    MemtableDatasetWriter,
)

import pyarrow as pa
import pyarrow.dataset
import pyarrow.json
import pyarrow.csv
import pyarrow.parquet

# These are the hardcoded default schema names
ALL = "all"
DEFAULT = "default"


class FieldsAccessor:
    """Accessor class used to make it easy to do actions like dataset.fields['name'] to work with fields in the Dataset.
    All field mutation and access should come through this class, or through the public helper functions in the dataset
    class, e.g. 'add_fields()'.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def __getitem__(self, field_name: str) -> Field:
        if field_name not in self.dataset.schemas[ALL]:
            raise KeyError(f"Field '{field_name}' not found in dataset.")
        return self.dataset.schemas[ALL][field_name]

    def __setitem__(self, field_name: str, field: Field):
        if not isinstance(field, Field):
            raise TypeError("Value must be a Field object")
        self.dataset.schemas[ALL][field_name] = field

    def __delitem__(self, field_name: str):
        if field_name not in self.dataset.schemas[ALL]:
            raise ValueError(f"Field '{field_name}' does not exist.")
        del self.dataset.schemas[ALL][field_name]
        for schema in self.dataset._schemas.values():
            if field_name in schema:
                del schema[field_name]

    def __contains__(self, field_name: str) -> bool:
        """Allows 'field_name in dataset.fields' checks."""
        return field_name in self.dataset.schemas[ALL]

    def __iter__(self):
        return iter(self.dataset.schemas[ALL].items())

    def __len__(self):
        return len(self.dataset.schemas[ALL])

    def __repr__(self):
        return f"Fields({list(self.dataset.schemas['all'].keys())})"

    def add(
        self,
        name: str,
        datatype: Datatype,
        *,
        schema_name: str = DEFAULT,
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
    class, e.g. 'add_fields()'.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def __getitem__(self, name: str) -> Schema:
        if name not in self.dataset._schemas:
            raise KeyError(f"Schema '{name}' not found.")
        return self.dataset._schemas[name]

    def __setitem__(self, schema_name: str, field_names: List[str]) -> None:
        self.dataset._add_fields_to_schema(
            field_names=field_names, schema_name=schema_name
        )

    def __delitem__(self, schema_name: str) -> None:
        if schema_name not in self.dataset._schemas:
            raise ValueError(f"Schema '{schema_name}' does not exist.")
        if schema_name == ALL:
            raise ValueError("Cannot remove the 'all' schema.")
        del self.dataset._schemas[schema_name]

    def __contains__(self, schema_name: str) -> bool:
        return schema_name in self.dataset._schemas

    def __iter__(self) -> Iterator[str]:
        return iter(self.dataset._schemas.keys())

    def __len__(self) -> int:
        return len(self.dataset._schemas)

    def __repr__(self) -> str:
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
            _schemas (dict[str, Schema]):
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
        self._schemas: Dict[str, Schema] = {ALL: Schema()}

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
    ) -> Dataset:
        """
        Create a Dataset from parquet files.

        TODO: Make pluggable(from_x) with other file formats.

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
        pyarrow_dataset = pyarrow.dataset.dataset(file_uri)

        if schema_mode == "intersect":
            schemas = [pyarrow.parquet.read_schema(f) for f in pyarrow_dataset.files]
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
            schemas = [pyarrow.parquet.read_schema(f) for f in pyarrow_dataset.files]
            pyarrow_schema = pa.unify_schemas(schemas)

        dataset_schema = Schema.from_pyarrow(pyarrow_schema, merge_keys)

        # TODO the file URI never gets stored/saved, do we need to do so?
        dataset = cls(
            dataset_name=name, metadata_uri=metadata_uri, schema=dataset_schema
        )

        # TODO: avoid write! associate fields with their source data.
        writer = dataset.writer()

        for batch in pyarrow_dataset.scanner().to_batches():
            writer.write(batch)
        writer.flush()

        return dataset

    @classmethod
    def from_json(
        cls,
        name: str,
        file_uri: str,
        merge_keys: str | Iterable[str],
        metadata_uri: Optional[str] = None,
        schema_mode: str = "union",
    ) -> "Dataset":
        """
        Create a Dataset from a single JSON file.

        TODO: Add support for reading directories with multiple JSON files.

        Args:
            name: Unique identifier for the dataset.
            metadata_uri: Base URI for the dataset, where dataset metadata is stored. If not specified, will be placed in ${file_uri}/riv-meta
            file_uri: Path to a single JSON file.
            merge_keys: Fields to specify as merge keys for future 'zipper merge' operations on the dataset.
            schema_mode: Currently ignored as this is for a single file.

        Returns:
            Dataset: New dataset instance with the schema automatically inferred
                     from the JSON file.
        """
        metadata_uri = metadata_uri or os.path.join(file_uri, "riv-meta")

        # Read the JSON file into a PyArrow Table
        pyarrow_table = pyarrow.json.read_json(file_uri)
        pyarrow_schema = pyarrow_table.schema

        # Create the dataset schema
        dataset_schema = Schema.from_pyarrow(pyarrow_schema, merge_keys)

        # Create the Dataset instance
        dataset = cls(
            dataset_name=name, metadata_uri=metadata_uri, schema=dataset_schema
        )

        writer = dataset.writer()
        writer.write(pyarrow_table.to_batches())
        writer.flush()

        return dataset

    @classmethod
    def from_csv(
        cls,
        name: str,
        file_uri: str,
        merge_keys: str | Iterable[str],
        metadata_uri: Optional[str] = None,
        schema_mode: str = "union",
    ) -> "Dataset":
        """
        Create a Dataset from a single JSON file.

        TODO: Add support for reading directories with multiple CSV files.

        Args:
            name: Unique identifier for the dataset.
            metadata_uri: Base URI for the dataset, where dataset metadata is stored. If not specified, will be placed in ${file_uri}/riv-meta
            file_uri: Path to a single CSV file.
            merge_keys: Fields to specify as merge keys for future 'zipper merge' operations on the dataset.
            schema_mode: Currently ignored as this is for a single file.

        Returns:
            Dataset: New dataset instance with the schema automatically inferred
                     from the CSV file.
        """
        metadata_uri = metadata_uri or os.path.join(file_uri, "riv-meta")

        # Read the CSV file into a PyArrow Table
        table = pyarrow.csv.read_csv(file_uri)
        pyarrow_schema = table.schema

        # Create the dataset schema
        dataset_schema = Schema.from_pyarrow(pyarrow_schema, merge_keys)

        # Create the Dataset instance
        dataset = cls(
            dataset_name=name, metadata_uri=metadata_uri, schema=dataset_schema
        )

        writer = dataset.writer()
        writer.write(table.to_batches())
        writer.flush()

        return dataset

    def print(self, num_records: int = 10) -> None:
        """Prints the first `num_records` records in the dataset."""
        records = self.scan().to_pydict()
        for record in itertools.islice(records, num_records):
            print(record)

    def export(
        self,
        file_uri: str,
        format: str = "parquet",
        query: QueryExpression = QueryExpression(),
    ) -> None:
        """Export the dataset to a file.

        Args:
            file_uri: The URI to write the dataset to.
            format: The format to write the dataset in. Options are [parquet, feather].
        """
        export_dataset(self, file_uri, format, query)

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
            if name not in self.schemas[ALL]:
                raise ValueError(f"Field '{name}' does not exist in the dataset.")

        # Begin adding schema/fields to the schema map, this must be completed as a transaction w/o error or the schemas will be
        # left in an undefined state.
        # TODO: This is not threadsafe

        # Create the empty schema if it doesn't exist
        if schema_name not in self._schemas:
            self._schemas[schema_name] = Schema()

        # Add the (existing) fields from the 'all' schema to the defined schema
        for name in field_names:
            self._schemas[schema_name].add_field(self.schemas[ALL][name])

    def add_fields(
        self,
        fields: Iterable[Tuple[str, Datatype] | Field],
        schema_name: str = DEFAULT,
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
                # Check if merge key status on field conflicts with any provided status form merge_key list
                if name in merge_keys:
                    if processed_field.is_merge_key is not True:
                        raise TypeError(
                            f"Merge key status conflict for field '{name}'. "
                            f"Field({name}).is_merge_key is set to 'false', but was '{name}' was provided in the merge_keys list. "
                            f"Remove {name} from merge_keys or change Field({name}).is_merge_key to true."
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
                    f"The following merge keys were not found in the provided fields: {', '.join(missing_keys)}"
                )

        # Add/update the schema
        self.add_schema(Schema(processed_fields), schema_name=schema_name)

    def add_schema(self, schema: Schema, schema_name: str = DEFAULT) -> None:
        """
        Merges the provided schema into the existing schema, or creates a new schema if it doesn't exist.
        Will also add all fields to the 'all' schema.

        Args:
            schema: The Schema to add or merge into the named dataset schema.
            schema_name: The name of the schema to update or create. Defaults to "default".

        Raises:
            ValueError: If fields in the provided schema conflict with existing fields in the dataset.
        """
        schema_name = schema_name or DEFAULT

        # Check for any fields that already exist
        for field in schema.values():
            if field.name in self.schemas[ALL]:
                existing_field = self.schemas[ALL][field.name]
                if existing_field is not None and field != existing_field:
                    raise ValueError(
                        f"Field '{field.name}' already exists and is of a different type: New({field}) Existing({existing_field})."
                    )

        # Begin adding fields, this must be completed as a transaction w/o error or the field maps will be
        # left in an undefined state.
        # TODO: This is not threadsafe

        # Create schema if it doesn't exist
        if schema_name not in self._schemas:
            self._schemas[schema_name] = Schema()

        # Merge new schema into 'all' and provided schema_name
        self._schemas[schema_name].merge(schema)
        self._schemas[ALL].merge(schema)

    def get_merge_keys(self) -> Iterable[str]:
        """Return a list of all merge keys."""
        return self.schemas[ALL].get_merge_keys()

    def writer(
        self,
        schema_name: str = None,
        file_format: str | None = None,
    ) -> DatasetWriter:
        """Create a new (stateful) writer using the schema at the conjunction of given schemas.

        Invoking this will register any unregistered schemas.

        :param schema_name: The schema to use for write, if None, uses the 'all' schema
        :param file_format Write data to this format. Options are [parquet, feather]. If not specified, library will choose
            based on schema
        :return: new dataset writer with a schema at the conjunction of the given schemas
        """
        schema_name = schema_name or ALL

        return MemtableDatasetWriter(
            self._location_provider, self.schemas[schema_name], file_format
        )

    def scan(
        self, query: QueryExpression = QueryExpression(), schema_name: str = ALL
    ) -> DataScan:
        dataset_reader = DatasetReader(self._metastore)
        return DataScan(self.schemas[schema_name], query, dataset_reader)
