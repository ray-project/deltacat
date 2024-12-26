from __future__ import annotations

from typing import Dict, Any, List, Iterable, Union
import pyarrow as pa

from deltacat.storage.rivulet.glob_path import GlobPath
from deltacat.storage.rivulet.dataset_executor import DatasetExecutor
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.mvp.Table import MvpTable
from deltacat.storage.rivulet.field_group import FieldGroup, FileSystemFieldGroup, GlobPathFieldGroup, PydictFieldGroup
from deltacat.storage.rivulet.logical_plan import LogicalPlan
from deltacat.storage.rivulet.reader.data_scan import DataScan
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat.storage.rivulet.reader.dataset_reader import DatasetReader
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.writer.dataset_writer import DatasetWriter
from deltacat.storage.rivulet.fs.file_location_provider import FileLocationProvider
from deltacat.storage.rivulet.writer.memtable_dataset_writer import MemtableDatasetWriter


class Dataset:
    __plan: LogicalPlan = None

    def __init__(self,
                 base_uri: str,
                 field_groups: List[FieldGroup] = None):
        self._file_store: FileStore = FileStore()

        # Set and maintain:
        # field_groups, _schema, _fieldToFieldGroup
        self.field_groups: List[FieldGroup] = []
        self._schema: Schema | None = None
        self._fieldToFieldGroup: Dict[str, FieldGroup] = {}
        self._location_provider = FileLocationProvider(base_uri, self._file_store)
        self._metastore: DatasetMetastore = DatasetMetastore(self._location_provider, self._file_store)
        self.__append_field_groups(field_groups or [])

    @classmethod
    def from_glob_path(cls,
                       base_uri: str,
                       glob_path: GlobPath,
                       schema: Schema):
        return cls(base_uri, field_groups=[GlobPathFieldGroup(glob_path, schema)])

    @classmethod
    def from_pydict(cls,
                    base_uri: str,
                    data: Dict[str, List[Any]],
                    primary_key: str):
        table = pa.Table.from_pydict(data)
        riv_schema = Schema.from_pyarrow_schema(table.schema, primary_key)
        return cls(base_uri, field_groups=[PydictFieldGroup(data, riv_schema)])

    def add_field_group(self, field_group: FieldGroup) -> None:
        """
        Add a new field group to the dataset
        """
        # Perform validations and update internal schema and mappings
        self.__append_field_groups([field_group])

    def new_field_group(self, schema: Schema) -> FieldGroup:
        """
        Add a new native field group to the dataset

        :param schema: schema to use for the new field group
        :return: new field group
        """
        field_group: FieldGroup = FileSystemFieldGroup(schema)
        self.__append_field_groups([field_group])
        return field_group

    def writer(self,
               field_groups: Union[FieldGroup, Iterable[FieldGroup]] = None,
               file_format: str | None = None,) -> DatasetWriter:
        """Create a new (stateful) writer using the schema at the conjunction of given field groups.

        Invoking this will register any unregistered field groups.

        :param field_groups: one or more field groups that form the schema of the data being written.
                             If None, uses the dataset-level schema
        :param file_format Write data to this format. Options are [parquet, feather]. If not specified, library will choose
            based on schema
        :return: new dataset writer with a schema at the conjunction of the given field groups
        """
        write_schema = self._schema
        if field_groups:
            if not isinstance(field_groups, Iterable):
                field_groups = [field_groups]
            if not field_groups:
                raise ValueError("Writer must contain one or more field groups or schemas")
            self.__append_field_groups(field_groups)
            schemas = [fg.schema for fg in field_groups]
            # merge the schema together
            write_schema = None
            for schema in schemas:
                if write_schema is None:
                    write_schema = Schema(schema.fields, schema.primary_key.name)
                    continue
                for field_name, field in schema.items():
                    if field_name == write_schema.primary_key.name:
                        continue
                    write_schema[field_name] = field
        return MemtableDatasetWriter(self._location_provider, write_schema, file_format)

    @property
    def _plan(self) -> LogicalPlan:
        """Lazily instantiate a plan on first usage with the dataset at that point in time."""
        if not self.__plan:
            self.__plan = LogicalPlan(self._schema)
        return self.__plan

    @property
    def schema(self) -> Schema:
        """
        Get the combined schema of all columns in the column groups.
        """
        if self._schema is None:
            raise AttributeError("Schema is not set")
        return self._schema

    def scan(self, query: QueryExpression = QueryExpression()) -> DataScan:
        dataset_reader = DatasetReader(self._metastore)
        return DataScan(self.schema, query, dataset_reader)

    def save(self, path: str) -> None:
        """
        Save the dataset to the specified path.

        Args:
            path: The path where the dataset should be saved.
        """

        # Save data from all field groups
        for field_group in self.field_groups:
            field_group.save(path)

    def select(self, fields: List[str]) -> "Dataset":
        """
        Enqueue a select operation on the dataset.

        Args:
            fields: A list of column names to select.
        """
        self._plan.select(fields)
        return self

    def collect(self) -> MvpTable:
        """
        Materialize the dataset and return the results.
        """
        self._plan.collect()
        return self._execute()

    ###
    # TODO make this more like a UDF interface
    #   Right now we can use PartiQL or similar syntax for computed fields
    #   And later add support for pluggable udfs
    #   Probably adding a computed field should go
    ###
    def add_computed_field(self, name, function):
        # (1) create new column group which is the computed function over the dataset
        # (2) enqueue computation to run function on dataset
        # TODO implement
        raise NotImplementedError()

    # Runs function on all data in dataset.
    #  Writes data to S3 or wherever they set up their catalog
    #  (default will be in memory catalog)
    # TODO how do we express which output columns get added as a result of function?
    # TODO how/where does user configure which catalog is used for dataset?
    def map(self, function) -> "Dataset":
        self._plan.map(function)
        return self

    ###
    # Methods to process dataframe data
    # Require materialization
    ###
    def show(self, n: int = 10) -> None:
        # (1) for each column group, perform some minimal IO to get data from it
        # (2) join data based on join key of each column group
        # (3) select n rows
        # TODO implement
        pass

    # Internal methods
    # This is a sort of janky visitor pattern. I think it's better than having the logical plan class call dataset._visit(operation) for
    #   each operation. In the future we could have a separate plan executor if we needed more complexity
    def _execute(self) -> MvpTable:
        executor = DatasetExecutor(self.field_groups, self.schema, self._metastore)
        return self._plan.execute(executor)

    def _commit(self, transaction, transaction_type):
        # Placeholder for actual commit logic
        pass

    def __append_field_groups(self, add_field_groups: List[FieldGroup]):
        """
        Validating added field groups and update self._schema and self._fieldToFieldGroup

        Only modifies private fields. self.field_groups maintained elsewhere

        Raises:
            ValueError: if any of the field groups do not contain the primary key.
            ValueError: if there are overlapping fields which would make it ambiguous which field group owns which schema field,
            OTHER THAN primary key
        """
        for field_group in add_field_groups:
            # If this is the first time initializing schema, initialize it to first field group schema
            if self._schema is None:
                self._schema = Schema(field_group.schema.fields, field_group.schema.primary_key.name)
                continue
            elif field_group in self.field_groups:
                continue
            if field_group.schema.primary_key != self._schema.primary_key:
                raise ValueError(
                    f"Field group '{field_group}' must use dataset's primary key of '{self._schema.primary_key}'")

            for field_name, field in field_group.schema.items():
                if field_name == self._schema.primary_key.name:
                    continue
                if field_name in self._schema:
                    raise ValueError(f"Ambiguous field '{field_name}' present in multiple field groups")
                self._schema[field_name] = field
                self._fieldToFieldGroup[field_name] = field_group

        for field_group in add_field_groups:
            if field_group in self.field_groups:
                continue
            self.field_groups.append(field_group)
