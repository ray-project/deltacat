from __future__ import annotations

from typing import List, Callable, Any

from deltacat.storage.rivulet.field_group import FieldGroup
from deltacat.storage.rivulet.mvp.Table import MvpTable
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.reader.data_scan import DataScan
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat.storage.rivulet.reader.dataset_reader import DatasetReader
from deltacat.storage.rivulet.reader.query_expression import QueryExpression


class DatasetExecutor:
    """
    Executor class which runs operations such as select, map, take, save

    This class may store intermediate state while it is executing.

    LogicalPlan is responsible for constructor an executor and ordering operations appropriately
    """

    def __init__(
        self,
        field_groups: List[FieldGroup],
        schema: Schema,
        metastore: DatasetMetastore,
    ):
        self.effective_schema: Schema = schema.__deepcopy__()
        self.field_groups = field_groups
        self.output: MvpTable | None = None
        self._metastore = metastore

    def collect(self) -> MvpTable:
        if not self.output:
            self.output = self._read(self.effective_schema)
        return self.output

    def select(self, fields: List[str]) -> "DatasetExecutor":
        """
        Reads data and selects a subset of fields
        Note that this implementation is super inefficient (does not push down filters to read, copies data to new MvpTable). That is OK since this will all be replaced
        """
        # Read data from original input sources if not already read
        if not self.output:
            self.output = self._read(self.effective_schema)
        # Calculate effective schema and apply it to data
        self.effective_schema.filter(fields)
        self.output = MvpTable(
            {
                key: value
                for key, value in self.output.data.items()
                if key in self.effective_schema
            },
        )
        return self

    def map(self, transform: Callable[[Any], Any]) -> "DatasetExecutor":
        raise NotImplementedError

    def _read(self, schema: Schema) -> MvpTable:
        """
        Internal helper method to read data

        TODO for now this is doing dumb in-memory implementation and later this is going to be replaced by rust library
        """
        if len(self.field_groups) == 1:
            return self._read_as_mvp_table(schema, self.field_groups[0])
        else:
            ds1 = self._read_as_mvp_table(schema, self.field_groups[0])
            ds2 = self._read_as_mvp_table(schema, self.field_groups[1])
            merged = MvpTable.merge(ds1, ds2, schema.primary_key.name)
            for i in range(2, len(self.field_groups)):
                ds_i = self._read_as_mvp_table(schema, self.field_groups[i])
                merged = MvpTable.merge(merged, ds_i, schema.primary_key.name)
            return merged

    def _read_as_mvp_table(self, schema: Schema, field_group: FieldGroup):
        data = list(
            DataScan(
                schema, QueryExpression(), DatasetReader(self._metastore)
            ).to_pydict()
        )
        output = {}
        for key in schema.fields.keys():
            output[key] = [d.get(key) for d in data]
        return MvpTable(output)
