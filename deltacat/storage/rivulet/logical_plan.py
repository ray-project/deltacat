from dataclasses import dataclass
from typing import List, Callable, Any, Protocol

from deltacat.storage.rivulet.dataset_executor import DatasetExecutor
from deltacat.storage.rivulet.mvp.Table import MvpTable
from deltacat.storage.rivulet import Schema


class DatasetOperation(Protocol):
    def visit(self, executor: DatasetExecutor):
        ...


@dataclass
class SelectOperation(DatasetOperation):
    """
    Select a subset of fields within the schema

    TODO need better interface for defining selection
            (e.g. "all fields except X")

    TODO in the future this should support basic filters (e.g. on primary key)
    """

    fields: List[str]

    def visit(self, executor: DatasetExecutor):
        executor.select(self.fields)


@dataclass
class MapOperation(DatasetOperation):
    """
    Map a function over each record in the dataset

    TODO need more sophistication in the interface of the callable function
    For now we will be super simple and just call the transform on each record
    """

    transform: Callable[[Any], Any]

    def visit(self, executor: DatasetExecutor):
        executor.map(self.transform)


class CollectOperation(DatasetOperation):
    """
    Materialize dataset
    """

    def visit(self, executor: DatasetExecutor):
        executor.collect()


class LogicalPlan:
    """
    A fluent builder for constructing a sequence of dataset operations.

    This class allows chaining of different dataset operations such as select and map.
    The actual implementation of these operations is delegated to the Dataset class
    using the visitor pattern.

    Example:
        plan = LogicalPlan().select(lambda x: x['age'] > 30).map(lambda x: x['name'])

    The plan can then be executed on a Dataset object, which will apply the
    operations in the order they were added.
    """

    def __init__(self, schema: Schema):
        self.operations: List[DatasetOperation] = []
        self.schema = schema
        # Tracks effective schema to perform each operation on
        self.effective_schema: Schema = schema.__deepcopy__()

    def select(self, filter: List[str]) -> "LogicalPlan":
        # Validate that select statement is allowed and mutate effective schema for future validations
        invalid_fields = [
            field for field in filter if field not in self.effective_schema.fields
        ]
        if invalid_fields:
            raise ValueError(f"Invalid fields: {', '.join(invalid_fields)}")

        # remove fields from effective schema if they are not in chosen fields
        remove_fields = [
            field for field in self.effective_schema.keys() if field not in filter
        ]
        for field in remove_fields:
            self.effective_schema.__delitem__(field)

        self.operations.append(SelectOperation(filter))
        return self

    def map(self, transform: Callable[[dict], dict]) -> "LogicalPlan":
        self.operations.append(MapOperation(transform))
        return self

    def collect(self) -> "LogicalPlan":
        self.operations.append(CollectOperation())
        return self

    def execute(self, executor: DatasetExecutor) -> "MvpTable":
        for operation in self.operations:
            operation.visit(executor)
        return executor.output
