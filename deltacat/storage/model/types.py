from __future__ import annotations

from enum import Enum
from typing import List, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from ray.data.dataset import Dataset
from daft import DataFrame as DaftDataFrame


LocalTable = Union[
    pa.Table,
    pd.DataFrame,
    np.ndarray,
    pa.parquet.ParquetFile,
]
LocalDataset = List[LocalTable]
DistributedDataset = Union[Dataset, DaftDataFrame]


class StreamFormat(str, Enum):
    DELTACAT = "deltacat"
    ICEBERG = "iceberg"
    HUDI = "hudi"
    DELTA_LAKE = "delta_lake"
    SQLITE3 = "SQLITE3"  # used by tests


class DeltaType(str, Enum):
    APPEND = "append"
    UPSERT = "upsert"
    DELETE = "delete"


class TransactionType(str, Enum):
    # the transaction reads existing data
    # does not conflict with any other transaction types
    READ = "read"
    # the transaction only appends new data
    # conflicts with other transaction types can be auto-resolved
    APPEND = "append"
    # the transaction alters existing data
    # (even if it also appends data)
    # conflicts with other alters/overwrites/restates/deletes fail
    ALTER = "alter"
    # the transaction overwrites existing data
    # (even if it also appends or alters data)
    # conflicts with other alters/overwrites/restates/deletes fail
    OVERWRITE = "overwrite"
    # the transaction restates existing data with a new layout
    # (even if it appends, alters, or overwrites data to do so)
    # conflicts with other alters/overwrites/restates/deletes fail
    RESTATE = "restate"
    # the transaction deletes existing data
    # (even if it also appends, alters, overwrites, or restates data)
    # conflicts with other alters/overwrites/restates/deletes fail
    DELETE = "delete"


class TransactionOperationType(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"

    READ_SIBLINGS = "read_siblings"
    READ_CHILDREN = "read_children"
    READ_LATEST = "read_latest"
    READ_EXISTS = "read_exists"

    @staticmethod
    def write_operations():
        return {
            TransactionOperationType.CREATE,
            TransactionOperationType.UPDATE,
            TransactionOperationType.DELETE,
        }

    @staticmethod
    def read_operations():
        return {
            TransactionOperationType.READ_SIBLINGS,
            TransactionOperationType.READ_CHILDREN,
            TransactionOperationType.READ_LATEST,
            TransactionOperationType.READ_EXISTS,
        }

    def is_write_operation(self) -> bool:
        return self in TransactionOperationType.write_operations()

    def is_read_operation(self) -> bool:
        return self in TransactionOperationType.read_operatins()


class LifecycleState(str, Enum):
    CREATED = "created"
    UNRELEASED = "unreleased"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    BETA = "beta"
    DELETED = "deleted"


class CommitState(str, Enum):
    STAGED = "staged"
    COMMITTED = "committed"
    DEPRECATED = "deprecated"


class SchemaConsistencyType(str, Enum):
    """
    DeltaCAT table schemas can be used to inform the data consistency checks
    run for each field. When present, the schema can be used to enforce the
    following field-level data consistency policies at table load time:

    NONE: No consistency checks are run.

    COERCE: Coerce fields to fit the schema whenever possible.

    VALIDATE: Raise an error for any fields that don't fit the schema.
    """

    NONE = "none"
    COERCE = "coerce"
    VALIDATE = "validate"


class SortOrder(str, Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"

    @classmethod
    def _missing_(cls, value: str):
        # pyiceberg.table.sorting.SortDirection mappings
        if value.lower() == "asc":
            return SortOrder.ASCENDING
        elif value.lower() == "desc":
            return SortOrder.DESCENDING
        return None


class NullOrder(str, Enum):
    AT_START = "at_start"
    AT_END = "at_end"

    @classmethod
    def _missing_(cls, value: str):
        # pyiceberg.table.sorting.NullOrder mappings
        if value.lower() == "nulls-first":
            return NullOrder.AT_START
        elif value.lower() == "nulls-last":
            return NullOrder.AT_END
        return None
