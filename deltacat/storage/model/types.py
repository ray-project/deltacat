from __future__ import annotations

from enum import Enum
from typing import List, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import polars as pl
from ray.data.dataset import Dataset as RayDataset
from daft import DataFrame as DaftDataFrame

from deltacat.constants import (
    RUNNING_TXN_DIR_NAME,
    PAUSED_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
    SUCCESS_TXN_DIR_NAME,
)

LocalTable = Union[
    pa.Table,
    pd.DataFrame,
    pl.DataFrame,
    np.ndarray,
    pa.parquet.ParquetFile,
]
LocalDataset = Union[LocalTable, List[LocalTable]]
DistributedDataset = Union[RayDataset, DaftDataFrame]
Dataset = Union[LocalDataset, DistributedDataset]


class StreamFormat(str, Enum):
    DELTACAT = "deltacat"
    ICEBERG = "iceberg"
    HIVE = "hive"
    HUDI = "hudi"
    DELTA_LAKE = "delta_lake"
    SQLITE3 = "SQLITE3"  # used by tests


class DeltaType(str, Enum):
    ADD = "add"
    APPEND = "append"
    UPSERT = "upsert"
    DELETE = "delete"


class TransactionOperationType(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    REPLACE = "replace"
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
            TransactionOperationType.REPLACE,
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
        return self in TransactionOperationType.read_operations()


class TransactionStatus(str, Enum):
    """
    Transaction user status types. Every transaction status maps to a distinct
    transaction log directory.
    """

    SUCCESS = "SUCCESS"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    FAILED = "FAILED"

    def dir_name(self) -> str:
        if self == TransactionStatus.RUNNING:
            return RUNNING_TXN_DIR_NAME
        elif self == TransactionStatus.PAUSED:
            return PAUSED_TXN_DIR_NAME
        elif self == TransactionStatus.FAILED:
            return FAILED_TXN_DIR_NAME
        elif self == TransactionStatus.SUCCESS:
            return SUCCESS_TXN_DIR_NAME


class TransactionState(str, Enum):
    """
    Transaction system state types. Transaction states do not map to distinct transaction log directories,
    but can be inferred by its presence in one or more directories. These states are used to infer whether
    to run system activities like transaction cleanup jobs.
    """

    FAILED = "FAILED"
    PURGED = "PURGED"
    TIMEOUT = "TIMEOUT"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PAUSED = "PAUSED"


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
