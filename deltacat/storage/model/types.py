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


class LifecycleState(str, Enum):
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
