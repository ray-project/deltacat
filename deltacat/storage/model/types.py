import pyarrow as pa
import pandas as pd
import numpy as np

from enum import Enum

from ray.data.dataset import Dataset
from ray.data.impl.arrow_block import ArrowRow

from typing import Any, List, Union

LocalTable = Union[pa.Table, pd.DataFrame, np.ndarray]
LocalDataset = List[LocalTable]
DistributedDataset = Dataset[Union[ArrowRow, np.ndarray, Any]]


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
    Schemas are optional for DeltaCAT tables and can be used to inform the data
    consistency checks run for each field. If a schema is present, it can be
    used to enforce the following column-level data consistency policies at
    table load time:

    NONE: No consistency checks are run. May be mixed with the below two
    policies by specifying column names to pass through together with
    column names to coerce/validate.

    COERCE: Coerce fields to fit the schema whenever possible. An explicit
    subset of column names to coerce may optionally be specified.

    VALIDATE: Raise an error for any fields that don't fit the schema. An
    explicit subset of column names to validate may optionally be specified.
    """
    NONE = "none"
    COERCE = "coerce"
    VALIDATE = "validate"
