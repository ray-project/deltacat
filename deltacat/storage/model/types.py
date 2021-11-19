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
    NONE = "none"
    COERCE = "coerce"
    VALIDATE = "validate"
