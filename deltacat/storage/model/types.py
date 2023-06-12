from enum import Enum
from typing import List, Union, Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pkg_resources
from ray.data._internal.arrow_block import ArrowRow
from ray.data.dataset import Dataset

LocalTable = Union[pa.Table, pd.DataFrame, np.ndarray]
LocalDataset = List[LocalTable]
# Starting Ray 2.5.0, Dataset follows a strict mode (https://docs.ray.io/en/latest/data/faq.html#migrating-to-strict-mode),
# and generic annotation is removed. So add a version checker to determine whether to use the old or new definition.
ray_version = pkg_resources.parse_version(pkg_resources.get_distribution("ray").version)
change_version = pkg_resources.parse_version("2.5.0")
if ray_version < change_version:
    DistributedDataset = Dataset[Union[ArrowRow, np.ndarray, Any]]
else:
    DistributedDataset = Dataset


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
