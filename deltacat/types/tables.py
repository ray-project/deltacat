from enum import Enum
from typing import Dict, Callable, Type, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import deltacat.storage as dcs

from ray.data.dataset import Dataset
from ray.data.read_api import from_arrow_refs, from_pandas_refs, from_numpy

from deltacat.types.media import TableType
from deltacat.utils import pyarrow as pa_utils, pandas as pd_utils, \
    numpy as np_utils
from deltacat.utils.ray_utils import dataset as ds_utils

TABLE_TYPE_TO_READER_FUNC: Dict[int, Callable] = {
    TableType.PYARROW.value: pa_utils.s3_file_to_table,
    TableType.PANDAS.value: pd_utils.s3_file_to_dataframe,
    TableType.NUMPY.value: np_utils.s3_file_to_ndarray
}

TABLE_CLASS_TO_WRITER_FUNC: Dict[
    Type[Union[dcs.LocalTable, dcs.DistributedDataset]], Callable] = {
    pa.Table: pa_utils.table_to_file,
    pd.DataFrame: pd_utils.dataframe_to_file,
    np.ndarray: np_utils.ndarray_to_file,
    Dataset: ds_utils.dataset_to_file,
}

TABLE_CLASS_TO_SLICER_FUNC: Dict[
    Type[Union[dcs.LocalTable, dcs.DistributedDataset]], Callable] = {
    pa.Table: pa_utils.slice_table,
    pd.DataFrame: pd_utils.slice_dataframe,
    np.ndarray: np_utils.slice_ndarray,
    Dataset: ds_utils.slice_dataset,
}

TABLE_CLASS_TO_SIZE_FUNC: Dict[
    Type[Union[dcs.LocalTable, dcs.DistributedDataset]], Callable] = {
    pa.Table: pa_utils.table_size,
    pd.DataFrame: pd_utils.dataframe_size,
    np.ndarray: np_utils.ndarray_size,
    Dataset: ds_utils.dataset_size,
}

TABLE_CLASS_TO_TABLE_TYPE: Dict[dcs.LocalTable, str] = {
    pa.Table: TableType.PYARROW.value,
    pd.DataFrame: TableType.PANDAS.value,
    np.ndarray: TableType.NUMPY.value,
}

TABLE_TYPE_TO_DATASET_CREATE_FUNC: Dict[str, Callable] = {
    TableType.PYARROW.value: from_arrow_refs,
    TableType.NUMPY.value: from_numpy,
    TableType.PANDAS.value: from_pandas_refs,
}


class TableWriteMode(str, Enum):
    """
    Enum controlling how a given dataset will be written to a table.

    AUTO: CREATE if the table doesn't exist, APPEND if the table exists
    without primary keys, and MERGE if the table exists with primary keys.
    CREATE: Create the table if it doesn't exist, throw an error if it does.
    APPEND: Append to the table if it exists, throw an error if it doesn't.
    REPLACE: Replace existing table contents with the data to write.
    MERGE: Insert, update, or delete records matching a given predicate.
    Updates or inserts records based on the table's primary and sort keys by
    default.
    """
    AUTO = "auto"
    CREATE = "create"
    APPEND = "append"
    REPLACE = "replace"
    MERGE = "merge"


def get_table_length(table: Union[dcs.LocalTable, dcs.DistributedDataset]) \
        -> int:
    return len(table) if not isinstance(table, Dataset) else table.count()


def get_table_writer(table: Union[dcs.LocalTable, dcs.DistributedDataset]) \
        -> Callable:
    table_writer_func = TABLE_CLASS_TO_WRITER_FUNC.get(type(table))
    if table_writer_func is None:
        msg = f"No writer found for table type: {type(table)}.\n" \
              f"Known table types: {TABLE_CLASS_TO_WRITER_FUNC.keys}"
        raise ValueError(msg)
    return table_writer_func


def get_table_slicer(table: Union[dcs.LocalTable, dcs.DistributedDataset]) \
        -> Callable:
    table_slicer_func = TABLE_CLASS_TO_SLICER_FUNC.get(type(table))
    if table_slicer_func is None:
        msg = f"No slicer found for table type: {type(table)}.\n" \
              f"Known table types: {TABLE_CLASS_TO_SLICER_FUNC.keys}"
        raise ValueError(msg)
    return table_slicer_func
