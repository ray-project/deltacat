from typing import List, Optional, Callable, Union

import numpy as np
import pyarrow as pa
from fsspec import AbstractFileSystem

from ray.data.datasource import FilenameProvider
from deltacat.types.media import ContentType
from deltacat.utils import pandas as pd_utils
from deltacat.utils import pyarrow as pa_utils
from deltacat.utils.common import ReadKwargsProvider


def slice_ndarray(np_array: np.ndarray, max_len: Optional[int]) -> List[np.ndarray]:
    """
    Iteratively creates max_len slices from the first dimension of an ndarray.
    """
    if max_len is None:
        return [np_array]

    # Slice along the first dimension of the ndarray.
    return [np_array[i : i + max_len] for i in range(0, len(np_array), max_len)]


def s3_file_to_ndarray(
    s3_url: str,
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pd_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **s3_client_kwargs
) -> np.ndarray:
    # TODO: Compare perf to s3 -> pyarrow -> pandas [Series/DataFrame] -> numpy
    dataframe = pd_utils.s3_file_to_dataframe(
        s3_url,
        content_type,
        content_encoding,
        column_names,
        include_columns,
        pd_read_func_kwargs_provider,
        **s3_client_kwargs
    )
    return dataframe.to_numpy()


def ndarray_size(np_array: np.ndarray) -> int:
    return np_array.nbytes


def ndarray_to_file(
    np_array: np.ndarray,
    path: str,
    file_system: AbstractFileSystem,
    block_path_provider: Union[FilenameProvider, Callable],
    content_type: str = ContentType.PARQUET.value,
    **kwargs
) -> None:
    """
    Writes the given Numpy ndarray to a file.
    """

    # PyArrow only supports 1D ndarrays, so convert to list of 1D arrays
    np_arrays = [array for array in np_array]
    pa_utils.table_to_file(
        pa.table({"data": np_arrays}),
        path,
        file_system,
        block_path_provider,
        content_type,
        **kwargs
    )
