from typing import Any, List, Optional

import numpy as np
import pyarrow as pa
from fsspec import AbstractFileSystem
from ray.data.datasource import BlockWritePathProvider

from deltacat.types.media import ContentType
from deltacat.utils import pandas as pd_utils
from deltacat.utils import pyarrow as pa_utils
from deltacat.utils.common import ReadKwargsProvider


def searchsorted_by_attr(
    attribute: str, obj_arr: List[Any], values_to_insert: List[Any], side: str = "right"
) -> List[int]:
    """
    Find the insertion indices for 'values_to_insert' in a sorted array of objects based on a specified attribute.
    This function takes an attribute name, a list of objects, and a list of values to insert.
    It finds the appropriate insertion indices for the given values based on the order of the attribute in obj_arr using numpy.searchsorted.

    Args:
        attribute (str): The name of the attribute to bisect search on. The obj arr should be sorted based on that attribute
        obj_arr (List[Any]): A list of objects to bisect search.
        values_to_insert (List[Any]): A list of values to find the insertion indices for.
        side (str, optional): The side to use for the insertion indices.
            If 'left', the indices are biased towards the left (lower values).
            If 'right', the indices are biased towards the right (higher values).
            Defaults to 'right'.

    Returns:
        List[int]: A list of insertion indices for each value in 'values_to_insert'.

    Raises:
        AssertionError: If the `side` argument is not 'left' or 'right'.
        AttributeError: If the attribute does not exist in the obj_arr

    Example:

        >>> class Point:
        ...     def __init__(self, x, y):
        ...         self.x = x
        ...         self.y = y
        ...
        >>> points = [Point(1, 2), Point(3, 4), Point(5, 6)]
        >>> searchsorted_by_attr('x', points, [0, 2, 4, 6])
        [0, 1, 2, 3]
        >>> searchsorted_by_attr('x', points, [0, 2, 4, 6], side='left')
        [0, 0, 1, 3]
    """
    assert side in (
        "left",
        "right",
    ), "side argument should be either 'left' to use bisect_left or 'right' to use bisect_right"
    return np.searchsorted(
        [getattr(input, attribute) for input in obj_arr], values_to_insert, side
    )


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
    block_path_provider: BlockWritePathProvider,
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
