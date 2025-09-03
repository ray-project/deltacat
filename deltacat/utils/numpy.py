from typing import List, Optional, Callable, Union, Dict, Any

import pandas as pd
import numpy as np
from fsspec import AbstractFileSystem
import pyarrow.fs as pafs
import logging

from ray.data.datasource import FilenameProvider
from deltacat.types.media import ContentType, ContentEncoding
from deltacat.utils import pandas as pd_utils

from deltacat.utils.common import ReadKwargsProvider
from deltacat import logs
from deltacat.utils.performance import timed_invocation
from deltacat.types.partial_download import PartialFileDownloadParams

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def slice_ndarray(np_array: np.ndarray, max_len: Optional[int]) -> List[np.ndarray]:
    """
    Iteratively creates max_len slices from the first dimension of an ndarray.
    """
    if max_len is None:
        return [np_array]

    # Slice along the first dimension of the ndarray.
    return [np_array[i : i + max_len] for i in range(0, len(np_array), max_len)]


def file_to_ndarray(
    path: str,
    content_type: str,
    content_encoding: str = ContentEncoding.IDENTITY.value,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pd_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    fs_open_kwargs: Dict[str, Any] = {},
    **kwargs,
) -> np.ndarray:
    """
    Read a file into a NumPy ndarray using any filesystem.

    This function delegates to the pandas file_to_dataframe function and converts
    the resulting DataFrame to a NumPy ndarray.

    Args:
        path: The file path to read
        content_type: The content type of the file (e.g., ContentType.CSV.value)
        content_encoding: The content encoding (default: IDENTITY)
        filesystem: The filesystem to use (if None, will be inferred from path)
        column_names: Optional column names to assign
        include_columns: Optional columns to include in the result
        pd_read_func_kwargs_provider: Optional kwargs provider for customization
        fs_open_kwargs: Optional kwargs for filesystem open operations
        **kwargs: Additional kwargs passed to the reader function

    Returns:
        np.ndarray: The loaded data as a NumPy ndarray
    """
    logger.debug(
        f"Reading {path} to NumPy ndarray. Content type: {content_type}. "
        f"Encoding: {content_encoding}"
    )

    dataframe, latency = timed_invocation(
        pd_utils.file_to_dataframe,
        path=path,
        content_type=content_type,
        content_encoding=content_encoding,
        filesystem=filesystem,
        column_names=column_names,
        include_columns=include_columns,
        pd_read_func_kwargs_provider=pd_read_func_kwargs_provider,
        partial_file_download_params=partial_file_download_params,
        fs_open_kwargs=fs_open_kwargs,
        **kwargs,
    )

    ndarray, conversion_latency = timed_invocation(dataframe.to_numpy)
    total_latency = latency + conversion_latency
    logger.debug(f"Time to read {path} into NumPy ndarray: {total_latency}s")
    return ndarray


def ndarray_size(np_array: np.ndarray) -> int:
    return np_array.nbytes


def ndarray_to_file(
    np_array: np.ndarray,
    path: str,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[FilenameProvider, Callable],
    content_type: str = ContentType.PARQUET.value,
    **kwargs,
) -> None:
    """
    Writes the given Numpy ndarray to a file.
    """
    import pyarrow as pa

    # Extract schema from kwargs if available
    schema = kwargs.pop("schema", None)

    # Convert to pandas DataFrame with proper column names if schema is available
    if schema and isinstance(schema, pa.Schema):
        if np_array.ndim == 1:
            # 1D array: single column
            column_names = [schema.names[0]] if schema.names else ["0"]
            df = pd.DataFrame({column_names[0]: np_array})
        elif np_array.ndim == 2:
            # 2D array: multiple columns
            column_names = (
                schema.names
                if len(schema.names) == np_array.shape[1]
                else [f"{i}" for i in range(np_array.shape[1])]
            )
            df = pd.DataFrame(np_array, columns=column_names)
        else:
            raise ValueError(
                f"NumPy arrays with {np_array.ndim} dimensions are not supported"
            )
    else:
        # Fallback to generic column names
        df = pd.DataFrame(np_array)

    pd_utils.dataframe_to_file(
        df,
        path,
        filesystem,
        block_path_provider,
        content_type,
        **kwargs,
    )


def concat_ndarrays(arrays: List[np.ndarray]) -> Optional[np.ndarray]:
    """
    Concatenate a list of NumPy ndarrays into a single ndarray.

    Args:
        arrays: List of NumPy ndarrays to concatenate

    Returns:
        Concatenated NumPy ndarray, or None if input is empty
    """
    if arrays is None or not len(arrays):
        return None
    if len(arrays) == 1:
        return next(iter(arrays))
    return np.concatenate(arrays, axis=0)


def append_column_to_ndarray(
    np_array: np.ndarray,
    column_name: str,
    column_value: Any,
) -> np.ndarray:
    # Add a new column with value repeating for each row of np_array
    return np.concatenate((np_array, np.full((len(np_array), 1), column_value)), axis=1)
