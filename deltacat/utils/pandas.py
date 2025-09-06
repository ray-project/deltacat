import csv
import logging
import math
import posixpath
import bz2
import gzip
from functools import partial
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
from fsspec import AbstractFileSystem
from ray.data.datasource import FilenameProvider

from deltacat import logs
from deltacat.types.media import (
    DELIMITED_TEXT_CONTENT_TYPES,
    TABULAR_CONTENT_TYPES,
    ContentEncoding,
    ContentType,
)
from deltacat.utils.common import ContentTypeKwargsProvider, ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.filesystem import resolve_path_and_filesystem
from deltacat.types.partial_download import PartialFileDownloadParams

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

# Encoding to file initialization function mapping
ENCODING_TO_FILE_INIT: Dict[str, Callable] = {
    ContentEncoding.GZIP.value: partial(gzip.open, mode="rb"),
    ContentEncoding.BZIP2.value: partial(bz2.open, mode="rb"),
    ContentEncoding.IDENTITY.value: lambda file_path: file_path,
}


def read_csv(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pd.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
            # Handle compression with smart detection for PyArrow auto-decompression
            if content_encoding in [
                ContentEncoding.GZIP.value,
                ContentEncoding.BZIP2.value,
            ]:
                try:
                    # First try to read as if already decompressed by PyArrow
                    return pd.read_csv(f, **read_kwargs)
                except (
                    gzip.BadGzipFile,
                    OSError,
                    UnicodeDecodeError,
                    pd.errors.EmptyDataError,
                    Exception,
                ):
                    # If that fails, we need to reopen the file since the stream may be closed/corrupted
                    pass

                # Reopen and try manual decompression
                with filesystem.open_input_stream(path, **fs_open_kwargs) as f_retry:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f_retry) as input_file:
                        return pd.read_csv(input_file, **read_kwargs)
            else:
                return pd.read_csv(f, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return pd.read_csv(input_file, **read_kwargs)


def read_parquet(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pd.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression with smart detection for PyArrow auto-decompression
            if content_encoding in [
                ContentEncoding.GZIP.value,
                ContentEncoding.BZIP2.value,
            ]:
                try:
                    # First try to read as if already decompressed by PyArrow
                    return pd.read_parquet(f, **read_kwargs)
                except (gzip.BadGzipFile, OSError, pa.ArrowInvalid, Exception):
                    # If that fails, we need to reopen the file
                    pass

                # Reopen and try manual decompression
                with filesystem.open_input_file(path, **fs_open_kwargs) as f_retry:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f_retry) as input_file:
                        return pd.read_parquet(input_file, **read_kwargs)
            else:
                return pd.read_parquet(f, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return pd.read_parquet(input_file, **read_kwargs)


def read_feather(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pd.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression with smart detection for PyArrow auto-decompression
            if content_encoding in [
                ContentEncoding.GZIP.value,
                ContentEncoding.BZIP2.value,
            ]:
                try:
                    # First try to read as if already decompressed by PyArrow
                    return pd.read_feather(f, **read_kwargs)
                except (gzip.BadGzipFile, OSError, pa.ArrowInvalid, Exception):
                    # If that fails, we need to reopen the file
                    pass

                # Reopen and try manual decompression
                with filesystem.open_input_file(path, **fs_open_kwargs) as f_retry:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f_retry) as input_file:
                        return pd.read_feather(input_file, **read_kwargs)
            else:
                return pd.read_feather(f, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return pd.read_feather(input_file, **read_kwargs)


def read_orc(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pd.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression with smart detection for PyArrow auto-decompression
            if content_encoding in [
                ContentEncoding.GZIP.value,
                ContentEncoding.BZIP2.value,
            ]:
                try:
                    # First try to read as if already decompressed by PyArrow
                    return pd.read_orc(f, **read_kwargs)
                except (gzip.BadGzipFile, OSError, pa.ArrowInvalid, Exception):
                    # If that fails, we need to reopen the file
                    pass

                # Reopen and try manual decompression
                with filesystem.open_input_file(path, **fs_open_kwargs) as f_retry:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f_retry) as input_file:
                        return pd.read_orc(input_file, **read_kwargs)
            else:
                return pd.read_orc(f, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return pd.read_orc(input_file, **read_kwargs)


def read_json(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pd.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
            # Handle compression with smart detection for PyArrow auto-decompression
            if content_encoding in [
                ContentEncoding.GZIP.value,
                ContentEncoding.BZIP2.value,
            ]:
                try:
                    # First try to read as if already decompressed by PyArrow
                    return pd.read_json(f, **read_kwargs)
                except (
                    gzip.BadGzipFile,
                    OSError,
                    UnicodeDecodeError,
                    ValueError,
                    Exception,
                ):
                    # If that fails, we need to reopen the file
                    pass

                # Reopen and try manual decompression
                with filesystem.open_input_stream(path, **fs_open_kwargs) as f_retry:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f_retry) as input_file:
                        return pd.read_json(input_file, **read_kwargs)
            else:
                return pd.read_json(f, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return pd.read_json(input_file, **read_kwargs)


def read_avro(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pd.DataFrame:
    """
    Read an Avro file using polars and convert to pandas.
    """
    import polars as pl

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression with smart detection for PyArrow auto-decompression
            if content_encoding in [
                ContentEncoding.GZIP.value,
                ContentEncoding.BZIP2.value,
            ]:
                try:
                    # First try to read as if already decompressed by PyArrow
                    pl_df = pl.read_avro(f, **read_kwargs)
                    return pl_df.to_pandas()
                except (gzip.BadGzipFile, OSError, Exception):
                    # If that fails, we need to reopen the file
                    pass

                # Reopen and try manual decompression
                with filesystem.open_input_file(path, **fs_open_kwargs) as f_retry:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f_retry) as input_file:
                        pl_df = pl.read_avro(input_file, **read_kwargs)
                        return pl_df.to_pandas()
            else:
                pl_df = pl.read_avro(f, **read_kwargs)
                return pl_df.to_pandas()
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                pl_df = pl.read_avro(input_file, **read_kwargs)
                return pl_df.to_pandas()


CONTENT_TYPE_TO_PD_READ_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: pd.read_csv,
    ContentType.TSV.value: pd.read_csv,
    ContentType.CSV.value: pd.read_csv,
    ContentType.PSV.value: pd.read_csv,
    ContentType.PARQUET.value: pd.read_parquet,
    ContentType.FEATHER.value: pd.read_feather,
    ContentType.ORC.value: pd.read_orc,
    ContentType.JSON.value: pd.read_json,
    ContentType.AVRO.value: read_avro,
}


# New mapping for encoding-aware reader functions used by file_to_dataframe
CONTENT_TYPE_TO_READ_FN: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: read_csv,
    ContentType.TSV.value: read_csv,
    ContentType.CSV.value: read_csv,
    ContentType.PSV.value: read_csv,
    ContentType.PARQUET.value: read_parquet,
    ContentType.FEATHER.value: read_feather,
    ContentType.ORC.value: read_orc,
    ContentType.JSON.value: read_json,
    ContentType.AVRO.value: read_avro,
}


class ReadKwargsProviderPandasCsvPureUtf8(ContentTypeKwargsProvider):
    """ReadKwargsProvider impl that reads columns of delimited text files
    as UTF-8 strings (i.e. disables type inference). Useful for ensuring
    lossless reads of UTF-8 delimited text datasets and improving read
    performance in cases where type casting is not required."""

    def __init__(self, include_columns: Optional[Iterable[str]] = None):
        self.include_columns = include_columns

    def _get_kwargs(self, content_type: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if content_type in DELIMITED_TEXT_CONTENT_TYPES:
            include_columns = (
                self.include_columns if self.include_columns else kwargs.get("usecols")
            )
            if not include_columns:
                # read all columns as strings
                kwargs["dtype"] = str
            else:
                # read only the included columns as strings
                kwargs["dtype"] = {column_name: str for column_name in include_columns}
            # use the fastest available engine for pure utf-8 reads
            kwargs["engine"] = "pyarrow"
        return kwargs


def content_type_to_reader_kwargs(content_type: str) -> Dict[str, Any]:
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "sep": "\t",
            "header": None,
            "na_values": [""],
            "keep_default_na": False,
            "quoting": csv.QUOTE_NONE,
        }
    if content_type == ContentType.TSV.value:
        return {"sep": "\t", "header": None}
    if content_type == ContentType.CSV.value:
        return {"sep": ",", "header": None}
    if content_type == ContentType.PSV.value:
        return {"sep": "|", "header": None}
    if content_type == ContentType.JSON.value:
        return {"lines": True}  # Support NDJSON format
    if content_type in {
        ContentType.PARQUET.value,
        ContentType.FEATHER.value,
        ContentType.ORC.value,
        ContentType.AVRO.value,
    }:
        return {}
    raise ValueError(f"Unsupported content type: {content_type}")


ENCODING_TO_PD_COMPRESSION: Dict[str, str] = {
    ContentEncoding.GZIP.value: "gzip",
    ContentEncoding.BZIP2.value: "bz2",
    ContentEncoding.IDENTITY.value: "none",
}


def slice_dataframe(
    dataframe: pd.DataFrame,
    max_len: Optional[int],
) -> List[pd.DataFrame]:
    """
    Iteratively create dataframe slices.
    """
    if max_len is None:
        return [dataframe]
    dataframes = []
    num_slices = math.ceil(len(dataframe) / max_len)
    for i in range(num_slices):
        dataframes.append(dataframe[i * max_len : (i + 1) * max_len])
    return dataframes


def concat_dataframes(
    dataframes: List[pd.DataFrame],
    axis: int = 0,
    copy: bool = False,
    ignore_index: bool = True,
    **kwargs,
) -> Optional[pd.DataFrame]:
    if dataframes is None or not len(dataframes):
        return None
    if len(dataframes) == 1:
        return next(iter(dataframes))
    return pd.concat(dataframes, axis=axis, copy=copy, ignore_index=ignore_index)


def append_column_to_dataframe(
    dataframe: pd.DataFrame,
    column_name: str,
    column_value: Any,
) -> pd.DataFrame:
    dataframe[column_name] = column_value
    return dataframe


def select_columns(
    dataframe: pd.DataFrame,
    column_names: List[str],
) -> pd.DataFrame:
    return dataframe[column_names]


def _add_column_kwargs(
    content_type: str,
    column_names: Optional[List[str]],
    include_columns: Optional[List[str]],
    kwargs: Dict[str, Any],
):

    if content_type in DELIMITED_TEXT_CONTENT_TYPES:
        kwargs["names"] = column_names
        kwargs["usecols"] = include_columns
    else:
        if content_type in TABULAR_CONTENT_TYPES:
            kwargs["columns"] = include_columns
        else:
            if include_columns:
                logger.warning(
                    f"Ignoring request to include columns {include_columns} "
                    f"for non-tabular content type {content_type}"
                )


def file_to_dataframe(
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
) -> pd.DataFrame:
    """
    Read a file into a Pandas DataFrame using any filesystem.

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
        pd.DataFrame: The loaded DataFrame
    """
    logger.debug(
        f"Reading {path} to Pandas. Content type: {content_type}. "
        f"Encoding: {content_encoding}"
    )

    pd_read_func = CONTENT_TYPE_TO_READ_FN.get(content_type)
    if not pd_read_func:
        raise NotImplementedError(
            f"Pandas reader for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{list(CONTENT_TYPE_TO_READ_FN.keys())}"
        )

    reader_kwargs = content_type_to_reader_kwargs(content_type)
    _add_column_kwargs(content_type, column_names, include_columns, reader_kwargs)

    # Merge with provided kwargs
    reader_kwargs.update(kwargs)

    if pd_read_func_kwargs_provider:
        reader_kwargs = pd_read_func_kwargs_provider(content_type, reader_kwargs)

    logger.debug(f"Reading {path} via {pd_read_func} with kwargs: {reader_kwargs}")

    dataframe, latency = timed_invocation(
        pd_read_func,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        content_encoding=content_encoding,
        **reader_kwargs,
    )
    logger.debug(f"Time to read {path} into Pandas DataFrame: {latency}s")
    return dataframe


def dataframe_size(dataframe: pd.DataFrame) -> int:
    # TODO (pdames): inspect latency vs. deep memory usage inspection
    return int(dataframe.memory_usage().sum())


def write_csv(
    dataframe: pd.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    # TODO (pdames): Add support for client-specified compression types.
    if kwargs.get("header") is None:
        kwargs["header"] = False

    # Check if the path already indicates compression to avoid double compression
    should_compress = path.endswith(".gz")

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            if should_compress:
                # Path ends with .gz, PyArrow filesystem automatically compresses, no need for additional compression
                dataframe.to_csv(f, **kwargs)
            else:
                # No compression indicated, apply explicit compression
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    dataframe.to_csv(out, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            if should_compress:
                # For fsspec filesystems, we need to apply compression explicitly
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    dataframe.to_csv(out, **kwargs)
            else:
                # No compression indicated, apply explicit compression
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    dataframe.to_csv(out, **kwargs)


def _preprocess_dataframe_for_parquet(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess DataFrame to convert PyArrow types to native Python types for parquet compatibility.

    This handles the case where from_pyarrow() creates pandas DataFrames with PyArrow array objects
    that cannot be serialized by pandas.to_parquet().
    """
    # Check if any columns contain PyArrow arrays
    needs_conversion = False
    for col in dataframe.columns:
        if dataframe[col].dtype == object:
            # Check if the column contains PyArrow arrays
            sample_val = dataframe[col].iloc[0] if len(dataframe) > 0 else None
            if (
                sample_val is not None
                and hasattr(sample_val, "__class__")
                and "pyarrow" in str(type(sample_val))
            ):
                needs_conversion = True
                break

    if not needs_conversion:
        return dataframe

    # Create a copy and convert PyArrow types
    df_copy = dataframe.copy()

    for col in df_copy.columns:
        if df_copy[col].dtype == object and len(df_copy) > 0:
            sample_val = df_copy[col].iloc[0]

            # Convert PyArrow arrays to Python lists
            if hasattr(sample_val, "__class__") and "pyarrow" in str(type(sample_val)):
                try:
                    if hasattr(sample_val, "to_pylist"):
                        # PyArrow array - convert to Python list
                        df_copy[col] = df_copy[col].apply(
                            lambda x: x.to_pylist() if hasattr(x, "to_pylist") else x
                        )
                    elif hasattr(sample_val, "as_py"):
                        # PyArrow scalar - convert to Python value
                        df_copy[col] = df_copy[col].apply(
                            lambda x: x.as_py() if hasattr(x, "as_py") else x
                        )
                except Exception as e:
                    logger.warning(
                        f"Could not convert PyArrow column {col}: {e}. Keeping original values."
                    )

    return df_copy


def write_parquet(
    dataframe: pd.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    # Preprocess DataFrame to handle PyArrow types
    processed_df = _preprocess_dataframe_for_parquet(dataframe)

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            processed_df.to_parquet(f, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            processed_df.to_parquet(f, **kwargs)


def write_orc(
    dataframe: pd.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            dataframe.to_orc(f, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            dataframe.to_orc(f, **kwargs)


def write_feather(
    dataframe: pd.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            dataframe.to_feather(f, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            dataframe.to_feather(f, **kwargs)


def write_json(
    dataframe: pd.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    # Check if the path already indicates compression to avoid double compression
    should_compress = path.endswith(".gz")

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            if should_compress:
                # Path ends with .gz, PyArrow filesystem automatically compresses, no need for additional compression
                dataframe.to_json(f, **kwargs)
            else:
                # No compression indicated, apply explicit compression
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    dataframe.to_json(out, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            if should_compress:
                # For fsspec filesystems, we need to apply compression explicitly
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    dataframe.to_json(out, **kwargs)
            else:
                # No compression indicated, apply explicit compression
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    dataframe.to_json(out, **kwargs)


def write_avro(
    dataframe: pd.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    """
    Write a pandas DataFrame to an AVRO file by delegating to polars implementation.
    """
    import polars as pl
    from deltacat.utils.polars import write_avro as polars_write_avro

    # Convert pandas DataFrame to polars
    include_index = kwargs.pop("index", False)
    pl_df = pl.from_pandas(dataframe, include_index=include_index)

    # Remove pandas-specific kwargs before passing to polars
    polars_kwargs = {k: v for k, v in kwargs.items() if k not in ["index"]}

    # Delegate to polars write_avro implementation
    polars_write_avro(
        pl_df,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        **polars_kwargs,
    )


CONTENT_TYPE_TO_PD_WRITE_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV: write_csv,
    ContentType.TSV.value: write_csv,
    ContentType.CSV.value: write_csv,
    ContentType.PSV.value: write_csv,
    ContentType.PARQUET.value: write_parquet,
    ContentType.FEATHER.value: write_feather,
    ContentType.JSON.value: write_json,
    ContentType.AVRO.value: write_avro,
    ContentType.ORC.value: write_orc,
}


def content_type_to_writer_kwargs(content_type: str) -> Dict[str, Any]:
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "sep": "\t",
            "header": False,
            "na_rep": [""],
            "lineterminator": "\n",
            "quoting": csv.QUOTE_NONE,
            "index": False,
        }
    if content_type == ContentType.TSV.value:
        return {
            "sep": "\t",
            "header": False,
            "lineterminator": "\n",
            "quoting": csv.QUOTE_MINIMAL,
            "index": False,
        }
    if content_type == ContentType.CSV.value:
        return {
            "sep": ",",
            "header": False,
            "index": False,
            "lineterminator": "\n",
            "quoting": csv.QUOTE_MINIMAL,
            "index": False,
        }
    if content_type == ContentType.PSV.value:
        return {
            "sep": "|",
            "header": False,
            "index": False,
            "lineterminator": "\n",
            "quoting": csv.QUOTE_MINIMAL,
        }
    if content_type == ContentType.PARQUET.value:
        return {"index": False}
    if content_type == ContentType.FEATHER.value:
        return {}
    if content_type == ContentType.JSON.value:
        return {"index": False, "orient": "records", "lines": True}
    if content_type == ContentType.AVRO.value:
        return {"index": False}
    if content_type == ContentType.ORC.value:
        return {"index": False}
    raise ValueError(f"Unsupported content type: {content_type}")


def dataframe_to_file(
    dataframe: pd.DataFrame,
    base_path: str,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[Callable, FilenameProvider],
    content_type: str = ContentType.PARQUET.value,
    schema: Optional[pa.Schema] = None,
    **kwargs,
) -> None:
    """
    Writes the given Pandas Dataframe to a file.
    """
    writer = CONTENT_TYPE_TO_PD_WRITE_FUNC.get(content_type)
    writer_kwargs = content_type_to_writer_kwargs(content_type)
    writer_kwargs.update(kwargs)
    if not writer:
        raise NotImplementedError(
            f"Pandas writer for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{CONTENT_TYPE_TO_PD_WRITE_FUNC.keys}"
        )
    filename = block_path_provider(base_path)
    path = posixpath.join(base_path, filename)
    writer(dataframe, path, filesystem=filesystem, **writer_kwargs)
