import logging
import posixpath
import bz2
import gzip
from functools import partial
from typing import Optional, List, Dict, Callable, Union, Iterable, Any

import polars as pl
import pyarrow as pa
import pyarrow.fs as pafs

from fsspec import AbstractFileSystem
from ray.data.datasource import FilenameProvider

from deltacat import logs
from deltacat.utils.filesystem import resolve_path_and_filesystem
from deltacat.utils.common import ContentTypeKwargsProvider, ReadKwargsProvider
from deltacat.utils.performance import timed_invocation

from deltacat.types.media import (
    ContentType,
    ContentEncoding,
    DELIMITED_TEXT_CONTENT_TYPES,
    TABULAR_CONTENT_TYPES,
)
from deltacat.types.partial_download import PartialFileDownloadParams

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

# Encoding to file initialization function mapping
ENCODING_TO_FILE_INIT: Dict[str, Callable] = {
    ContentEncoding.GZIP.value: partial(gzip.open, mode="rb"),
    ContentEncoding.BZIP2.value: partial(bz2.open, mode="rb"),
    ContentEncoding.IDENTITY.value: lambda file_path: file_path,
}


def write_json(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    # Check if the path already indicates compression to avoid double compression
    should_compress = path.endswith(".gz")

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            if should_compress:
                # Path ends with .gz, PyArrow filesystem automatically compresses
                table.write_ndjson(f, **write_kwargs)
            else:
                # No compression indicated, write uncompressed
                table.write_ndjson(f, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            if should_compress:
                # For fsspec filesystems, we need to apply compression explicitly
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    table.write_ndjson(out, **write_kwargs)
            else:
                # No compression indicated, write uncompressed
                table.write_ndjson(f, **write_kwargs)


def content_type_to_writer_kwargs(content_type: str) -> Dict[str, any]:
    """
    Returns writer kwargs for the given content type when writing with polars.
    """
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "separator": "\t",
            "include_header": False,
            "null_value": "",
            "quote_style": "never",  # Equivalent to QUOTE_NONE in pandas
        }
    if content_type == ContentType.TSV.value:
        return {
            "separator": "\t",
            "include_header": False,
            "quote_style": "necessary",
        }
    if content_type == ContentType.CSV.value:
        return {
            "separator": ",",
            "include_header": False,
            "quote_style": "necessary",
        }
    if content_type == ContentType.PSV.value:
        return {
            "separator": "|",
            "include_header": False,
            "quote_style": "necessary",
        }
    if content_type in {
        ContentType.PARQUET.value,
        ContentType.FEATHER.value,
        ContentType.JSON.value,
        ContentType.AVRO.value,
        ContentType.ORC.value,
    }:
        return {}
    raise ValueError(f"Unsupported content type: {content_type}")


def write_csv(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    """
    Write a polars DataFrame to a CSV file (or other delimited text format).
    """
    # Check if the path already indicates compression to avoid double compression
    should_compress = path.endswith(".gz")

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            if should_compress:
                # Path ends with .gz, PyArrow filesystem automatically compresses
                table.write_csv(f, **kwargs)
            else:
                # No compression indicated, write uncompressed
                table.write_csv(f, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            if should_compress:
                # For fsspec filesystems, we need to apply compression explicitly
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    table.write_csv(out, **kwargs)
            else:
                # No compression indicated, write uncompressed
                table.write_csv(f, **kwargs)


def write_avro(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            table.write_avro(f, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            table.write_avro(f, **write_kwargs)


def write_parquet(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            table.write_parquet(f, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            table.write_parquet(f, **write_kwargs)


def write_feather(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **kwargs,
) -> None:
    """
    Write a polars DataFrame to a Feather file.
    """
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            table.write_ipc(f, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            table.write_ipc(f, **kwargs)


def write_orc(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    """
    Write a polars DataFrame to an ORC file by delegating to PyArrow implementation.
    """
    from deltacat.utils.pyarrow import write_orc as pyarrow_write_orc

    # Convert polars DataFrame to PyArrow Table
    pa_table = table.to_arrow()

    # Delegate to PyArrow write_orc implementation
    pyarrow_write_orc(
        pa_table,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        **write_kwargs,
    )


CONTENT_TYPE_TO_PL_WRITE_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: write_csv,
    ContentType.TSV.value: write_csv,
    ContentType.CSV.value: write_csv,
    ContentType.PSV.value: write_csv,
    ContentType.PARQUET.value: write_parquet,
    ContentType.FEATHER.value: write_feather,
    ContentType.JSON.value: write_json,
    ContentType.AVRO.value: write_avro,
    ContentType.ORC.value: write_orc,
}


def slice_table(table: pl.DataFrame, max_len: Optional[int]) -> List[pl.DataFrame]:
    """
    Iteratively create 0-copy table slices.
    """
    if max_len is None:
        return [table]
    tables = []
    offset = 0
    records_remaining = len(table)
    while records_remaining > 0:
        records_this_entry = min(max_len, records_remaining)
        tables.append(table.slice(offset, records_this_entry))
        records_remaining -= records_this_entry
        offset += records_this_entry
    return tables


def dataframe_size(table: pl.DataFrame) -> int:
    return table.estimated_size()


def dataframe_to_file(
    table: pl.DataFrame,
    base_path: str,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[Callable, FilenameProvider],
    content_type: str = ContentType.PARQUET.value,
    schema: Optional[pa.Schema] = None,
    **kwargs,
) -> None:
    """
    Writes the given Polars DataFrame to a file.
    """
    writer = CONTENT_TYPE_TO_PL_WRITE_FUNC.get(content_type)
    writer_kwargs = content_type_to_writer_kwargs(content_type)
    writer_kwargs.update(kwargs)
    if not writer:
        raise NotImplementedError(
            f"Polars writer for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{CONTENT_TYPE_TO_PL_WRITE_FUNC.keys()}"
        )
    filename = block_path_provider(base_path)
    path = posixpath.join(base_path, filename)
    logger.debug(f"Writing table: {table} with kwargs: {writer_kwargs} to path: {path}")
    writer(table, path, filesystem=filesystem, **writer_kwargs)


def write_table(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_type: str = ContentType.PARQUET.value,
    **kwargs,
) -> None:
    """
    Write a polars DataFrame to a file in the specified format.
    """
    writer = CONTENT_TYPE_TO_PL_WRITE_FUNC.get(content_type)
    writer_kwargs = content_type_to_writer_kwargs(content_type)
    writer_kwargs.update(kwargs)
    if not writer:
        raise NotImplementedError(
            f"Polars writer for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{CONTENT_TYPE_TO_PL_WRITE_FUNC.keys()}"
        )
    writer(
        table,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        **writer_kwargs,
    )


CONTENT_TYPE_TO_PL_READ_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: pl.read_csv,
    ContentType.TSV.value: pl.read_csv,
    ContentType.CSV.value: pl.read_csv,
    ContentType.PSV.value: pl.read_csv,
    ContentType.PARQUET.value: pl.read_parquet,
    ContentType.FEATHER.value: pl.read_ipc,
    ContentType.JSON.value: pl.read_ndjson,
    ContentType.AVRO.value: pl.read_avro,
}


class ReadKwargsProviderPolarsStringTypes(ContentTypeKwargsProvider):
    """ReadKwargsProvider impl that reads columns of delimited text files
    as UTF-8 strings (i.e. disables type inference). Useful for ensuring
    lossless reads of UTF-8 delimited text datasets and improving read
    performance in cases where type casting is not required."""

    def __init__(self, include_columns: Optional[Iterable[str]] = None):
        self.include_columns = include_columns

    def _get_kwargs(self, content_type: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if content_type in DELIMITED_TEXT_CONTENT_TYPES:
            include_columns = (
                self.include_columns if self.include_columns else kwargs.get("columns")
            )
            if not include_columns:
                # read all columns as strings - disable schema inference
                kwargs["infer_schema"] = False
            else:
                # read only the included columns as strings
                kwargs["schema_overrides"] = {
                    column_name: pl.Utf8 for column_name in include_columns
                }
        return kwargs


def content_type_to_reader_kwargs(content_type: str) -> Dict[str, Any]:
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "separator": "\t",
            "has_header": False,
            "null_values": [""],
            "quote_char": None,
        }
    if content_type == ContentType.TSV.value:
        return {"separator": "\t", "has_header": False}
    if content_type == ContentType.CSV.value:
        return {"separator": ",", "has_header": False}
    if content_type == ContentType.PSV.value:
        return {"separator": "|", "has_header": False}
    if content_type in {
        ContentType.PARQUET.value,
        ContentType.FEATHER.value,
        ContentType.ORC.value,
        ContentType.JSON.value,
        ContentType.AVRO.value,
    }:
        return {}
    raise ValueError(f"Unsupported content type: {content_type}")


def _add_column_kwargs(
    content_type: str,
    column_names: Optional[List[str]],
    include_columns: Optional[List[str]],
    kwargs: Dict[str, Any],
):
    if content_type in DELIMITED_TEXT_CONTENT_TYPES:
        if column_names:
            kwargs["new_columns"] = column_names
        if include_columns:
            kwargs["columns"] = include_columns
    else:
        if content_type in TABULAR_CONTENT_TYPES:
            if include_columns:
                kwargs["columns"] = include_columns
        else:
            if include_columns:
                logger.warning(
                    f"Ignoring request to include columns {include_columns} "
                    f"for non-tabular content type {content_type}"
                )


def concat_dataframes(dataframes: List[pl.DataFrame]) -> Optional[pl.DataFrame]:
    if dataframes is None or not len(dataframes):
        return None
    if len(dataframes) == 1:
        return next(iter(dataframes))
    return pl.concat(dataframes)


def append_column_to_table(
    table: pl.DataFrame,
    column_name: str,
    column_value: Any,
) -> pl.DataFrame:
    return table.with_columns(pl.lit(column_value).alias(column_name))


def select_columns(
    table: pl.DataFrame,
    column_names: List[str],
) -> pl.DataFrame:
    return table.select(column_names)


def file_to_dataframe(
    path: str,
    content_type: str,
    content_encoding: str = ContentEncoding.IDENTITY.value,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pl_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    fs_open_kwargs: Dict[str, Any] = {},
    **kwargs,
) -> pl.DataFrame:
    """
    Read a file into a Polars DataFrame using any filesystem.

    Args:
        path: The file path to read
        content_type: The content type of the file (e.g., ContentType.CSV.value)
        content_encoding: The content encoding (default: IDENTITY)
        filesystem: The filesystem to use (if None, will be inferred from path)
        column_names: Optional column names to assign
        include_columns: Optional columns to include in the result
        pl_read_func_kwargs_provider: Optional kwargs provider for customization
        fs_open_kwargs: Optional kwargs for filesystem open operations
        **kwargs: Additional kwargs passed to the reader function

    Returns:
        pl.DataFrame: The loaded DataFrame
    """
    logger.debug(
        f"Reading {path} to Polars. Content type: {content_type}. "
        f"Encoding: {content_encoding}"
    )

    pl_read_func = CONTENT_TYPE_TO_READ_FN.get(content_type)
    if not pl_read_func:
        raise NotImplementedError(
            f"Polars reader for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{list(CONTENT_TYPE_TO_READ_FN.keys())}"
        )

    reader_kwargs = content_type_to_reader_kwargs(content_type)
    _add_column_kwargs(content_type, column_names, include_columns, reader_kwargs)

    # Merge with provided kwargs
    reader_kwargs.update(kwargs)

    if pl_read_func_kwargs_provider:
        reader_kwargs = pl_read_func_kwargs_provider(content_type, reader_kwargs)

    logger.debug(f"Reading {path} via {pl_read_func} with kwargs: {reader_kwargs}")

    dataframe, latency = timed_invocation(
        pl_read_func,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        content_encoding=content_encoding,
        **reader_kwargs,
    )
    logger.debug(f"Time to read {path} into Polars DataFrame: {latency}s")
    return dataframe


def read_csv(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pl.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        if content_encoding == ContentEncoding.IDENTITY.value:
            with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
                return pl.read_csv(f, **read_kwargs)
        else:
            # For compressed files with PyArrow, we need to be careful because PyArrow
            # may auto-decompress some formats. Try to read directly first.
            try:
                with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
                    # Try reading as if it's already decompressed by PyArrow
                    return pl.read_csv(f, **read_kwargs)
            except Exception:
                # If that fails, try manual decompression
                with filesystem.open_input_file(path, **fs_open_kwargs) as f:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f) as input_file:
                        content = input_file.read()
                        if isinstance(content, str):
                            content = content.encode("utf-8")
                        return pl.read_csv(content, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_csv(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    if isinstance(content, str):
                        content = content.encode("utf-8")
                    return pl.read_csv(content, **read_kwargs)


def read_parquet(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pl.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_parquet(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    return pl.read_parquet(content, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_parquet(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    return pl.read_parquet(content, **read_kwargs)


def read_ipc(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pl.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_ipc(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    return pl.read_ipc(content, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_ipc(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    return pl.read_ipc(content, **read_kwargs)


def read_ndjson(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pl.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        if content_encoding == ContentEncoding.IDENTITY.value:
            with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
                return pl.read_ndjson(f, **read_kwargs)
        else:
            # For compressed files with PyArrow, we need to be careful because PyArrow
            # may auto-decompress some formats. Try to read directly first.
            try:
                with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
                    # Try reading as if it's already decompressed by PyArrow
                    return pl.read_ndjson(f, **read_kwargs)
            except Exception:
                # If that fails, try manual decompression
                with filesystem.open_input_file(path, **fs_open_kwargs) as f:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f) as input_file:
                        content = input_file.read()
                        if isinstance(content, str):
                            content = content.encode("utf-8")
                        return pl.read_ndjson(content, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_ndjson(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    if isinstance(content, str):
                        content = content.encode("utf-8")
                    return pl.read_ndjson(content, **read_kwargs)


def read_avro(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pl.DataFrame:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_avro(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    return pl.read_avro(content, **read_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                return pl.read_avro(f, **read_kwargs)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content as bytes and pass to polars
                    content = input_file.read()
                    return pl.read_avro(content, **read_kwargs)


def read_orc(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pl.DataFrame:
    """
    Read an ORC file using pandas and convert to polars since polars doesn't have native ORC support.
    """
    import pandas as pd

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                pd_df = pd.read_orc(f, **read_kwargs)
                return pl.from_pandas(pd_df)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content and pass to pandas
                    content = input_file.read()
                    import io

                    pd_df = pd.read_orc(io.BytesIO(content), **read_kwargs)
                    return pl.from_pandas(pd_df)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            if content_encoding == ContentEncoding.IDENTITY.value:
                pd_df = pd.read_orc(f, **read_kwargs)
                return pl.from_pandas(pd_df)
            else:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Read decompressed content and pass to pandas
                    content = input_file.read()
                    import io

                    pd_df = pd.read_orc(io.BytesIO(content), **read_kwargs)
                    return pl.from_pandas(pd_df)


# New mapping for encoding-aware reader functions used by file_to_dataframe
CONTENT_TYPE_TO_READ_FN: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: read_csv,
    ContentType.TSV.value: read_csv,
    ContentType.CSV.value: read_csv,
    ContentType.PSV.value: read_csv,
    ContentType.PARQUET.value: read_parquet,
    ContentType.FEATHER.value: read_ipc,
    ContentType.JSON.value: read_ndjson,
    ContentType.AVRO.value: read_avro,
    ContentType.ORC.value: read_orc,
}
