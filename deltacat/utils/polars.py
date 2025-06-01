import logging
import io
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
    EXPLICIT_COMPRESSION_CONTENT_TYPES,
    TABULAR_CONTENT_TYPES,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def write_json(
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
            with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                table.write_ndjson(out, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            # TODO (pdames): Add support for client-specified compression types.
            with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                table.write_ndjson(out, **write_kwargs)


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
        }
    if content_type == ContentType.CSV.value:
        return {
            "separator": ",",
            "include_header": False,
        }
    if content_type == ContentType.PSV.value:
        return {
            "separator": "|",
            "include_header": False,
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
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                table.write_csv(out, **kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                table.write_csv(out, **kwargs)


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
    path = block_path_provider(base_path)
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
    writer(table, path, filesystem=filesystem, fs_open_kwargs=fs_open_kwargs, **writer_kwargs)


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
                # read all columns as strings - polars uses dtypes dict
                kwargs["dtypes"] = pl.Utf8
            else:
                # read only the included columns as strings
                kwargs["dtypes"] = {column_name: pl.Utf8 for column_name in include_columns}
        return kwargs


def content_type_to_reader_kwargs(content_type: str) -> Dict[str, Any]:
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "separator": "\t",
            "has_header": False,
            "null_values": [""],
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


def s3_file_to_dataframe(
    s3_url: str,
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pl_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **s3_client_kwargs,
) -> pl.DataFrame:

    from deltacat.aws import s3u as s3_utils

    logger.debug(
        f"Reading {s3_url} to Polars. Content type: {content_type}. "
        f"Encoding: {content_encoding}"
    )
    s3_obj = s3_utils.get_object_at_url(s3_url, **s3_client_kwargs)
    logger.debug(f"Read S3 object from {s3_url}: {s3_obj}")
    
    # Handle ORC files specially since polars doesn't have native support
    if content_type == ContentType.ORC.value:
        # Use pandas to read ORC and convert to polars
        import pandas as pd
        pd_df = pd.read_orc(io.BytesIO(s3_obj["Body"].read()))
        dataframe = pl.from_pandas(pd_df)
        logger.debug(f"Time to read {s3_url} into Polars DataFrame via pandas")
        return dataframe
    
    pl_read_func = CONTENT_TYPE_TO_PL_READ_FUNC[content_type]
    kwargs = content_type_to_reader_kwargs(content_type)
    _add_column_kwargs(content_type, column_names, include_columns, kwargs)

    # Handle compression for delimited text files
    if content_type in EXPLICIT_COMPRESSION_CONTENT_TYPES:
        if content_encoding == ContentEncoding.GZIP.value:
            # Polars can handle gzip compression automatically for CSV-like files
            # For gzip files, we need to handle them specially
            import gzip
            with gzip.GzipFile(fileobj=io.BytesIO(s3_obj["Body"].read())) as gz:
                source = io.BytesIO(gz.read())
        elif content_encoding == ContentEncoding.BZIP2.value:
            # Polars doesn't natively support bz2, need to decompress first
            import bz2
            data = bz2.decompress(s3_obj["Body"].read())
            source = io.BytesIO(data)
        else:
            source = io.BytesIO(s3_obj["Body"].read())
    else:
        source = io.BytesIO(s3_obj["Body"].read())
    
    if pl_read_func_kwargs_provider:
        kwargs = pl_read_func_kwargs_provider(content_type, kwargs)
    
    logger.debug(f"Reading {s3_url} via {pl_read_func} with kwargs: {kwargs}")
     
    dataframe, latency = timed_invocation(pl_read_func, source, **kwargs)
    logger.debug(f"Time to read {s3_url} into Polars DataFrame: {latency}s")
    return dataframe
