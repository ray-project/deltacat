import logging
from typing import Optional, List, Dict, Callable, Union

import polars as pl
import pyarrow as pa
import pyarrow.fs as pafs

from fsspec import AbstractFileSystem
from ray.data.datasource import FilenameProvider

from deltacat import logs
from deltacat.utils.filesystem import resolve_path_and_filesystem

from deltacat.types.media import ContentType, ContentEncoding

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
            "has_header": False,
            "null_values": [""],
            "quote_char": None,  # Equivalent to QUOTE_NONE in pandas
        }
    if content_type == ContentType.TSV.value:
        return {
            "separator": "\t",
            "has_header": False,
        }
    if content_type == ContentType.CSV.value:
        return {
            "separator": ",",
            "has_header": False,
        }
    if content_type == ContentType.PSV.value:
        return {
            "separator": "|",
            "has_header": False,
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
    Writes the given Pyarrow Table to a file.
    """
    writer = CONTENT_TYPE_TO_PL_WRITE_FUNC.get(content_type)
    if not writer:
        raise NotImplementedError(
            f"Pyarrow writer for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{CONTENT_TYPE_TO_PL_WRITE_FUNC.keys}"
        )
    path = block_path_provider(base_path)
    logger.debug(f"Writing table: {table} with kwargs: {kwargs} to path: {path}")
    writer(table, path, filesystem=filesystem, **kwargs)


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
