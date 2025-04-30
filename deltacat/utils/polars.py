import logging
from typing import Optional, List, Dict, Callable, Union

import polars as pl

from fsspec import AbstractFileSystem
from ray.data.datasource import FilenameProvider

from deltacat import logs

from deltacat.types.media import ContentType

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def write_json(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[AbstractFileSystem] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem:
        table.write_ndjson(path, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            table.write_ndjson(f, **write_kwargs)


def write_csv(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[AbstractFileSystem] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem:
        table.write_csv(path, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            table.write_csv(f, **write_kwargs)


def write_avro(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[AbstractFileSystem] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem:
        table.write_avro(path, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            table.write_avro(f, **write_kwargs)


def write_parquet(
    table: pl.DataFrame,
    path: str,
    *,
    filesystem: Optional[AbstractFileSystem] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem:
        table.write_parquet(path, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            table.write_parquet(f, **write_kwargs)


CONTENT_TYPE_TO_PL_WRITE_FUNC: Dict[str, Callable] = {
    # TODO (pdames): add support for other delimited text content types as
    #  pyarrow adds support for custom delimiters, escaping, and None value
    #  representations to pyarrow.csv.WriteOptions.
    ContentType.AVRO.value: write_avro,
    ContentType.CSV.value: write_csv,
    ContentType.PARQUET.value: write_parquet,
    ContentType.JSON.value: write_json,
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
    file_system: Optional[AbstractFileSystem],
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
    writer(table, path, filesystem=file_system, **kwargs)
