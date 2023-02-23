import csv
import io
import logging
import math
from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from fsspec import AbstractFileSystem
from ray.data.datasource import BlockWritePathProvider

from deltacat import logs
from deltacat.types.media import (
    DELIMITED_TEXT_CONTENT_TYPES,
    EXPLICIT_COMPRESSION_CONTENT_TYPES,
    TABULAR_CONTENT_TYPES,
    ContentEncoding,
    ContentType,
)
from deltacat.utils.common import ContentTypeKwargsProvider, ReadKwargsProvider
from deltacat.utils.performance import timed_invocation

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


CONTENT_TYPE_TO_PD_READ_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: pd.read_csv,
    ContentType.TSV.value: pd.read_csv,
    ContentType.CSV.value: pd.read_csv,
    ContentType.PSV.value: pd.read_csv,
    ContentType.PARQUET.value: pd.read_parquet,
    ContentType.FEATHER.value: pd.read_feather,
    ContentType.ORC.value: pd.read_orc,
    ContentType.JSON.value: pd.read_json,
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
        }
    if content_type == ContentType.TSV.value:
        return {"sep": "\t", "header": None}
    if content_type == ContentType.CSV.value:
        return {"sep": ",", "header": None}
    if content_type == ContentType.PSV.value:
        return {"sep": "|", "header": None}
    if content_type in {
        ContentType.PARQUET.value,
        ContentType.FEATHER.value,
        ContentType.ORC.value,
        ContentType.JSON.value,
    }:
        return {}
    raise ValueError(f"Unsupported content type: {content_type}")


ENCODING_TO_PD_COMPRESSION: Dict[str, str] = {
    ContentEncoding.GZIP.value: "gzip",
    ContentEncoding.BZIP2.value: "bz2",
    ContentEncoding.IDENTITY.value: "none",
}


def slice_dataframe(
    dataframe: pd.DataFrame, max_len: Optional[int]
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


def concat_dataframes(dataframes: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if dataframes is None or not len(dataframes):
        return None
    if len(dataframes) == 1:
        return next(iter(dataframes))
    return pd.concat(dataframes, axis=0, copy=False)


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
            kwargs["columns"]: include_columns
        else:
            if include_columns:
                logger.warning(
                    f"Ignoring request to include columns {include_columns} "
                    f"for non-tabular content type {content_type}"
                )


def s3_file_to_dataframe(
    s3_url: str,
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pd_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **s3_client_kwargs,
) -> pd.DataFrame:

    from deltacat.aws import s3u as s3_utils

    logger.debug(
        f"Reading {s3_url} to Pandas. Content type: {content_type}. "
        f"Encoding: {content_encoding}"
    )
    s3_obj = s3_utils.get_object_at_url(s3_url, **s3_client_kwargs)
    logger.debug(f"Read S3 object from {s3_url}: {s3_obj}")
    pd_read_func = CONTENT_TYPE_TO_PD_READ_FUNC[content_type]
    args = [io.BytesIO(s3_obj["Body"].read())]
    kwargs = content_type_to_reader_kwargs(content_type)
    _add_column_kwargs(content_type, column_names, include_columns, kwargs)

    if content_type in EXPLICIT_COMPRESSION_CONTENT_TYPES:
        kwargs["compression"] = ENCODING_TO_PD_COMPRESSION.get(
            content_encoding, "infer"
        )
    if pd_read_func_kwargs_provider:
        kwargs = pd_read_func_kwargs_provider(content_type, kwargs)
    logger.debug(f"Reading {s3_url} via {pd_read_func} with kwargs: {kwargs}")
    dataframe, latency = timed_invocation(pd_read_func, *args, **kwargs)
    logger.debug(f"Time to read {s3_url} into Pandas Dataframe: {latency}s")
    return dataframe


def dataframe_size(dataframe: pd.DataFrame) -> int:
    # TODO (pdames): inspect latency vs. deep memory usage inspection
    return int(dataframe.memory_usage().sum())


def write_csv(
    dataframe: pd.DataFrame, path: str, *, filesystem: AbstractFileSystem, **kwargs
) -> None:
    with filesystem.open(path, "wb") as f:
        # TODO (pdames): Add support for client-specified compression types.
        with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
            dataframe.to_csv(out, **kwargs)


def write_parquet(
    dataframe: pd.DataFrame, path: str, *, filesystem: AbstractFileSystem, **kwargs
) -> None:
    with filesystem.open(path, "wb") as f:
        dataframe.to_parquet(f, **kwargs)


def write_feather(
    dataframe: pd.DataFrame, path: str, *, filesystem: AbstractFileSystem, **kwargs
) -> None:
    with filesystem.open(path, "wb") as f:
        dataframe.to_feather(f, **kwargs)


def write_json(
    dataframe: pd.DataFrame, path: str, *, filesystem: AbstractFileSystem, **kwargs
) -> None:
    with filesystem.open(path, "wb") as f:
        # TODO (pdames): Add support for client-specified compression types.
        with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
            dataframe.to_json(out, **kwargs)


CONTENT_TYPE_TO_PD_WRITE_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV: write_csv,
    ContentType.TSV.value: write_csv,
    ContentType.CSV.value: write_csv,
    ContentType.PSV.value: write_csv,
    ContentType.PARQUET.value: write_parquet,
    ContentType.FEATHER.value: write_feather,
    ContentType.JSON.value: write_json,
}


def content_type_to_writer_kwargs(content_type: str) -> Dict[str, Any]:
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "sep": "\t",
            "header": False,
            "na_rep": [""],
            "line_terminator": "\n",
            "quoting": csv.QUOTE_NONE,
            "index": False,
        }
    if content_type == ContentType.TSV.value:
        return {
            "sep": "\t",
            "header": False,
            "line_terminator": "\n",
            "index": False,
        }
    if content_type == ContentType.CSV.value:
        return {
            "sep": ",",
            "header": False,
            "line_terminator": "\n",
            "index": False,
        }
    if content_type == ContentType.PSV.value:
        return {
            "sep": "|",
            "header": False,
            "line_terminator": "\n",
            "index": False,
        }
    if content_type == ContentType.PARQUET.value:
        return {"index": False}
    if content_type == ContentType.FEATHER.value:
        return {}
    if content_type == ContentType.JSON.value:
        return {"index": False}
    raise ValueError(f"Unsupported content type: {content_type}")


def dataframe_to_file(
    dataframe: pd.DataFrame,
    base_path: str,
    file_system: AbstractFileSystem,
    block_path_provider: BlockWritePathProvider,
    content_type: str = ContentType.PARQUET.value,
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
    path = block_path_provider(base_path)
    writer(dataframe, path, filesystem=file_system, **writer_kwargs)
