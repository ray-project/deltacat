import pandas as pd
import math
import io
import logging
import pyarrow as pa

from fsspec import AbstractFileSystem

from ray.data.datasource import BlockWritePathProvider

from deltacat.types.media import ContentType, ContentEncoding, \
    EXPLICIT_COMPRESSION_CONTENT_TYPES
from deltacat.types.media import CONTENT_TYPE_TO_USER_KWARGS_KEY, \
    DELIMITED_TEXT_CONTENT_TYPES, TABULAR_CONTENT_TYPES
from deltacat import logs
from deltacat.utils.performance import timed_invocation
from deltacat.utils import pyarrow as pa_utils

from typing import Any, Callable, Dict, List, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


CONTENT_TYPE_TO_PD_READ_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: pd.read_csv,
    ContentType.TSV.value: pd.read_csv,
    ContentType.CSV.value: pd.read_csv,
    ContentType.PSV.value: pd.read_csv,
    ContentType.PARQUET.value: pd.read_parquet,
    ContentType.FEATHER.value: pd.read_feather,
    ContentType.ORC.value: pd.read_orc,
    ContentType.JSON.value: pd.read_json
}

CONTENT_TYPE_TO_READER_KWARGS: Dict[str, Dict[str, Any]] = {
    ContentType.UNESCAPED_TSV.value: {
        "sep": "\t",
        "header": None,
        "na_values": [""],
        "keep_default_na": False,
    },
    ContentType.TSV.value: {
        "sep": "\t",
        "header": None
    },
    ContentType.CSV.value: {
        "sep": ",",
        "header": None
    },
    ContentType.PSV.value: {
        "sep": "|",
        "header": None
    },
    ContentType.PARQUET.value: {},
    ContentType.FEATHER.value: {},
    ContentType.ORC.value: {},
    ContentType.JSON.value: {},
}

ENCODING_TO_PD_COMPRESSION: Dict[str, str] = {
    ContentEncoding.GZIP.value: "gzip",
    ContentEncoding.BZIP2.value: "bz2",
    ContentEncoding.IDENTITY.value: "none"
}


def slice_dataframe(
        dataframe: pd.DataFrame,
        max_len: Optional[int]) -> List[pd.DataFrame]:
    """
    Iteratively create dataframe slices.
    """
    if max_len is None:
        return [dataframe]
    dataframes = []
    num_slices = math.ceil(len(dataframe) / max_len)
    for i in range(num_slices):
        dataframes.append(dataframe[i * max_len: (i + 1) * max_len])
    return dataframes


def concat_dataframes(dataframes: List[pd.DataFrame]) \
        -> Optional[pd.DataFrame]:
    if dataframes is None or not len(dataframes):
        return None
    if len(dataframes) == 1:
        return next(iter(dataframes))
    return pd.concat(dataframes, axis=0, copy=False)


def _add_column_kwargs(
        content_type: str,
        column_names: Optional[List[str]],
        include_columns: Optional[List[str]],
        kwargs: Dict[str, Any]):

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
                    f"for non-tabular content type {content_type}")


def s3_file_to_dataframe(
        s3_url: str,
        content_type: str,
        content_encoding: str,
        column_names: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        pd_read_func_kwargs: Optional[Dict[str, Any]] = None,
        **s3_client_kwargs) -> pd.DataFrame:

    from deltacat.aws import s3u as s3_utils
    logger.debug(f"Reading {s3_url} to Pandas. Content type: {content_type}. "
                 f"Encoding: {content_encoding}")
    s3_obj = s3_utils.get_object_at_url(
        s3_url,
        **s3_client_kwargs
    )
    logger.debug(f"Read S3 object from {s3_url}: {s3_obj}")
    pd_read_func = CONTENT_TYPE_TO_PD_READ_FUNC[content_type]
    args = [io.BytesIO(s3_obj["Body"].read())]
    kwargs = CONTENT_TYPE_TO_READER_KWARGS[content_type]
    _add_column_kwargs(content_type, column_names, include_columns, kwargs)

    if pd_read_func_kwargs is None:
        pd_read_func_kwargs = {}
    if content_type in EXPLICIT_COMPRESSION_CONTENT_TYPES:
        kwargs["compression"] = ENCODING_TO_PD_COMPRESSION.get(
            content_encoding,
            "infer"
        )
    if pd_read_func_kwargs:
        kwargs.update(pd_read_func_kwargs.get(
            CONTENT_TYPE_TO_USER_KWARGS_KEY[content_type]
        ))
    logger.debug(f"Reading {s3_url} via {pd_read_func} with kwargs: {kwargs}")
    dataframe, latency = timed_invocation(
        pd_read_func,
        *args,
        **kwargs
    )
    logger.debug(f"Time to read {s3_url} into Pandas Dataframe: {latency}s")
    return dataframe


def dataframe_size(dataframe: pd.DataFrame) -> int:
    # TODO (pdames): inspect latency vs. deep memory usage inspection
    return int(dataframe.memory_usage().sum())


def dataframe_to_file(
        dataframe: pd.DataFrame,
        path: str,
        file_system: AbstractFileSystem,
        block_path_provider: BlockWritePathProvider,
        content_type: str = ContentType.PARQUET.value,
        **kwargs) -> None:
    """
    Writes the given Pandas Dataframe to a file.
    """
    pa_utils.table_to_file(
        pa.Table.from_pandas(dataframe),
        path,
        file_system,
        block_path_provider,
        content_type,
        **kwargs
    )
