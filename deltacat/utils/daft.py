import logging
from typing import Optional, List, Any, Dict, Callable
import daft
import ray
from daft.table import read_parquet_into_pyarrow
from daft import TimeUnit, DataFrame
from daft.io import IOConfig, S3Config
import pyarrow as pa

from deltacat import logs
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.schema import coerce_pyarrow_table_to_schema

from deltacat.types.media import ContentType, ContentEncoding
from deltacat.aws.constants import (
    BOTO_MAX_RETRIES,
    DAFT_MAX_S3_CONNECTIONS_PER_FILE,
    AWS_REGION,
    DEFAULT_FILE_READ_TIMEOUT_MS,
)
from deltacat.utils.performance import timed_invocation

from deltacat.types.partial_download import (
    PartialFileDownloadParams,
)


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def s3_files_to_dataframe(
    uris: List[str],
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Optional[Callable[[int, Any], Dict[str, Any]]] = None,
    s3_client_kwargs: Optional[Any] = None,
    ray_init_options: Optional[Dict[str, Any]] = None,
) -> DataFrame:

    if ray_init_options is None:
        ray_init_options = {}

    assert (
        content_type == ContentType.PARQUET.value
    ), f"daft native reader currently only supports parquet, got {content_type}"

    assert (
        content_encoding == ContentEncoding.IDENTITY.value
    ), f"daft native reader currently only supports identity encoding, got {content_encoding}"

    if not ray.is_initialized():
        ray.init(address="auto", ignore_reinit_error=True, **ray_init_options)

    daft.context.set_runner_ray(noop_if_initialized=True)

    if s3_client_kwargs is None:
        s3_client_kwargs = {}

    kwargs = {}
    if read_func_kwargs_provider is not None:
        kwargs = read_func_kwargs_provider(content_type, kwargs)

    # TODO(raghumdani): pass in coerce_int96_timestamp arg
    # https://github.com/Eventual-Inc/Daft/issues/1894

    io_config = _get_s3_io_config(s3_client_kwargs=s3_client_kwargs)

    logger.debug(
        f"Preparing to read S3 object from {len(uris)} files into daft dataframe"
    )

    df, latency = timed_invocation(
        daft.read_parquet, path=uris, io_config=io_config, use_native_downloader=True
    )

    logger.debug(f"Time to create daft dataframe from {len(uris)} files is {latency}s")

    columns_to_read = include_columns or column_names

    logger.debug(f"Taking columns {columns_to_read} from the daft df.")

    if columns_to_read:
        return df.select(*columns_to_read)
    else:
        return df


def daft_s3_file_to_table(
    s3_url: str,
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    **s3_client_kwargs,
):
    assert (
        content_type == ContentType.PARQUET.value
    ), f"daft native reader currently only supports parquet, got {content_type}"

    assert (
        content_encoding == ContentEncoding.IDENTITY.value
    ), f"daft native reader currently only supports identity encoding, got {content_encoding}"

    kwargs = {}
    if pa_read_func_kwargs_provider is not None:
        kwargs = pa_read_func_kwargs_provider(content_type, kwargs)

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )
    file_timeout_ms = kwargs.get("file_timeout_ms", DEFAULT_FILE_READ_TIMEOUT_MS)

    row_groups = None
    if (
        partial_file_download_params is not None
        and partial_file_download_params.row_groups_to_download is not None
    ):
        row_groups = partial_file_download_params.row_groups_to_download

    io_config = _get_s3_io_config(s3_client_kwargs=s3_client_kwargs)

    logger.debug(f"Preparing to read S3 object from {s3_url} into daft table")

    pa_table, latency = timed_invocation(
        read_parquet_into_pyarrow,
        path=s3_url,
        columns=include_columns or column_names,
        row_groups=row_groups,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        multithreaded_io=False,
        file_timeout_ms=file_timeout_ms,
    )

    logger.debug(f"Time to read S3 object from {s3_url} into daft table: {latency}s")

    if kwargs.get("schema") is not None:
        input_schema = kwargs["schema"]
        if include_columns is not None:
            input_schema = pa.schema(
                [input_schema.field(col) for col in include_columns],
                metadata=input_schema.metadata,
            )
        elif column_names is not None:
            input_schema = pa.schema(
                [input_schema.field(col) for col in column_names],
                metadata=input_schema.metadata,
            )
        return coerce_pyarrow_table_to_schema(pa_table, input_schema)
    else:
        return pa_table


def _get_s3_io_config(s3_client_kwargs) -> IOConfig:
    return IOConfig(
        s3=S3Config(
            key_id=s3_client_kwargs.get("aws_access_key_id"),
            access_key=s3_client_kwargs.get("aws_secret_access_key"),
            session_token=s3_client_kwargs.get("aws_session_token"),
            region_name=AWS_REGION,
            retry_mode="adaptive",
            num_tries=BOTO_MAX_RETRIES,
            max_connections=DAFT_MAX_S3_CONNECTIONS_PER_FILE,
            connect_timeout_ms=5_000,  # Timeout to connect to server
            read_timeout_ms=10_000,  # Timeout for first byte from server
        )
    )
