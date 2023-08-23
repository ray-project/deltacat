import logging
from typing import Optional, List

from daft.table import Table
from daft.logical.schema import Schema
from daft import TimeUnit
from daft.io import IOConfig, S3Config
import pyarrow as pa

from deltacat import logs
from deltacat.utils.common import ReadKwargsProvider

from deltacat.types.media import ContentType, ContentEncoding
from deltacat.aws.constants import BOTO_MAX_RETRIES
from deltacat.utils.performance import timed_invocation


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def daft_s3_file_to_table(
    s3_url: str,
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **s3_client_kwargs,
):
    assert (
        content_type == ContentType.PARQUET.value
    ), "daft native reader currently only supports parquet, got {content_type}"

    assert (
        content_encoding == ContentEncoding.IDENTITY.value
    ), "daft native reader currently only supports identity encoding, got {content_encoding}"

    kwargs = {}
    if pa_read_func_kwargs_provider is not None:
        kwargs = pa_read_func_kwargs_provider(content_type, kwargs)

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )

    io_config = IOConfig(
        s3=S3Config(
            key_id=s3_client_kwargs.get("aws_access_key_id"),
            access_key=s3_client_kwargs.get("aws_secret_access_key"),
            session_token=s3_client_kwargs.get("aws_session_token"),
            num_tries=BOTO_MAX_RETRIES,
        )
    )

    table, latency = timed_invocation(
        Table.read_parquet,
        path=s3_url,
        columns=include_columns or column_names,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )

    logger.debug(f"Time to read S3 object from {s3_url} into daft table: {latency}s")

    if kwargs.get("schema") is not None:
        schema = kwargs["schema"]
        if include_columns is not None:
            schema = pa.schema([schema.field(col) for col in include_columns])
        daft_schema = Schema.from_pyarrow_schema(schema)
        return table.cast_to_schema(daft_schema).to_arrow()
    else:
        return table.to_arrow()
