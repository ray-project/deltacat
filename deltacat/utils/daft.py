import logging
from typing import Optional, List

from daft.table import Table
from daft.logical.schema import Schema
from daft import TimeUnit
from daft.io import IOConfig, S3Config
import pyarrow as pa

from deltacat import logs
from deltacat.utils.common import ReadKwargsProvider

from deltacat.types.media import ContentType

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def daft_s3_file_to_table(
    s3_url: str,
    content_type: str,
    content_encoding: str,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **s3_client_kwargs,
):
    assert (
        content_encoding == ContentType.PARQUET.value
    ), "daft native reader currently only supports parquet"
    kwargs = {}
    if pa_read_func_kwargs_provider is not None:
        kwargs = pa_read_func_kwargs_provider(content_type, kwargs)

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )

    table = Table.read_parquet(
        path=s3_url,
        columns=include_columns,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        io_config=IOConfig(
            s3=S3Config(
                key_id=s3_client_kwargs["aws_access_key_id"],
                access_key=s3_client_kwargs["aws_secret_access_key"],
                session_token=s3_client_kwargs["aws_session_token"],
                num_tries=25,
            )
        ),
    )
    logger.debug(f"Read S3 object from {s3_url} using daft")

    if "schema" in kwargs:
        schema = kwargs["schema"]
        if include_columns is not None:
            schema = pa.schema([schema.field(col) for col in include_columns])
        daft_schema = Schema.from_pyarrow_schema(schema)
        return table.cast_to_schema(daft_schema).to_arrow()
    else:
        return table.to_arrow()
