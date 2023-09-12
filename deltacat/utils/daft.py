import logging
from typing import Optional, List

from daft.table import read_parquet_into_pyarrow
from daft import TimeUnit
from daft.io import IOConfig, S3Config
import pyarrow as pa

from deltacat import logs
from deltacat.utils.common import ReadKwargsProvider

from deltacat.types.media import ContentType, ContentEncoding
from deltacat.aws.constants import BOTO_MAX_RETRIES, DAFT_MAX_S3_CONNECTIONS_PER_FILE
from deltacat.utils.performance import timed_invocation

from deltacat.types.partial_download import (
    PartialFileDownloadParams,
)


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


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

    row_groups = None
    if (
        partial_file_download_params is not None
        and partial_file_download_params.row_groups_to_download is not None
    ):
        row_groups = partial_file_download_params.row_groups_to_download

    io_config = IOConfig(
        s3=S3Config(
            key_id=s3_client_kwargs.get("aws_access_key_id"),
            access_key=s3_client_kwargs.get("aws_secret_access_key"),
            session_token=s3_client_kwargs.get("aws_session_token"),
            retry_mode="adaptive",
            num_tries=BOTO_MAX_RETRIES,
            max_connections=DAFT_MAX_S3_CONNECTIONS_PER_FILE,
        )
    )

    pa_table, latency = timed_invocation(
        read_parquet_into_pyarrow,
        path=s3_url,
        columns=include_columns or column_names,
        row_groups=row_groups,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        multithreaded_io=False,
    )

    logger.debug(f"Time to read S3 object from {s3_url} into daft table: {latency}s")

    if kwargs.get("schema") is not None:
        input_schema = kwargs["schema"]
        if include_columns is not None:
            input_schema = pa.schema([input_schema.field(col) for col in include_columns])
        elif column_names is not None:
            input_schema = pa.schema([input_schema.field(col) for col in column_names])
        input_schema_names = set(input_schema.names)

        # Perform casting of types to provided schema's types
        cast_to_schema = [
            input_schema.field(inferred_field.name)
            if inferred_field.name in input_schema_names
            else inferred_field
            for inferred_field in pa_table.schema
        ]
        casted_table = pa_table.cast(pa.schema(cast_to_schema))

        # Reorder and pad columns with a null column where necessary
        pa_table_column_names = set(casted_table.column_names)
        columns = []
        for name in input_schema.names:
            if name in pa_table_column_names:
                columns.append(casted_table[name])
            else:
                columns.append(pa.nulls(len(casted_table), type=input_schema.field(name).type))
        return pa.Table.from_arrays(
            columns, names=input_schema.names, metadata=pa_table.schema.metadata
        )
    else:
        return pa_table
