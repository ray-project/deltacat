# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import copy
import bz2
import gzip
import io
import logging
import posixpath
from functools import partial
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, Tuple
from datetime import datetime, date
from decimal import Decimal

import pyarrow as pa
import numpy as np
import pyarrow.compute as pc
import pyarrow.fs as pafs
from pyarrow.parquet import ParquetFile

from fsspec import AbstractFileSystem
from pyarrow import csv as pacsv
from pyarrow import feather as paf
from pyarrow import json as pajson
from pyarrow import parquet as papq
from pyarrow import orc as paorc
from ray.data.datasource import FilenameProvider

from deltacat import logs
from deltacat.exceptions import ContentTypeValidationError
from deltacat.types.media import (
    DELIMITED_TEXT_CONTENT_TYPES,
    TABULAR_CONTENT_TYPES,
    ContentEncoding,
    ContentType,
)
from deltacat.types.partial_download import (
    PartialFileDownloadParams,
    PartialParquetParameters,
)
from deltacat.utils.common import ContentTypeKwargsProvider, ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.schema import coerce_pyarrow_table_to_schema
from deltacat.utils.arguments import (
    sanitize_kwargs_to_callable,
    sanitize_kwargs_by_supported_kwargs,
)
from deltacat.utils.filesystem import resolve_path_and_filesystem
from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deltacat.storage.model.manifest import Manifest
    from deltacat.storage.model.delta import Delta


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

RAISE_ON_EMPTY_CSV_KWARG = "raise_on_empty_csv"
READER_TYPE_KWARG = "reader_type"
OVERRIDE_CONTENT_ENCODING_FOR_PARQUET_KWARG = "override_content_encoding_for_parquet"

"""
By default, round decimal values using half_to_even round mode when
rescaling a decimal to the given scale and precision in the schema would cause
data loss. Setting any non null value of this argument will result
in an error instead.
"""
RAISE_ON_DECIMAL_OVERFLOW = "raise_on_decimal_overflow"
# Note the maximum from https://arrow.apache.org/docs/python/generated/pyarrow.Decimal256Type.html#pyarrow.Decimal256Type
DECIMAL256_DEFAULT_SCALE = 38
DECIMAL256_MAX_PRECISION = 76
MAX_INT_BYTES = 2147483646


def _filter_schema_for_columns(schema: pa.Schema, columns: List[str]) -> pa.Schema:

    target_schema_fields = []

    for column_name in columns:
        index = schema.get_field_index(column_name)

        if index != -1:
            target_field = schema.field(index)
            target_schema_fields.append(target_field)

    target_schema = pa.schema(target_schema_fields, metadata=schema.metadata)

    return target_schema


def _extract_arrow_schema_from_read_csv_kwargs(kwargs: Dict[str, Any]) -> pa.Schema:
    schema = None
    if (
        "convert_options" in kwargs
        and kwargs["convert_options"].column_types is not None
    ):
        schema = kwargs["convert_options"].column_types
        if not isinstance(schema, pa.Schema):
            schema = pa.schema(schema)
        if kwargs["convert_options"].include_columns:
            schema = _filter_schema_for_columns(
                schema, kwargs["convert_options"].include_columns
            )
        elif (
            kwargs.get("read_options") is not None
            and kwargs["read_options"].column_names
        ):
            schema = _filter_schema_for_columns(
                schema, kwargs["read_options"].column_names
            )
    else:
        logger.debug(
            "Schema not specified in the kwargs."
            " Hence, schema could not be inferred from the empty CSV."
        )

    return schema


def _new_schema_with_replaced_fields(
    schema: pa.Schema, field_to_replace: Callable[[pa.Field], Optional[pa.Field]]
) -> pa.Schema:
    if schema is None:
        return None

    new_schema_fields = []
    for field in schema:
        new_field = field_to_replace(field)
        if new_field is not None:
            new_schema_fields.append(new_field)
        else:
            new_schema_fields.append(field)

    return pa.schema(new_schema_fields, metadata=schema.metadata)


def _read_csv_rounding_decimal_columns_to_fit_scale(
    schema: pa.Schema, reader_args: List[Any], reader_kwargs: Dict[str, Any]
) -> pa.Table:
    # Note: We read decimals as strings first because CSV
    # conversion to decimal256 isn't implemented as of pyarrow==12.0.1
    new_schema = _new_schema_with_replaced_fields(
        schema,
        lambda fld: (
            pa.field(fld.name, pa.string(), metadata=fld.metadata)
            if pa.types.is_decimal128(fld.type) or pa.types.is_decimal256(fld.type)
            else None
        ),
    )
    new_kwargs = sanitize_kwargs_by_supported_kwargs(
        ["read_options", "parse_options", "convert_options", "memory_pool"],
        reader_kwargs,
    )
    # Creating a shallow copy for efficiency
    new_convert_options = copy.copy(new_kwargs["convert_options"])
    new_convert_options.column_types = new_schema
    new_reader_kwargs = {**new_kwargs, "convert_options": new_convert_options}
    arrow_table = pacsv.read_csv(*reader_args, **new_reader_kwargs)

    for column_index, field in enumerate(schema):
        if pa.types.is_decimal128(field.type) or pa.types.is_decimal256(field.type):
            column_array = arrow_table[field.name]
            # We always cast to decimal256 to accomodate fixed scale of 38
            cast_to_type = pa.decimal256(
                DECIMAL256_MAX_PRECISION, DECIMAL256_DEFAULT_SCALE
            )
            casted_decimal_array = pc.cast(column_array, cast_to_type)
            # Note that scale can be negative
            rounded_column_array = pc.round(
                casted_decimal_array, ndigits=field.type.scale
            )
            final_decimal_array = pc.cast(rounded_column_array, field.type)
            arrow_table = arrow_table.set_column(
                column_index,
                field,
                final_decimal_array,
            )
            logger.debug(
                f"Rounded decimal column: {field.name} to {field.type.scale} scale and"
                f" {field.type.precision} precision"
            )

    return arrow_table


def pyarrow_read_csv_default(*args, **kwargs):
    new_kwargs = sanitize_kwargs_by_supported_kwargs(
        ["read_options", "parse_options", "convert_options", "memory_pool"], kwargs
    )

    try:
        return pacsv.read_csv(*args, **new_kwargs)
    except pa.lib.ArrowInvalid as e:
        error_str = e.__str__()
        schema = _extract_arrow_schema_from_read_csv_kwargs(kwargs)

        if error_str == "Empty CSV file" and not kwargs.get(RAISE_ON_EMPTY_CSV_KWARG):
            logger.debug(f"Read CSV empty schema being used: {schema}")
            return pa.Table.from_pylist([], schema=schema)
        if not kwargs.get(RAISE_ON_DECIMAL_OVERFLOW):
            # Note, this logic requires expensive casting. To prevent downgrading performance
            # for happy path reads, we are handling this case in response to an error.
            logger.warning(
                "Rescaling Decimal to the given scale in the schema. "
                f"Original error: {error_str}"
            )

            if schema is not None and "convert_options" in kwargs:
                if (
                    "Rescaling Decimal" in error_str
                    and "value would cause data loss" in error_str
                ):
                    logger.debug(f"Checking if the file: {args[0]}...")
                    # Since we are re-reading the file, we have to seek to beginning
                    if isinstance(args[0], io.IOBase) and args[0].seekable():
                        logger.debug(f"Seeking to the beginning of the file {args[0]}")
                        args[0].seek(0)
                    return _read_csv_rounding_decimal_columns_to_fit_scale(
                        schema=schema, reader_args=args, reader_kwargs=kwargs
                    )
            else:
                logger.debug(
                    "Schema is None when trying to adjust decimal values. "
                    "Hence, bubbling up exception..."
                )

        raise e


# TODO(pdames): Remove deprecated S3-only readers.
def read_csv(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pa.Table:
    # Filter out DeltaCAT-specific parameters that PyArrow doesn't understand
    from deltacat.types.tables import _filter_kwargs_for_external_readers

    pyarrow_kwargs = _filter_kwargs_for_external_readers(read_kwargs)
    # TODO(pdames): Merge in decimal256 support from pure S3 path reader.

    # Check if compression is already indicated by file path
    should_decompress = path.endswith(".gz")

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
            # Handle decompression - avoid double decompression for PyArrow filesystem
            if should_decompress:
                # PyArrow filesystem already handles .gz decompression automatically
                return pacsv.read_csv(f, **pyarrow_kwargs)
            else:
                # Apply explicit decompression if needed
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    return pacsv.read_csv(input_file, **pyarrow_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle decompression - apply explicit decompression for fsspec
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return pacsv.read_csv(input_file, **pyarrow_kwargs)


def read_feather(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pa.Table:
    # Filter out DeltaCAT-specific parameters that PyArrow doesn't understand
    from deltacat.types.tables import _filter_kwargs_for_external_readers

    pyarrow_kwargs = _filter_kwargs_for_external_readers(read_kwargs)
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return paf.read_table(input_file, **pyarrow_kwargs)
    else:
        # fsspec AbstractFileSystem - Feather requires seekable files
        # For local files, we can use the file path directly
        if hasattr(filesystem, "protocol") and filesystem.protocol == "file":
            if content_encoding != ContentEncoding.IDENTITY.value:
                # For compressed files, decompress to a temporary file
                import tempfile
                import shutil

                with filesystem.open(path, "rb", **fs_open_kwargs) as f:
                    input_file_init = ENCODING_TO_FILE_INIT.get(
                        content_encoding, lambda x: x
                    )
                    with input_file_init(f) as input_file:
                        # Create temporary file to hold decompressed data
                        with tempfile.NamedTemporaryFile() as temp_file:
                            shutil.copyfileobj(input_file, temp_file)
                            temp_file.flush()
                            return paf.read_table(temp_file.name, **read_kwargs)
            else:
                # No compression, can read directly from file path
                return paf.read_table(path, **pyarrow_kwargs)
        else:
            # For non-local filesystems, always read from temporary file
            import tempfile
            import shutil

            with filesystem.open(path, "rb", **fs_open_kwargs) as f:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Create temporary file to hold data
                    with tempfile.NamedTemporaryFile() as temp_file:
                        shutil.copyfileobj(input_file, temp_file)
                        temp_file.flush()
                        return paf.read_table(temp_file.name, **read_kwargs)


def read_json(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pa.Table:
    # Filter out DeltaCAT-specific parameters that PyArrow doesn't understand
    from deltacat.types.tables import _filter_kwargs_for_external_readers

    pyarrow_kwargs = _filter_kwargs_for_external_readers(read_kwargs)
    # Check if decompression is already indicated by file path
    should_decompress = path.endswith(".gz")

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
            # Handle decompression - avoid double decompression for PyArrow filesystem
            if should_decompress:
                # PyArrow filesystem already handles .gz decompression automatically
                return pajson.read_json(f, **pyarrow_kwargs)
            else:
                # Apply explicit decompression if needed
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    return pajson.read_json(input_file, **pyarrow_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle decompression - apply explicit decompression for fsspec
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return pajson.read_json(input_file, **pyarrow_kwargs)


def read_orc(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pa.Table:
    # Filter out DeltaCAT-specific parameters that PyArrow doesn't understand
    from deltacat.types.tables import _filter_kwargs_for_external_readers

    pyarrow_kwargs = _filter_kwargs_for_external_readers(read_kwargs)
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return paorc.read_table(input_file, **pyarrow_kwargs)
    else:
        # fsspec AbstractFileSystem - ORC requires seekable files, so handle compression differently
        if content_encoding != ContentEncoding.IDENTITY.value:
            # For compressed files with fsspec, we need to decompress to a temporary file
            # since ORC requires seekable streams
            import tempfile
            import shutil

            with filesystem.open(path, "rb", **fs_open_kwargs) as f:
                input_file_init = ENCODING_TO_FILE_INIT.get(
                    content_encoding, lambda x: x
                )
                with input_file_init(f) as input_file:
                    # Create temporary file to hold decompressed data
                    with tempfile.NamedTemporaryFile() as temp_file:
                        shutil.copyfileobj(input_file, temp_file)
                        temp_file.flush()
                        return paorc.read_table(temp_file.name, **pyarrow_kwargs)
        else:
            # No compression, can read directly
            with filesystem.open(path, "rb", **fs_open_kwargs) as f:
                return paorc.read_table(f, **pyarrow_kwargs)


def read_parquet(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pa.Table:
    # Convert DeltaCAT Schema to PyArrow Schema if present
    if "schema" in read_kwargs:
        from deltacat.storage.model.schema import Schema as DeltaCATSchema

        schema = read_kwargs["schema"]
        if isinstance(schema, DeltaCATSchema):
            read_kwargs["schema"] = schema.arrow

    # Filter out DeltaCAT-specific parameters that PyArrow doesn't understand
    # Use local import to avoid circular dependency
    from deltacat.types.tables import _filter_kwargs_for_external_readers

    pyarrow_kwargs = _filter_kwargs_for_external_readers(read_kwargs)
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_file(path, **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return papq.read_table(input_file, **pyarrow_kwargs)
    else:
        # fsspec AbstractFileSystem
        with filesystem.open(path, "rb", **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                return papq.read_table(input_file, **pyarrow_kwargs)


def read_avro(
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    content_encoding: str = ContentEncoding.IDENTITY.value,
    **read_kwargs,
) -> pa.Table:
    # Filter out DeltaCAT-specific parameters that Polars doesn't understand
    from deltacat.types.tables import _filter_kwargs_for_external_readers

    polars_kwargs = _filter_kwargs_for_external_readers(read_kwargs)
    """
    Read an Avro file using polars and convert to PyArrow.
    """
    import polars as pl

    # If path is a file-like object, read directly
    if hasattr(path, "read"):
        pl_df = pl.read_avro(path, **polars_kwargs)
        return pl_df.to_arrow()

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_stream(path, **fs_open_kwargs) as f:
            # Handle compression
            input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
            with input_file_init(f) as input_file:
                pl_df = pl.read_avro(input_file, **polars_kwargs)
                return pl_df.to_arrow()
    with filesystem.open(path, "rb", **fs_open_kwargs) as f:
        input_file_init = ENCODING_TO_FILE_INIT.get(content_encoding, lambda x: x)
        with input_file_init(f) as input_file:
            pl_df = pl.read_avro(input_file, **polars_kwargs)
            return pl_df.to_arrow()


def pyarrow_read_csv(*args, **kwargs) -> pa.Table:
    schema = _extract_arrow_schema_from_read_csv_kwargs(kwargs)

    # CSV conversion to decimal256 isn't supported as of pyarrow=12.0.1
    # Below ensures decimal256 is casted properly.
    schema_includes_decimal256 = (
        (True if any([pa.types.is_decimal256(x.type) for x in schema]) else False)
        if schema is not None
        else None
    )
    if schema_includes_decimal256 and not kwargs.get(RAISE_ON_DECIMAL_OVERFLOW):
        # falling back to expensive method of reading CSV
        return _read_csv_rounding_decimal_columns_to_fit_scale(
            schema, reader_args=args, reader_kwargs=kwargs
        )
    else:
        return pyarrow_read_csv_default(*args, **kwargs)


CONTENT_TYPE_TO_PA_S3_READ_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: pyarrow_read_csv,
    ContentType.TSV.value: pyarrow_read_csv,
    ContentType.CSV.value: pyarrow_read_csv,
    ContentType.PSV.value: pyarrow_read_csv,
    ContentType.PARQUET.value: papq.read_table,
    ContentType.FEATHER.value: paf.read_table,
    ContentType.JSON.value: pajson.read_json,
    ContentType.ORC.value: paorc.read_table,
    ContentType.AVRO.value: read_avro,
}


CONTENT_TYPE_TO_READ_FN: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: read_csv,
    ContentType.TSV.value: read_csv,
    ContentType.CSV.value: read_csv,
    ContentType.PSV.value: read_csv,
    ContentType.PARQUET.value: read_parquet,
    ContentType.FEATHER.value: read_feather,
    ContentType.JSON.value: read_json,
    ContentType.ORC.value: read_orc,
    ContentType.AVRO.value: read_avro,
}


def write_feather(
    table: pa.Table,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            paf.write_feather(table, f, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            paf.write_feather(table, f, **write_kwargs)


def write_csv(
    table: pa.Table,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if write_kwargs.get("write_options") is None:
        # column names are kept in table metadata, so omit header
        write_kwargs["write_options"] = pacsv.WriteOptions(include_header=False)

    # Check if the path already indicates compression to avoid double compression
    should_compress = path.endswith(".gz")

    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            if should_compress:
                # Path ends with .gz, PyArrow filesystem automatically compresses, no need for additional compression
                pacsv.write_csv(table, f, **write_kwargs)
            else:
                # No compression indicated, write uncompressed
                pacsv.write_csv(table, f, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            if should_compress:
                # For fsspec filesystems, we need to apply compression explicitly
                with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
                    pacsv.write_csv(table, out, **write_kwargs)
            else:
                # No compression indicated, write uncompressed
                pacsv.write_csv(table, f, **write_kwargs)


def write_orc(
    table: pa.Table,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            paorc.write_table(table, f, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            paorc.write_table(table, f, **write_kwargs)


def write_parquet(
    table: pa.Table,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_output_stream(path, **fs_open_kwargs) as f:
            papq.write_table(table, f, **write_kwargs)
    else:
        with filesystem.open(path, "wb", **fs_open_kwargs) as f:
            papq.write_table(table, f, **write_kwargs)


def write_json(
    table: pa.Table,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    """
    Write a PyArrow Table to a JSON file by delegating to polars implementation.
    """
    import polars as pl
    from deltacat.utils.polars import write_json as polars_write_json

    # Convert PyArrow Table to polars DataFrame
    pl_df = pl.from_arrow(table)

    # Delegate to polars write_json implementation with GZIP compression
    polars_write_json(
        pl_df,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        **write_kwargs,
    )


def write_avro(
    table: pa.Table,
    path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    fs_open_kwargs: Dict[str, any] = {},
    **write_kwargs,
) -> None:
    """
    Write a PyArrow Table to an AVRO file by delegating to polars implementation.
    """
    import polars as pl
    from deltacat.utils.polars import write_avro as polars_write_avro

    # Convert PyArrow Table to polars DataFrame
    pl_df = pl.from_arrow(table)

    # Delegate to polars write_avro implementation
    polars_write_avro(
        pl_df,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        **write_kwargs,
    )


CONTENT_TYPE_TO_PA_WRITE_FUNC: Dict[str, Callable] = {
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


def content_type_to_writer_kwargs(content_type: str) -> Dict[str, Any]:
    """
    Returns writer kwargs for the given content type when writing with pyarrow.
    """
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "write_options": pacsv.WriteOptions(
                delimiter="\t",
                include_header=False,
                quoting_style="none",
            )
        }
    if content_type == ContentType.TSV.value:
        return {
            "write_options": pacsv.WriteOptions(
                include_header=False,
                delimiter="\t",
                quoting_style="needed",
            )
        }
    if content_type == ContentType.CSV.value:
        return {
            "write_options": pacsv.WriteOptions(
                include_header=False,
                delimiter=",",
                quoting_style="needed",
            )
        }
    if content_type == ContentType.PSV.value:
        return {
            "write_options": pacsv.WriteOptions(
                include_header=False,
                delimiter="|",
                quoting_style="needed",
            )
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


def content_type_to_reader_kwargs(content_type: str) -> Dict[str, Any]:
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "parse_options": pacsv.ParseOptions(delimiter="\t", quote_char=False),
            "convert_options": pacsv.ConvertOptions(
                null_values=[""],  # pyarrow defaults are ["", "NULL", "null"]
                strings_can_be_null=True,
            ),
        }
    if content_type == ContentType.TSV.value:
        return {"parse_options": pacsv.ParseOptions(delimiter="\t")}
    if content_type == ContentType.CSV.value:
        return {"parse_options": pacsv.ParseOptions(delimiter=",")}
    if content_type == ContentType.PSV.value:
        return {"parse_options": pacsv.ParseOptions(delimiter="|")}
    if content_type in {
        ContentType.PARQUET.value,
        ContentType.FEATHER.value,
        ContentType.JSON.value,
        ContentType.ORC.value,
        ContentType.AVRO.value,
    }:
        return {}
    raise ValueError(f"Unsupported content type: {content_type}")


# TODO (pdames): add deflate and snappy
ENCODING_TO_FILE_INIT: Dict[str, Callable] = {
    ContentEncoding.GZIP.value: partial(gzip.open, mode="rb"),
    ContentEncoding.BZIP2.value: partial(bz2.open, mode="rb"),
    ContentEncoding.IDENTITY.value: lambda file_path: file_path,
}


def slice_table(
    table: pa.Table,
    max_len: Optional[int],
) -> List[pa.Table]:
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


def append_column_to_table(
    table: pa.Table,
    column_name: str,
    column_value: Any,
) -> pa.Table:
    num_rows = table.num_rows
    column_array = pa.array([column_value] * num_rows)
    return table.append_column(column_name, column_array)


def select_columns(
    table: pa.Table,
    column_names: List[str],
) -> pa.Table:
    return table.select(column_names)


class ReadKwargsProviderPyArrowCsvPureUtf8(ContentTypeKwargsProvider):
    """ReadKwargsProvider impl that reads columns of delimited text files
    as UTF-8 strings (i.e. disables type inference). Useful for ensuring
    lossless reads of UTF-8 delimited text datasets and improving read
    performance in cases where type casting is not required."""

    def __init__(self, include_columns: Optional[Iterable[str]] = None):
        self.include_columns = include_columns

    def _get_kwargs(self, content_type: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if content_type in DELIMITED_TEXT_CONTENT_TYPES:
            convert_options: pacsv.ConvertOptions = kwargs.get("convert_options")
            if convert_options is None:
                convert_options = pacsv.ConvertOptions()
            # read only the included columns as strings?
            column_names = (
                self.include_columns
                if self.include_columns
                else convert_options.include_columns
            )
            if not column_names:
                # read all columns as strings?
                read_options: pacsv.ReadOptions = kwargs.get("read_options")
                if read_options and read_options.column_names:
                    column_names = read_options.column_names
                else:
                    raise ValueError("No column names found!")
            convert_options.column_types = {
                column_name: pa.string() for column_name in column_names
            }
            kwargs["convert_options"] = convert_options
        return kwargs


class ReadKwargsProviderPyArrowSchemaOverride(ContentTypeKwargsProvider):
    """ReadKwargsProvider impl that explicitly maps column names to column types when
    loading dataset files into a PyArrow table. Disables the default type inference
    behavior on the defined columns."""

    def __init__(
        self,
        schema: Optional[pa.Schema] = None,
        pq_coerce_int96_timestamp_unit: Optional[str] = None,
        parquet_reader_type: Optional[str] = None,
        file_read_timeout_ms: Optional[int] = None,
    ):
        """

        Args:
            schema: The schema to use for reading the dataset.
                If unspecified, the schema will be inferred from the source.
            pq_coerce_int96_timestamp_unit: When reading from parquet files, cast timestamps that are stored in INT96
                format to a particular resolution (e.g. 'ms').

        """
        self.schema = schema
        self.pq_coerce_int96_timestamp_unit = pq_coerce_int96_timestamp_unit
        self.parquet_reader_type = parquet_reader_type
        self.file_read_timeout_ms = file_read_timeout_ms

    def _get_kwargs(self, content_type: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if content_type in DELIMITED_TEXT_CONTENT_TYPES:
            convert_options = kwargs.get("convert_options", pacsv.ConvertOptions())
            if self.schema:
                convert_options.column_types = self.schema
            kwargs["convert_options"] = convert_options
        elif content_type == ContentType.PARQUET:
            # kwargs here are passed into `pyarrow.parquet.read_table`.
            # Only supported in PyArrow 8.0.0+
            if self.schema:
                kwargs["schema"] = self.schema

            # Coerce deprecated int96 timestamp to millisecond if unspecified
            if self.pq_coerce_int96_timestamp_unit is not None:
                kwargs[
                    "coerce_int96_timestamp_unit"
                ] = self.pq_coerce_int96_timestamp_unit

            if self.parquet_reader_type:
                kwargs["reader_type"] = self.parquet_reader_type
            else:
                kwargs["reader_type"] = "daft"

            kwargs["file_timeout_ms"] = self.file_read_timeout_ms

        return kwargs


def _add_column_kwargs(
    content_type: str,
    column_names: Optional[List[str]],
    include_columns: Optional[List[str]],
    kwargs: Dict[str, Any],
):

    if content_type in DELIMITED_TEXT_CONTENT_TYPES:
        read_options: pacsv.ReadOptions = kwargs.get("read_options")
        if read_options is None:
            read_options = pacsv.ReadOptions()
        if column_names:
            read_options.column_names = column_names
        else:
            read_options.autogenerate_column_names = True
        kwargs["read_options"] = read_options
        convert_options: pacsv.ConvertOptions = kwargs.get("convert_options")
        if convert_options is None:
            convert_options = pacsv.ConvertOptions()
        if include_columns:
            convert_options.include_columns = include_columns
        kwargs["convert_options"] = convert_options
    else:
        if content_type in TABULAR_CONTENT_TYPES:
            kwargs["columns"] = include_columns
        else:
            if include_columns:
                logger.warning(
                    f"Ignoring request to include columns {include_columns} "
                    f"for non-tabular content type {content_type}"
                )


def partial_parquet_file_to_table(
    path: str,
    content_type: str,
    content_encoding: str,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialParquetParameters] = None,
    **kwargs,
) -> pa.Table:

    assert (
        partial_file_download_params is not None
    ), "Partial parquet params must not be None"
    assert (
        partial_file_download_params.row_groups_to_download is not None
    ), "No row groups to download"

    # Resolve filesystem and path
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)

    pq_file = file_to_parquet(
        path=path,
        content_type=content_type,
        content_encoding=content_encoding,
        filesystem=filesystem,
        partial_file_download_params=partial_file_download_params,
        pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
        **kwargs,
    )

    table, latency = timed_invocation(
        pq_file.read_row_groups,
        partial_file_download_params.row_groups_to_download,
        columns=include_columns or column_names,
    )

    logger.debug(f"Successfully read from path={path} in {latency}s")

    kwargs = {}

    if pa_read_func_kwargs_provider:
        kwargs = pa_read_func_kwargs_provider(content_type, kwargs)

    # Note: ordering is consistent with the `input_schema` if provided
    if kwargs.get("schema") is not None:
        input_schema = kwargs.get("schema")
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
        coerced_table, coerce_latency = timed_invocation(
            coerce_pyarrow_table_to_schema, table, input_schema
        )

        logger.debug(
            f"Coercing the PyArrow table of len {len(coerced_table)} "
            f"into passed schema took {coerce_latency}s"
        )

        return coerced_table

    return table


def table_size(table: pa.Table) -> int:
    return table.nbytes


def parquet_file_size(table: papq.ParquetFile) -> int:
    return table.metadata.serialized_size


def table_to_file(
    table: pa.Table,
    base_path: str,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[Callable, FilenameProvider],
    content_type: str = ContentType.PARQUET.value,
    schema: Optional[pa.Schema] = None,
    **kwargs,
) -> None:
    """
    Writes the given Pyarrow Table to a file.

    Args:
        table: The PyArrow table to write
        base_path: Base path to write to
        file_system: Optional filesystem to use
        block_path_provider: Provider for block path generation
        content_type: Content type for the output file
        schema: Optional schema (for compatibility with explicit schema parameter pattern)
        kwargs: Keyword arguments passed to the PyArrow write function
    """
    writer = CONTENT_TYPE_TO_PA_WRITE_FUNC.get(content_type)
    if not writer:
        raise NotImplementedError(
            f"Pyarrow writer for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{CONTENT_TYPE_TO_PA_WRITE_FUNC.keys}"
        )
    filename = block_path_provider(base_path)
    path = posixpath.join(base_path, filename)
    writer_kwargs = content_type_to_writer_kwargs(content_type)
    writer_kwargs.update(kwargs)
    logger.debug(f"Writing table: {table} with kwargs: {writer_kwargs} to path: {path}")
    writer(table, path, filesystem=filesystem, **writer_kwargs)


class RecordBatchTables:
    def __init__(self, batch_size: int):
        """
        Data structure for maintaining a batched list of tables, where each batched table has
        a record count of some multiple of the specified record batch size.

        Remaining records are stored in a separate list of tables.

        Args:
            batch_size: Minimum record count per table to batch by. Batched tables are
             guaranteed to have a record count multiple of the batch_size.
        """
        self._batched_tables: List[pa.Table] = []
        self._batched_record_count: int = 0
        self._remaining_tables: List[pa.Table] = []
        self._remaining_record_count: int = 0
        self._batch_size: int = batch_size

    def append(self, table: pa.Table) -> None:
        """
        Appends a table for batching.

        Table record counts are added to any previous remaining record count.
        If the new remainder record count meets or exceeds the configured batch size record count,
        the remainder will be shifted over to the list of batched tables in FIFO order via table slicing.
        Batched tables will always have a record count of some multiple of the configured batch size.

        Record ordering is preserved from input tables whenever tables are shifted from the remainder
        over to the batched list. Records from Table A will always precede records from Table B,
        if Table A was appended before Table B. Records from the batched list will always precede records
        from the remainders.

        Ex:
            bt = RecordBatchTables(8)
            col1 = pa.array([i for i in range(10)])
            test_table = pa.Table.from_arrays([col1], names=["col1"])
            bt.append(test_table)

            print(bt.batched_records)  # 8
            print(bt.batched)  # [0, 1, 2, 3, 4, 5, 6, 7]
            print(bt.remaining_records)  # 2
            print(bt.remaining)  # [8, 9]

        Args:
            table: Input table to add

        """
        if self._remaining_tables:
            if self._remaining_record_count + len(table) < self._batch_size:
                self._remaining_tables.append(table)
                self._remaining_record_count += len(table)
                return

            records_to_fit = self._batch_size - self._remaining_record_count
            fitted_table = table.slice(length=records_to_fit)
            self._remaining_tables.append(fitted_table)
            self._remaining_record_count += len(fitted_table)
            table = table.slice(offset=records_to_fit)

        record_count = len(table)
        record_multiplier, records_leftover = (
            record_count // self._batch_size,
            record_count % self._batch_size,
        )

        if record_multiplier > 0:
            batched_table = table.slice(length=record_multiplier * self._batch_size)
            # Add to remainder tables to preserve record ordering
            self._remaining_tables.append(batched_table)
            self._remaining_record_count += len(batched_table)

        if self._remaining_tables:
            self._shift_remaining_to_new_batch()

        if records_leftover > 0:
            leftover_table = table.slice(offset=record_multiplier * self._batch_size)
            self._remaining_tables.append(leftover_table)
            self._remaining_record_count += len(leftover_table)

    def _shift_remaining_to_new_batch(self) -> None:
        new_batch = pa.concat_tables(self._remaining_tables)
        self._batched_tables.append(new_batch)
        self._batched_record_count += self._remaining_record_count
        self.clear_remaining()

    @staticmethod
    def from_tables(tables: List[pa.Table], batch_size: int) -> RecordBatchTables:
        """
        Static factory for generating batched tables and remainders given a list of input tables.

        Args:
            tables: A list of input tables with various record counts
            batch_size: Minimum record count per table to batch by. Batched tables are
             guaranteed to have a record count multiple of the batch_size.

        Returns: A batched tables object

        """
        rbt = RecordBatchTables(batch_size)
        for table in tables:
            rbt.append(table)
        return rbt

    @property
    def batched(self) -> List[pa.Table]:
        """
        List of tables batched and ready for processing.
        Each table has N records, where N records are some multiple of the configured records batch size.

        For example, if the configured batch size is 5, then a list of batched tables
        could have the following record counts: [60, 5, 30, 10]

        Returns: a list of batched tables

        """
        return self._batched_tables

    @property
    def batched_record_count(self) -> int:
        """
        The number of total records from the batched list.

        Returns: batched record count

        """
        return self._batched_record_count

    @property
    def remaining(self) -> List[pa.Table]:
        """
        List of tables carried over from table slicing during the batching operation.
        The sum of all record counts in the remaining tables is guaranteed to be less than the configured batch size.

        Returns: a list of remaining tables

        """
        return self._remaining_tables

    @property
    def remaining_record_count(self) -> int:
        """
        The number of total records from the remaining tables list.

        Returns: remaining record count

        """
        return self._remaining_record_count

    @property
    def batch_size(self) -> int:
        """
        The configured batch size.

        Returns: batch size

        """
        return self._batch_size

    def has_batches(self) -> bool:
        """
        Checks if there are any currently batched tables ready for processing.

        Returns: true if batched records exist, otherwise false

        """
        return self._batched_record_count > 0

    def has_remaining(self) -> bool:
        """
        Checks if any remaining tables exist after batching.

        Returns: true if remaining records exist, otherwise false

        """
        return self._remaining_record_count > 0

    def evict(self) -> List[pa.Table]:
        """
        Evicts all batched tables from this object and returns them.

        Returns: a list of batched tables

        """
        evicted_tables = [*self.batched]
        self.clear_batches()
        return evicted_tables

    def clear_batches(self) -> None:
        """
        Removes all batched tables and resets batched records.

        """
        self._batched_tables.clear()
        self._batched_record_count = 0

    def clear_remaining(self) -> None:
        """
        Removes all remaining tables and resets remaining records.

        """
        self._remaining_tables.clear()
        self._remaining_record_count = 0


@lru_cache(maxsize=1)
def _int_max_string_len() -> int:
    PA_UINT64_MAX_STR_BYTES = pc.binary_length(
        pc.cast(pa.scalar(2**64 - 1, type=pa.uint64()), pa.string())
    ).as_py()
    PA_INT64_MAX_STR_BYTES = pc.binary_length(
        pc.cast(pa.scalar(-(2**63), type=pa.int64()), pa.string())
    ).as_py()
    return max(PA_UINT64_MAX_STR_BYTES, PA_INT64_MAX_STR_BYTES)


@lru_cache(maxsize=1)
def _float_max_string_len() -> int:
    PA_POS_FLOAT64_MAX_STR_BYTES = pc.binary_length(
        pc.cast(pa.scalar(np.finfo(np.float64).max, type=pa.float64()), pa.string())
    ).as_py()
    PA_NEG_FLOAT64_MAX_STR_BYTES = pc.binary_length(
        pc.cast(pa.scalar(np.finfo(np.float64).min, type=pa.float64()), pa.string())
    ).as_py()
    return max(PA_POS_FLOAT64_MAX_STR_BYTES, PA_NEG_FLOAT64_MAX_STR_BYTES)


def _max_decimal128_string_len():
    return 40  # "-" + 38 digits + decimal


def _max_decimal256_string_len():
    return 78  # "-" + 76 digits + decimal


def sliced_string_cast(array: pa.ChunkedArray) -> pa.ChunkedArray:
    """performs slicing of a pyarrow array prior casting to a string.
    This prevents a pyarrow from allocating too large of an array causing a failure.
    Issue: https://github.com/apache/arrow/issues/38835
    TODO: deprecate this function when pyarrow performs proper ChunkedArray -> ChunkedArray casting
    """
    dtype = array.type
    max_str_len = None
    if pa.types.is_integer(dtype):
        max_str_len = _int_max_string_len()
    elif pa.types.is_floating(dtype):
        max_str_len = _float_max_string_len()
    elif pa.types.is_decimal128(dtype):
        max_str_len = _max_decimal128_string_len()
    elif pa.types.is_decimal256(dtype):
        max_str_len = _max_decimal256_string_len()

    if max_str_len is not None:
        max_elems_per_chunk = MAX_INT_BYTES // (2 * max_str_len)  # safety factor of 2
        all_chunks = []
        for chunk in array.chunks:
            if len(chunk) < max_elems_per_chunk:
                all_chunks.append(chunk)
            else:
                curr_pos = 0
                total_len = len(chunk)
                while curr_pos < total_len:
                    sliced = chunk.slice(curr_pos, max_elems_per_chunk)
                    curr_pos += len(sliced)
                    all_chunks.append(sliced)
        array = pa.chunked_array(all_chunks, type=dtype)

    return pc.cast(array, pa.string())


def file_to_table(
    path: str,
    content_type: str,
    content_encoding: str = ContentEncoding.IDENTITY.value,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    fs_open_kwargs: Dict[str, Any] = {},
    **kwargs,
) -> pa.Table:
    """
    Read a file into a PyArrow Table using any filesystem.

    Args:
        path: The file path to read
        content_type: The content type of the file (e.g., ContentType.CSV.value)
        content_encoding: The content encoding (default: IDENTITY)
        filesystem: The filesystem to use (if None, will be inferred from path)
        column_names: Optional column names to assign
        include_columns: Optional columns to include in the result
        pa_read_func_kwargs_provider: Optional kwargs provider for customization
        fs_open_kwargs: Optional kwargs for filesystem open operations
        **kwargs: Additional kwargs passed to the reader function

    Returns:
        pa.Table: The loaded PyArrow Table
    """
    logger.debug(
        f"Reading {path} to PyArrow. Content type: {content_type}. "
        f"Encoding: {content_encoding}"
    )

    if (
        content_type == ContentType.PARQUET.value
        and content_encoding == ContentEncoding.IDENTITY.value
        and not filesystem
        and path.startswith("s3://")
    ):
        # Use optimized partial parquet reader for s3 if possible
        logger.debug(
            f"Reading {path} using parquet reader for encoding={content_encoding} "
            f"and content_type={content_type}"
        )

        parquet_reader_func = None
        if kwargs.get(READER_TYPE_KWARG, "daft") == "daft":
            from deltacat.utils.daft import daft_file_to_pyarrow_table

            parquet_reader_func = daft_file_to_pyarrow_table
        elif partial_file_download_params and isinstance(
            partial_file_download_params, PartialParquetParameters
        ):
            parquet_reader_func = partial_parquet_file_to_table

        if parquet_reader_func is not None:
            return parquet_reader_func(
                path=path,
                content_type=content_type,
                content_encoding=content_encoding,
                filesystem=filesystem,
                column_names=column_names,
                include_columns=include_columns,
                pa_read_func_kwargs_provider=pa_read_func_kwargs_provider,
                partial_file_download_params=partial_file_download_params,
                **kwargs,
            )

    if READER_TYPE_KWARG in kwargs:
        kwargs.pop(READER_TYPE_KWARG)

    pa_read_func = CONTENT_TYPE_TO_READ_FN.get(content_type)
    if not pa_read_func:
        raise NotImplementedError(
            f"PyArrow reader for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{list(CONTENT_TYPE_TO_READ_FN.keys())}"
        )

    reader_kwargs = content_type_to_reader_kwargs(content_type)

    _add_column_kwargs(content_type, column_names, include_columns, reader_kwargs)

    # Merge with provided kwargs
    reader_kwargs.update(kwargs)

    if pa_read_func_kwargs_provider:
        reader_kwargs = pa_read_func_kwargs_provider(content_type, reader_kwargs)

    logger.debug(f"Reading {path} via {pa_read_func} with kwargs: {reader_kwargs}")

    table, latency = timed_invocation(
        pa_read_func,
        path,
        filesystem=filesystem,
        fs_open_kwargs=fs_open_kwargs,
        content_encoding=content_encoding,
        **reader_kwargs,
    )
    logger.debug(f"Time to read {path} into PyArrow Table: {latency}s")
    return table


def file_to_parquet(
    path: str,
    content_type: str = ContentType.PARQUET.value,
    content_encoding: str = ContentEncoding.IDENTITY.value,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    fs_open_kwargs: Dict[str, Any] = {},
    **kwargs,
) -> ParquetFile:
    """
    Read a file into a PyArrow ParquetFile using any filesystem.

    It returns a ParquetFile object which provides metadata access and lazy loading.

    Args:
        path: The file path to read
        content_type: The content type (must be PARQUET, default: PARQUET)
        content_encoding: The content encoding (must be IDENTITY, default: IDENTITY)
        filesystem: The filesystem to use (if None, will be inferred from path)
        column_names: Optional column names (unused for ParquetFile but kept for API consistency)
        include_columns: Optional columns (unused for ParquetFile but kept for API consistency)
        pa_read_func_kwargs_provider: Optional kwargs provider for customization
        fs_open_kwargs: Optional kwargs for filesystem open operations
        **kwargs: Additional kwargs passed to ParquetFile constructor

    Returns:
        ParquetFile: The ParquetFile object for lazy loading and metadata access

    Raises:
        ContentTypeValidationError: If content_type is not PARQUET or content_encoding is not IDENTITY
    """
    logger.debug(
        f"Reading {path} to PyArrow ParquetFile. "
        f"Content type: {content_type}. Encoding: {content_encoding}"
    )
    # Validate content type and encoding
    if (
        content_type != ContentType.PARQUET.value
        or content_encoding != ContentEncoding.IDENTITY.value
    ):
        raise ContentTypeValidationError(
            f"File with content type: {content_type} and content encoding: {content_encoding} "
            "cannot be read into pyarrow.parquet.ParquetFile"
        )

    # Resolve filesystem and path
    if not filesystem or isinstance(filesystem, pafs.FileSystem):
        path, filesystem = resolve_path_and_filesystem(path, filesystem)

    # Build kwargs for ParquetFile constructor
    parquet_kwargs = {}

    # Add filesystem to kwargs if we have one
    if filesystem:
        parquet_kwargs["filesystem"] = filesystem

    # Apply kwargs provider if provided
    if pa_read_func_kwargs_provider:
        parquet_kwargs = pa_read_func_kwargs_provider(content_type, parquet_kwargs)

    # Merge with provided kwargs
    parquet_kwargs.update(kwargs)

    logger.debug(f"Pre-sanitize kwargs for {path}: {parquet_kwargs}")

    # Sanitize kwargs to only include those supported by ParquetFile.__init__
    parquet_kwargs = sanitize_kwargs_to_callable(ParquetFile.__init__, parquet_kwargs)

    logger.debug(
        f"Reading the file from {path} into ParquetFile with kwargs: {parquet_kwargs}"
    )

    def _create_parquet_file():
        return ParquetFile(path, **parquet_kwargs)

    pq_file, latency = timed_invocation(_create_parquet_file)

    logger.debug(f"Time to get {path} into parquet file: {latency}s")

    return pq_file


def concat_tables(
    tables: List[Union[pa.Table, papq.ParquetFile]],
    promote_options: Optional[str] = "permissive",
    **kwargs,
) -> Optional[Union[pa.Table, List[papq.ParquetFile]]]:
    """
    Concatenate a list of PyArrow Tables or ParquetFiles.

    Args:
        tables: List of PyArrow Tables or ParquetFiles to concatenate

    Returns:
        - Single table/ParquetFile if only one input
        - List of ParquetFiles if all inputs are ParquetFiles (preserves lazy behavior)
        - Concatenated PyArrow Table if mixed types or multiple PyArrow Tables
        - None if input is empty
    """
    if tables is None or not len(tables):
        return None
    if len(tables) == 1:
        # Return single table as-is to preserve lazy behavior
        return next(iter(tables))

    # Check if all tables are ParquetFiles - return list to preserve lazy behavior
    if all(isinstance(table, papq.ParquetFile) for table in tables):
        return list(tables)

    # Convert all tables to PyArrow Tables for concatenation
    converted_tables = []
    for table in tables:
        if isinstance(table, papq.ParquetFile):
            converted_tables.append(table.read())
        else:
            converted_tables.append(table)

    return pa.concat_tables(converted_tables, promote_options=promote_options, **kwargs)


def delta_manifest_to_table(
    manifest: "Manifest",
    delta: Optional["Delta"] = None,
) -> pa.Table:
    """Create a flattened PyArrow table from a delta manifest.

    This implementation can process ~1.4MM records/second on a
    10-core 2025 Macbook Air M4 with 16GB of RAM.

    Args:
        manifest: The manifest to convert to a table
        delta: Optional parent delta of the manifest

    Returns:
        PyArrow table with flattened manifest entry data
    """
    if not manifest.entries:
        return pa.table({})

    num_entries = len(manifest.entries)

    # Get manifest-level data once
    manifest_author = manifest.author
    author_name = manifest_author.name if manifest_author else None
    author_version = manifest_author.version if manifest_author else None

    # Get delta-level data once
    stream_position = delta.stream_position if delta else None
    previous_stream_position = delta.previous_stream_position if delta else None

    # Pre-allocate lists for core columns to avoid repeated list operations
    url_values = [None] * num_entries
    id_values = [None] * num_entries
    mandatory_values = [None] * num_entries

    # Meta columns - most common fields in manifest entries
    meta_record_count = [None] * num_entries
    meta_content_length = [None] * num_entries
    meta_source_content_length = [None] * num_entries
    meta_content_type = [None] * num_entries
    meta_content_encoding = [None] * num_entries

    # Track any additional meta fields we haven't seen before
    additional_meta_fields = {}
    additional_entry_fields = {}

    # Single pass through entries with direct list assignment
    for i, entry in enumerate(manifest.entries):
        # Handle core entry fields efficiently
        url_values[i] = entry.get("url") or entry.get("uri")
        id_values[i] = entry.get("id")
        mandatory_values[i] = entry.get("mandatory")

        # Handle meta fields efficiently
        meta = entry.get("meta", {})
        meta_record_count[i] = meta.get("record_count")
        meta_content_length[i] = meta.get("content_length")
        meta_source_content_length[i] = meta.get("source_content_length")
        meta_content_type[i] = meta.get("content_type")
        meta_content_encoding[i] = meta.get("content_encoding")

        # Handle any additional meta fields not in our core set
        for meta_key, meta_value in meta.items():
            if meta_key not in {
                "record_count",
                "content_length",
                "source_content_length",
                "content_type",
                "content_encoding",
                "entry_type",
            }:
                field_name = f"meta_{meta_key}"
                if field_name not in additional_meta_fields:
                    additional_meta_fields[field_name] = [None] * num_entries
                additional_meta_fields[field_name][i] = meta_value

        # Handle any additional entry fields not in our core set
        for entry_key, entry_value in entry.items():
            if entry_key not in {"url", "uri", "id", "mandatory", "meta"}:
                if entry_key not in additional_entry_fields:
                    additional_entry_fields[entry_key] = [None] * num_entries
                additional_entry_fields[entry_key][i] = entry_value

    # Build the arrays dict with core columns
    arrays_dict = {
        "id": pa.array(id_values),
        "mandatory": pa.array(mandatory_values),
        "meta_content_encoding": pa.array(meta_content_encoding),
        "meta_content_length": pa.array(meta_content_length),
        "meta_content_type": pa.array(meta_content_type),
        "meta_record_count": pa.array(meta_record_count),
        "meta_source_content_length": pa.array(meta_source_content_length),
        "path": pa.array(url_values),
    }

    # Add additional fields if they exist
    for field_name, field_values in additional_meta_fields.items():
        arrays_dict[field_name] = pa.array(field_values)

    for field_name, field_values in additional_entry_fields.items():
        arrays_dict[field_name] = pa.array(field_values)

    # Add manifest/delta columns only if they have data (avoid null columns)
    if author_name is not None:
        arrays_dict["author_name"] = pa.array([author_name] * num_entries)
    if author_version is not None:
        arrays_dict["author_version"] = pa.array([author_version] * num_entries)
    if stream_position is not None:
        arrays_dict["stream_position"] = pa.array([stream_position] * num_entries)
    if previous_stream_position is not None:
        arrays_dict["previous_stream_position"] = pa.array(
            [previous_stream_position] * num_entries
        )

    return pa.table(arrays_dict)


def get_base_arrow_type_name(arrow_type: pa.DataType) -> str:
    """Get the base type name from a PyArrow DataType for compatibility lookup.

    This function normalizes complex PyArrow types to their base type names for
    use in reader compatibility validation. Only specific complex types are
    normalized; all others return their string representation.

    Args:
        arrow_type: The PyArrow DataType to normalize

    Returns:
        str: The normalized type name for compatibility lookup

    Examples:
        >>> get_base_arrow_type_name(pa.int32())
        'int32'
        >>> get_base_arrow_type_name(pa.list_(pa.int32()))
        'list'
        >>> get_base_arrow_type_name(pa.timestamp('s', tz='UTC'))
        'timestamp_tz'
    """
    # Only normalize specific complex types, otherwise return str(arrow_type)
    if isinstance(arrow_type, pa.FixedShapeTensorType):
        return "fixed_shape_tensor"
    elif pa.types.is_large_list(arrow_type):
        return "large_list"
    elif pa.types.is_list_view(arrow_type):
        return "list_view"
    elif pa.types.is_large_list_view(arrow_type):
        return "large_list_view"
    elif pa.types.is_fixed_size_list(arrow_type):
        return "fixed_size_list"
    elif pa.types.is_list(arrow_type):
        return "list"
    elif pa.types.is_map(arrow_type):
        return "map"
    elif pa.types.is_struct(arrow_type):
        return "struct"
    elif pa.types.is_dictionary(arrow_type):
        return "dictionary"
    elif pa.types.is_decimal(arrow_type):
        if isinstance(arrow_type, pa.Decimal128Type):
            return "decimal128"
        elif isinstance(arrow_type, pa.Decimal256Type):
            return "decimal256"
    elif pa.types.is_timestamp(arrow_type):
        # Check if it has timezone info
        if arrow_type.tz is not None:
            return f"timestamp_tz[{arrow_type.unit}]"
        else:
            return str(arrow_type)
    else:
        # For all other types, return the string representation
        return str(arrow_type)


def get_supported_test_types() -> List[Tuple[str, str, List[Any]]]:
    """Get comprehensive PyArrow types supported by DeltaCAT writers and readers.

    This utility function returns example Arrow arrays for every Arrow type
    supported by DeltaCAT writers and readers of tables with schemas. The data
    is used for testing compatibility between different dataset types and
    content types.

    Returns:
        List[Tuple[str, str, List[Any]]]: List of tuples containing:
            - Test name (str): Human-readable name for the test case
            - Arrow type code (str): Python code to create the PyArrow DataType
            - Test data (List[Any]): Sample data values for testing

    Examples:
        >>> test_types = get_supported_test_types()
        >>> for name, type_code, data in test_types[:2]:
        ...     print(f"{name}: {type_code} -> {data}")
        int8: pa.int8() -> [127, -128, 0]
        int16: pa.int16() -> [32767, -32768, 1000]
    """

    return [
        # Integer types
        ("int8", "pa.int8()", [127, -128, 0]),
        ("int16", "pa.int16()", [32767, -32768, 1000]),
        ("int32", "pa.int32()", [2147483647, -2147483648, 1000]),
        ("int64", "pa.int64()", [9223372036854775807, -9223372036854775808, 1000]),
        ("uint8", "pa.uint8()", [255, 0, 128]),
        ("uint16", "pa.uint16()", [65535, 0, 1000]),
        ("uint32", "pa.uint32()", [4294967295, 0, 1000]),
        ("uint64", "pa.uint64()", [18446744073709551615, 0, 1000]),
        # Float types
        ("float16", "pa.float16()", np.array([1.5, np.nan], dtype=np.float16)),
        ("float32", "pa.float32()", [3.14159, -2.71828, 1.41421]),
        ("float64", "pa.float64()", [1.123456789, -2.987654321, 3.141592653589793]),
        # Boolean and null
        ("bool_", "pa.bool_()", [True, False, True]),
        ("null", "pa.null()", [None, None, None]),
        # String types
        ("string", "pa.string()", ["hello", "world", "test"]),
        (
            "large_string",
            "pa.large_string()",
            ["large hello", "large world", "large test"],
        ),
        # Binary types
        ("binary", "pa.binary()", [b"hello", b"world", b"test"]),
        (
            "large_binary",
            "pa.large_binary()",
            [b"large hello", b"large world", b"large test"],
        ),
        # Date and time types
        (
            "date32",
            "pa.date32()",
            [date(2023, 1, 1), date(2023, 12, 31), date(2024, 6, 15)],
        ),
        (
            "date64",
            "pa.date64()",
            [date(2023, 1, 1), date(2023, 12, 31), date(2024, 6, 15)],
        ),
        ("time32_s", "pa.time32('s')", [1754962113, 1754962114, 1754962115]),
        ("time32_ms", "pa.time32('ms')", [1754962113, 1754962114, 1754962115]),
        (
            "time64_us",
            "pa.time64('us')",
            [1754962113000000, 1754962114000000, 1754962115000000],
        ),
        (
            "time64_ns",
            "pa.time64('ns')",
            [1754962113000000000, 1754962114000000000, 1754962115000000000],
        ),
        (
            "timestamp_s",
            "pa.timestamp('s')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_ms",
            "pa.timestamp('ms')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_us",
            "pa.timestamp('us')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_ns",
            "pa.timestamp('ns')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_s_utc",
            "pa.timestamp('s', tz='UTC')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_ms_utc",
            "pa.timestamp('ms', tz='UTC')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_us_utc",
            "pa.timestamp('us', tz='UTC')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        (
            "timestamp_ns_utc",
            "pa.timestamp('ns', tz='UTC')",
            [
                datetime(2023, 1, 1, 12, 0, 0),
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2024, 6, 15, 10, 30, 45),
            ],
        ),
        ("duration_s", "pa.duration('s')", [1754962113, 1754962114, 1754962115]),
        (
            "duration_ms",
            "pa.duration('ms')",
            [1754962113000, 1754962114000, 1754962115000],
        ),
        (
            "duration_us",
            "pa.duration('us')",
            [1754962113000000, 1754962114000000, 1754962115000000],
        ),
        (
            "duration_ns",
            "pa.duration('ns')",
            [1754962113000000000, 1754962114000000000, 1754962115000000000],
        ),
        (
            "month_day_nano",
            "pa.month_day_nano_interval()",
            [
                pa.scalar((1, 15, -30), type=pa.month_day_nano_interval()),
                pa.scalar((2, 15, -30), type=pa.month_day_nano_interval()),
                pa.scalar((3, 15, -30), type=pa.month_day_nano_interval()),
            ],
        ),
        # Decimal
        (
            "decimal128_5_2",
            "pa.decimal128(5, 2)",
            [Decimal("123.45"), Decimal("-67.89"), Decimal("999.99")],
        ),
        (
            "decimal128_38_0",
            "pa.decimal128(38, 0)",
            [
                Decimal("12345678901234567890123456789012345678"),
                Decimal("-12345678901234567890123456789012345678"),
                Decimal("0"),
            ],
        ),
        (
            "decimal128_1_0",
            "pa.decimal128(1, 0)",
            [Decimal("1"), Decimal("2"), Decimal("3")],
        ),
        (
            "decimal128_38_10",
            "pa.decimal128(38, 10)",
            [
                Decimal("1234567890123456789012345678.9012345678"),
                Decimal("-1234567890123456789012345678.9012345678"),
                Decimal("0.0000000000"),
            ],
        ),
        (
            "decimal256_76_0",
            "pa.decimal256(76, 0)",
            [
                Decimal(
                    "1234567890123456789012345678901234567812345678901234567890123456789012345678"
                ),
                Decimal("-0"),
                Decimal("0"),
            ],
        ),
        (
            "decimal256_1_0",
            "pa.decimal256(1, 0)",
            [Decimal("1"), Decimal("2"), Decimal("3")],
        ),
        (
            "decimal256_5_2",
            "pa.decimal256(5, 2)",
            [Decimal("123.45"), Decimal("-67.89"), Decimal("999.99")],
        ),
        (
            "decimal256_76_38",
            "pa.decimal256(76, 38)",
            [
                Decimal(
                    "12345678901234567890123456789012345678.12345678901234567890123456789012345678"
                ),
                Decimal("-0.00000000000000000000000000000000000000"),
                Decimal("0.00000000000000000000000000000000000000"),
            ],
        ),
        # List types
        ("list_int32", "pa.list_(pa.int32())", [[1, 2, 3], [4, 5], [6, 7, 8, 9]]),
        ("list_string", "pa.list_(pa.string())", [["a", "b"], ["c", "d", "e"], ["f"]]),
        # Struct type
        (
            "struct_simple",
            "pa.struct([('name', pa.string()), ('age', pa.int32())])",
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
            ],
        ),
        (
            "large_list_int32",
            "pa.large_list(pa.int32())",
            [[1, 2, 3], [4, 5], [6, 7, 8, 9]],
        ),
        (
            "fixed_size_list_int32",
            "pa.list_(pa.int32(), 3)",
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        ),
        (
            "list_view_int32",
            "pa.list_view(pa.int32())",
            [[1, 2, 3], [4, 5], [6, 7, 8, 9]],
        ),
        (
            "large_list_view_int32",
            "pa.large_list_view(pa.int32())",
            [[1, 2, 3], [4, 5], [6, 7, 8, 9]],
        ),
        # Dictionary type
        (
            "dictionary_string",
            "pa.dictionary(pa.int32(), pa.string())",
            ["apple", "banana", "apple"],
        ),
        # Map type
        (
            "map_string_int32",
            "pa.map_(pa.string(), pa.int32())",
            [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {"e": 5}],
        ),
        # Extension Types
        (
            "fixed_shape_tensor",
            "pa.fixed_shape_tensor(pa.int32(), [3, 3])",
            [
                np.array([1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=np.int32),
                np.array([10, 11, 12, 13, 14, 15, 16, 17, 18], dtype=np.int32),
                np.array([19, 20, 21, 22, 23, 24, 25, 26, 27], dtype=np.int32),
            ],
        ),
    ]
