import pyarrow as pa
import gzip
import bz2
import io
import logging

from functools import partial
from fsspec import AbstractFileSystem

from pyarrow import feather as paf, parquet as papq, csv as pacsv, \
    json as pajson

from ray.data.datasource import BlockWritePathProvider

from deltacat import logs
from deltacat.types.media import ContentType, ContentEncoding, \
    DELIMITED_TEXT_CONTENT_TYPES, TABULAR_CONTENT_TYPES
from deltacat.utils.common import ReadKwargsProvider, ContentTypeKwargsProvider
from deltacat.utils.performance import timed_invocation

from typing import Any, Callable, Dict, List, Optional, Iterable, Union

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


CONTENT_TYPE_TO_PA_READ_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: pacsv.read_csv,
    ContentType.TSV.value: pacsv.read_csv,
    ContentType.CSV.value: pacsv.read_csv,
    ContentType.PSV.value: pacsv.read_csv,
    ContentType.PARQUET.value: papq.read_table,
    ContentType.FEATHER.value: paf.read_table,
    # Pyarrow.orc is disabled in Pyarrow 0.15, 0.16:
    # https://issues.apache.org/jira/browse/ARROW-7811
    # ContentType.ORC.value: paorc.ContentType.ORCFile,
    ContentType.JSON.value: pajson.read_json
}


def write_feather(
        table: pa.Table,
        path: str,
        *,
        filesystem: AbstractFileSystem,
        **kwargs) -> None:

    with filesystem.open(path, "wb") as f:
        paf.write_feather(table, f, **kwargs)


def write_csv(
        table: pa.Table,
        path: str,
        *,
        filesystem: AbstractFileSystem,
        **kwargs) -> None:

    with filesystem.open(path, "wb") as f:
        # TODO (pdames): Add support for client-specified compression types.
        with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
            if kwargs.get("write_options") is None:
                # column names are kept in table metadata, so omit header
                kwargs["write_options"] = pacsv.WriteOptions(
                    include_header=False)
            pacsv.write_csv(table, out, **kwargs)


CONTENT_TYPE_TO_PA_WRITE_FUNC: Dict[str, Callable] = {
    # TODO (pdames): add support for other delimited text content types as
    #  pyarrow adds support for custom delimiters, escaping, and None value
    #  representations to pyarrow.csv.WriteOptions.
    ContentType.CSV.value: write_csv,
    ContentType.PARQUET.value: papq.write_table,
    ContentType.FEATHER.value: write_feather,
}


def content_type_to_reader_kwargs(content_type: str) -> Dict[str, Any]:
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "parse_options": pacsv.ParseOptions(
                delimiter="\t",
                quote_char=False),
            "convert_options": pacsv.ConvertOptions(
                null_values=[""],  # pyarrow defaults are ["", "NULL", "null"]
                strings_can_be_null=True,
            )
        }
    if content_type == ContentType.TSV.value:
        return {"parse_options": pacsv.ParseOptions(delimiter="\t")}
    if content_type == ContentType.CSV.value:
        return {"parse_options": pacsv.ParseOptions(delimiter=",")}
    if content_type == ContentType.PSV.value:
        return {"parse_options": pacsv.ParseOptions(delimiter="|")}
    if content_type in {ContentType.PARQUET.value,
                        ContentType.FEATHER.value,
                        ContentType.JSON.value}:
        return {}
    # Pyarrow.orc is disabled in Pyarrow 0.15, 0.16:
    # https://issues.apache.org/jira/browse/ARROW-7811
    # if DataTypes.ContentType.ORC:
    #   return {},
    raise ValueError(f"Unsupported content type: {content_type}")


# TODO (pdames): add deflate and snappy
ENCODING_TO_FILE_INIT: Dict[str, Callable] = {
    ContentEncoding.GZIP.value: partial(gzip.GzipFile, mode='rb'),
    ContentEncoding.BZIP2.value: partial(bz2.BZ2File, mode='rb'),
    ContentEncoding.IDENTITY.value: lambda fileobj: fileobj,
}


def slice_table(
        table: pa.Table,
        max_len: Optional[int]) -> List[pa.Table]:
    """
    Iteratively create 0-copy table slices.
    """
    if max_len is None:
        return [table]
    tables = []
    offset = 0
    records_remaining = len(table)
    while records_remaining > 0:
        records_this_entry = min(
            max_len,
            records_remaining
        )
        tables.append(table.slice(offset, records_this_entry))
        records_remaining -= records_this_entry
        offset += records_this_entry
    return tables


class ReadKwargsProviderPyArrowCsvPureUtf8(ContentTypeKwargsProvider):
    """ReadKwargsProvider impl that reads columns of delimited text files
    as UTF-8 strings (i.e. disables type inference). Useful for ensuring
    lossless reads of UTF-8 delimited text datasets and improving read
    performance in cases where type casting is not required."""
    def __init__(self, include_columns: Optional[Iterable[str]] = None):
        self.include_columns = include_columns

    def _get_kwargs(
            self,
            content_type: str,
            kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if content_type in DELIMITED_TEXT_CONTENT_TYPES:
            convert_options: pacsv.ConvertOptions = \
                kwargs.get("convert_options")
            if convert_options is None:
                convert_options = pacsv.ConvertOptions()
            # read only the included columns as strings?
            column_names = self.include_columns \
                if self.include_columns else convert_options.include_columns
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
    def __init__(self, schema: Optional[pa.Schema] = None):
        self.schema = schema

    def _get_kwargs(
            self,
            content_type: str,
            kwargs: Dict[str, Any]) -> Dict[str, Any]:
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
        return kwargs


def _add_column_kwargs(
        content_type: str,
        column_names: Optional[List[str]],
        include_columns: Optional[List[str]],
        kwargs: Dict[str, Any]):

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
                    f"for non-tabular content type {content_type}")


def s3_file_to_table(
        s3_url: str,
        content_type: str,
        content_encoding: str,
        column_names: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
        **s3_client_kwargs) -> pa.Table:

    from deltacat.aws import s3u as s3_utils
    logger.debug(f"Reading {s3_url} to PyArrow. Content type: {content_type}. "
                 f"Encoding: {content_encoding}")
    s3_obj = s3_utils.get_object_at_url(
        s3_url,
        **s3_client_kwargs
    )
    logger.debug(f"Read S3 object from {s3_url}: {s3_obj}")
    pa_read_func = CONTENT_TYPE_TO_PA_READ_FUNC[content_type]
    input_file_init = ENCODING_TO_FILE_INIT[content_encoding]
    input_file = input_file_init(fileobj=io.BytesIO(s3_obj["Body"].read()))

    args = [input_file]
    kwargs = content_type_to_reader_kwargs(content_type)
    _add_column_kwargs(content_type, column_names, include_columns, kwargs)

    if pa_read_func_kwargs_provider:
        kwargs = pa_read_func_kwargs_provider(content_type, kwargs)

    logger.debug(f"Reading {s3_url} via {pa_read_func} with kwargs: {kwargs}")
    table, latency = timed_invocation(
        pa_read_func,
        *args,
        **kwargs
    )
    logger.debug(f"Time to read {s3_url} into PyArrow table: {latency}s")
    return table


def table_size(table: pa.Table) -> int:
    return table.nbytes


def table_to_file(
        table: pa.Table,
        base_path: str,
        file_system: AbstractFileSystem,
        block_path_provider: BlockWritePathProvider,
        content_type: str = ContentType.PARQUET.value,
        **kwargs) -> None:
    """
    Writes the given Pyarrow Table to a file.
    """
    writer = CONTENT_TYPE_TO_PA_WRITE_FUNC.get(content_type)
    if not writer:
        raise NotImplementedError(
            f"Pyarrow writer for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{CONTENT_TYPE_TO_PA_WRITE_FUNC.keys}")
    path = block_path_provider(base_path)
    writer(
        table,
        path,
        filesystem=file_system,
        **kwargs
    )
