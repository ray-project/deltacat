# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import bz2
import gzip
import io
import logging
from functools import partial
from typing import Any, Callable, Dict, Iterable, List, Optional

import pyarrow as pa
from fsspec import AbstractFileSystem
from pyarrow import csv as pacsv
from pyarrow import feather as paf
from pyarrow import json as pajson
from pyarrow import parquet as papq
from ray.data.datasource import BlockWritePathProvider

from deltacat import logs
from deltacat.types.media import (
    DELIMITED_TEXT_CONTENT_TYPES,
    TABULAR_CONTENT_TYPES,
    ContentEncoding,
    ContentType,
)
from deltacat.utils.common import ContentTypeKwargsProvider, ReadKwargsProvider
from deltacat.utils.performance import timed_invocation

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
    ContentType.JSON.value: pajson.read_json,
}


def write_feather(
    table: pa.Table, path: str, *, filesystem: AbstractFileSystem, **kwargs
) -> None:

    with filesystem.open(path, "wb") as f:
        paf.write_feather(table, f, **kwargs)


def write_csv(
    table: pa.Table, path: str, *, filesystem: AbstractFileSystem, **kwargs
) -> None:

    with filesystem.open(path, "wb") as f:
        # TODO (pdames): Add support for client-specified compression types.
        with pa.CompressedOutputStream(f, ContentEncoding.GZIP.value) as out:
            if kwargs.get("write_options") is None:
                # column names are kept in table metadata, so omit header
                kwargs["write_options"] = pacsv.WriteOptions(include_header=False)
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
    }:
        return {}
    # Pyarrow.orc is disabled in Pyarrow 0.15, 0.16:
    # https://issues.apache.org/jira/browse/ARROW-7811
    # if DataTypes.ContentType.ORC:
    #   return {},
    raise ValueError(f"Unsupported content type: {content_type}")


# TODO (pdames): add deflate and snappy
ENCODING_TO_FILE_INIT: Dict[str, Callable] = {
    ContentEncoding.GZIP.value: partial(gzip.GzipFile, mode="rb"),
    ContentEncoding.BZIP2.value: partial(bz2.BZ2File, mode="rb"),
    ContentEncoding.IDENTITY.value: lambda fileobj: fileobj,
}


def slice_table(table: pa.Table, max_len: Optional[int]) -> List[pa.Table]:
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


def s3_file_to_table(
    s3_url: str,
    content_type: str,
    content_encoding: str,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    pa_read_func_kwargs_provider: Optional[ReadKwargsProvider] = None,
    **s3_client_kwargs,
) -> pa.Table:

    from deltacat.aws import s3u as s3_utils

    logger.debug(
        f"Reading {s3_url} to PyArrow. Content type: {content_type}. "
        f"Encoding: {content_encoding}"
    )
    s3_obj = s3_utils.get_object_at_url(s3_url, **s3_client_kwargs)
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
    table, latency = timed_invocation(pa_read_func, *args, **kwargs)
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
    **kwargs,
) -> None:
    """
    Writes the given Pyarrow Table to a file.
    """
    writer = CONTENT_TYPE_TO_PA_WRITE_FUNC.get(content_type)
    if not writer:
        raise NotImplementedError(
            f"Pyarrow writer for content type '{content_type}' not "
            f"implemented. Known content types: "
            f"{CONTENT_TYPE_TO_PA_WRITE_FUNC.keys}"
        )
    path = block_path_provider(base_path)
    logger.debug(f"Writing table: {table} with kwargs: {kwargs} to path: {path}")
    writer(table, path, filesystem=file_system, **kwargs)


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
