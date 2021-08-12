import pyarrow as pa
import gzip
import bz2
import io
import logging
from typing import Any, Callable, Dict, List, Optional
from fsspec import AbstractFileSystem
from pyarrow import feather as paf, parquet as papq, csv as pacsv, \
    json as pajson
from deltacat import logs
from deltacat.types.media import ContentType, ContentEncoding, \
    DELIMITED_TEXT_CONTENT_TYPES
from deltacat.types.media import CONTENT_TYPE_TO_USER_KWARGS_KEY
from deltacat.aws import s3u as s3_utils
from deltacat.utils.performance import timed_invocation
from functools import partial

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
        file_system: AbstractFileSystem,
        **kwargs):

    with file_system.open(path, "wb") as f:
        paf.write_feather(table, f, **kwargs)


CONTENT_TYPE_TO_PA_WRITE_FUNC: Dict[str, Callable] = {
    ContentType.PARQUET.value: papq.write_table,
    ContentType.FEATHER.value: write_feather
}


CONTENT_TYPE_TO_READER_KWARGS: Dict[str, Dict[str, Any]] = {
    ContentType.UNESCAPED_TSV.value: {
        "parse_options": pacsv.ParseOptions(
            delimiter="\t",
            quote_char=False)
    },
    ContentType.TSV.value: {
        "parse_options": pacsv.ParseOptions(
            delimiter="\t")
    },
    ContentType.CSV.value: {
        "parse_options": pacsv.ParseOptions(
            delimiter=",")
    },
    ContentType.PSV.value: {
        "parse_options": pacsv.ParseOptions(
            delimiter="|")
    },
    ContentType.PARQUET.value: {},
    ContentType.FEATHER.value: {},
    # Pyarrow.orc is disabled in Pyarrow 0.15, 0.16:
    # https://issues.apache.org/jira/browse/ARROW-7811
    # DataTypes.ContentType.ORC: {},
    ContentType.JSON.value: {},
}

# TODO: add deflate and snappy
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


def s3_file_to_table(
        s3_url: str,
        content_type: str,
        content_encoding: str,
        pa_read_func_kwargs: Optional[Dict[str, Any]] = None,
        **s3_client_kwargs) -> pa.Table:

    logger.debug(f"Reading {s3_url} to PyArrow. Content type: {content_type}. "
                 f"Encoding: {content_encoding}")
    s3_obj = s3_utils.get_object_at_url(
        s3_url,
        **s3_client_kwargs
    )
    logger.debug(f"Read S3 object from {s3_url}: {s3_obj}")
    pa_read_func = CONTENT_TYPE_TO_PA_READ_FUNC[content_type]
    input_file_init = ENCODING_TO_FILE_INIT[content_encoding]
    input_file = input_file_init(fileobj=io.BytesIO(s3_obj['Body'].read()))

    args = [input_file]
    kwargs = CONTENT_TYPE_TO_READER_KWARGS[content_type]

    if pa_read_func_kwargs is None:
        pa_read_func_kwargs = {}
    if content_type in DELIMITED_TEXT_CONTENT_TYPES:
        # ReadOptions can't be included in CONTENT_TYPE_TO_KWARGS because it doesn't pickle:
        #   File "/home/ubuntu/anaconda3/lib/python3.7/site-packages/ray/cloudpickle/cloudpickle_fast.py", line 563, in dump
        #       return Pickler.dump(self, obj)
        #   File "stringsource", line 2, in pyarrow._csv.ReadOptions.__reduce_cython__
        #   TypeError: self.options cannot be converted to a Python object for pickling
        logger.debug(f"{content_type} is a delimited text content type")
        kwargs["read_options"] = pacsv.ReadOptions(
            autogenerate_column_names=True
        )
    if pa_read_func_kwargs:
        kwargs.update(pa_read_func_kwargs.get(
            CONTENT_TYPE_TO_USER_KWARGS_KEY[content_type]
        ))
    table, latency = timed_invocation(
        pa_read_func,
        *args,
        **kwargs
    )
    # Pyarrow.orc is disabled in Pyarrow 0.15, 0.16:
    # https://issues.apache.org/jira/browse/ARROW-7811
    # if content_type == DatasetConstants.ContentType.ORC:
    #    result = result.read()
    logger.debug(f"Time to read {s3_url} into PyArrow table: {latency}s")
    return table


def table_size(table: pa.Table) -> int:
    return table.nbytes


def table_to_file(
        table: pa.Table,
        path: str,
        file_system: AbstractFileSystem,
        content_type: str = ContentType.PARQUET.value,
        **kwargs):
    """
    Writes the given Pyarrow Table to a file.
    """
    writer = CONTENT_TYPE_TO_PA_WRITE_FUNC[content_type]
    writer(
        table,
        path,
        filesystem=file_system,
        **kwargs
    )
