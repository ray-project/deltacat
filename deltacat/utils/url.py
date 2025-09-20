# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import functools
import json
import posixpath
import logging

from typing import Callable, List, Tuple, Any, Union, Optional
from urllib.parse import urlparse, urlunparse, parse_qs

import ray
import daft

import pandas as pd
import numpy as np
import pyarrow as pa
import polars as pl
import deltacat as dc

import pyarrow.csv as pacsv
import pyarrow.json as pajson

from deltacat.catalog import CatalogProperties
from deltacat.constants import DEFAULT_NAMESPACE
from deltacat.storage.model.manifest import reconstruct_manifest_entry_url
from deltacat.storage import (
    metastore,
    Dataset,
    Delta,
    DeltaLocator,
    ListResult,
    Metafile,
    Namespace,
    NamespaceLocator,
    Partition,
    Stream,
    StreamFormat,
    StreamLocator,
    PartitionLocator,
    Table,
    TableLocator,
    TableVersion,
    TableVersionLocator,
)
from deltacat.types.media import (
    DatasetType,
    DatastoreType,
)
from deltacat.utils import pyarrow as pa_utils
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _normalize_partition_values_from_json(partition_values):
    """
    Normalize partition values parsed from JSON URLs.

    Both None and empty list [] represent unpartitioned data, but they should be
    normalized to None for consistent lookup and validation.

    Args:
        partition_values: Partition values parsed from JSON

    Returns:
        None for unpartitioned data (both None and [] inputs),
        original value for partitioned data
    """
    if partition_values is None or (
        isinstance(partition_values, list) and len(partition_values) == 0
    ):
        return None
    return partition_values


RAY_DATASTORE_TYPE_TO_READER = {
    DatastoreType.AUDIO: lambda url: functools.partial(
        ray.data.read_audio,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.AVRO: lambda url: functools.partial(
        ray.data.read_avro,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.BIGQUERY: lambda url: functools.partial(
        ray.data.read_bigquery,
        project_id=url.parsed.netloc,
        dataset=url.path_elements[0] if url.path_elements else None,
        **url.query_params,
    ),
    DatastoreType.BINARY: lambda url: functools.partial(
        ray.data.read_binary_files,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.CSV: lambda url: functools.partial(
        ray.data.read_csv,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.CLICKHOUSE: lambda url: functools.partial(
        ray.data.read_clickhouse,
        table=url.parsed.query,
        dsn=url.url,
        **url.query_params,
    ),
    DatastoreType.DATABRICKS_TABLES: lambda url: functools.partial(
        ray.data.read_databricks_tables,
        warehouse_id=url.parsed.netloc,
        **url.query_params,
    ),
    DatastoreType.DELTA_SHARING: lambda url: functools.partial(
        ray.data.read_delta_sharing_tables,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.HUDI: lambda url: functools.partial(
        ray.data.read_hudi,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.ICEBERG: lambda url: functools.partial(
        ray.data.read_iceberg,
        table_identifier=url.parsed.netloc,
        **url.query_params,
    ),
    DatastoreType.IMAGES: lambda url: functools.partial(
        ray.data.read_images,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        ray.data.read_json,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.LANCE: lambda url: functools.partial(
        ray.data.read_lance,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.MONGO: lambda url: functools.partial(
        ray.data.read_mongo,
        url.url,
        **url.query_params,
    ),
    DatastoreType.NUMPY: lambda url: functools.partial(
        ray.data.read_numpy,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        ray.data.read_parquet,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.TEXT: lambda url: functools.partial(
        ray.data.read_text,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.TFRECORDS: lambda url: functools.partial(
        ray.data.read_tfrecords,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.VIDEOS: lambda url: functools.partial(
        ray.data.read_videos,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.WEBDATASET: lambda url: functools.partial(
        ray.data.read_webdataset,
        url.url_path,
        **url.query_params,
    ),
}

RAY_DATASTORE_TYPE_TO_WRITER = {
    DatastoreType.BIGQUERY: lambda url: functools.partial(
        ray.data.Dataset.write_bigquery,
        project_id=url.parsed.netloc,
        dataset=url.path_elements[0] if url.path_elements else None,
        **url.query_params,
    ),
    DatastoreType.CSV: lambda url: functools.partial(
        ray.data.write_csv,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.ICEBERG: lambda url: functools.partial(
        ray.data.Dataset.write_iceberg,
        table_identifier=url.parsed.netloc,
        **url.query_params,
    ),
    DatastoreType.IMAGES: lambda url: functools.partial(
        ray.data.Dataset.write_images,
        path=url.url_path,
        column=url.query_params.pop("column", "image") if url.query_params else "image",
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        ray.data.Dataset.write_json,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.LANCE: lambda url: functools.partial(
        ray.data.Dataset.write_lance,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.MONGO: lambda url: functools.partial(
        ray.data.Dataset.write_mongo,
        url.url,
        **url.query_params,
    ),
    DatastoreType.NUMPY: lambda url: functools.partial(
        ray.data.Dataset.write_numpy,
        path=url.url_path,
        column=url.query_params.pop("column", "data") if url.query_params else "data",
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        ray.data.Dataset.write_parquet,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.TFRECORDS: lambda url: functools.partial(
        ray.data.Dataset.write_tfrecords,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.WEBDATASET: lambda url: functools.partial(
        ray.data.Dataset.write_webdataset,
        url.url_path,
        **url.query_params,
    ),
}


def _daft_binary_reader(url_path: str) -> daft.DataFrame:
    df = daft.from_pydict({"url": [url_path]})
    return df.with_column("data", df["url"].url.download())


DAFT_DATASTORE_TYPE_TO_READER = {
    DatastoreType.BINARY: lambda url: functools.partial(
        _daft_binary_reader,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.CSV: lambda url: functools.partial(
        daft.io.read_csv,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.DELTA_LAKE: lambda url: functools.partial(
        daft.io.read_deltalake,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.HUDI: lambda url: functools.partial(
        daft.io.read_hudi,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.ICEBERG: lambda url: functools.partial(
        daft.io.read_iceberg,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        daft.io.read_json,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        daft.io.read_parquet,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.WARC: lambda url: functools.partial(
        daft.io.read_warc,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.TEXT: lambda url: functools.partial(
        daft.io.read_csv,
        url.url_path,
        infer_schema=False,
        schema={"text": daft.DataType.string()},
        has_headers=False,
        delimiter=chr(25),  # end of medium char
        double_quote=False,
        comment=None,
    ),
}

DAFT_DATASTORE_TYPE_TO_WRITER = {
    DatastoreType.CSV: lambda url: functools.partial(
        daft.DataFrame.write_csv,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.DELTA_LAKE: lambda url: functools.partial(
        daft.DataFrame.write_deltalake,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.ICEBERG: lambda url: functools.partial(
        daft.DataFrame.write_iceberg,
        **url.query_params,
    ),
    DatastoreType.LANCE: lambda url: functools.partial(
        daft.DataFrame.write_lance,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        daft.DataFrame.write_parquet,
        url.url_path,
        **url.query_params,
    ),
}

PYARROW_DATASTORE_TYPE_TO_READER = {
    DatastoreType.CSV: lambda url: functools.partial(
        pa_utils.read_csv,
        url.url_path,
        read_options=pacsv.ReadOptions(use_threads=False),
        **url.query_params,
    ),
    DatastoreType.FEATHER: lambda url: functools.partial(
        pa_utils.read_feather,
        url.url_path,
        use_threads=False,
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        pa_utils.read_json,
        url.url_path,
        pajson.ReadOptions(use_threads=False),
        **url.query_params,
    ),
    DatastoreType.ORC: lambda url: functools.partial(
        pa_utils.read_orc,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        pa_utils.read_parquet,
        url.url_path,
        use_threads=False,
        **url.query_params,
    ),
    DatastoreType.TEXT: lambda url: functools.partial(
        pa_utils.read_csv,
        url.url_path,
        read_options=pacsv.ReadOptions(
            use_threads=False,
            column_names=["text"],
        ),
        parse_options=pacsv.ParseOptions(
            delimiter=chr(25),  # end of medium char
            quote_char=False,
            double_quote=False,
        ),
        convert_options=pacsv.ConvertOptions(
            check_utf8=False,
            column_types={"text": pa.string()},
        ),
    ),
}

PYARROW_DATASTORE_TYPE_TO_WRITER = {
    DatastoreType.CSV: lambda url: functools.partial(
        pa_utils.write_csv,
        path=url.url_path,
        **url.query_params,
    ),
    DatastoreType.FEATHER: lambda url: functools.partial(
        pa_utils.write_feather,
        path=url.url_path,
        **url.query_params,
    ),
    DatastoreType.ORC: lambda url: functools.partial(
        pa_utils.write_orc,
        path=url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        pa_utils.write_parquet,
        path=url.url_path,
        **url.query_params,
    ),
}

POLARS_DATASTORE_TYPE_TO_READER = {
    DatastoreType.CSV: lambda url: functools.partial(
        pl.read_csv,
        url.url_path,
        n_threads=1,
        **url.query_params,
    ),
    DatastoreType.DELTA_LAKE: lambda url: functools.partial(
        pl.read_delta,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.ICEBERG: lambda url: functools.partial(
        pl.scan_iceberg,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        pl.read_json,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        pl.read_parquet,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.TEXT: lambda url: functools.partial(
        pl.read_csv,
        url.url_path,
        new_columns=["text"],
        n_threads=1,
        separator=chr(25),  # end of medium char
        has_header=False,
        quote_char=None,
        infer_schema=False,
    ),
}

POLARS_DATASTORE_TYPE_TO_WRITER = {
    DatastoreType.AVRO: lambda url: functools.partial(
        pl.DataFrame.write_avro,
        file=url.url_path,
        **url.query_params,
    ),
    DatastoreType.CSV: lambda url: functools.partial(
        pl.DataFrame.write_csv,
        file=url.url_path,
        **url.query_params,
    ),
    DatastoreType.DELTA_LAKE: lambda url: functools.partial(
        pl.DataFrame.write_delta,
        target=url.url_path,
        **url.query_params,
    ),
    DatastoreType.ICEBERG: lambda url: functools.partial(
        pl.DataFrame.write_iceberg,
        target=url.url_path,
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        pl.DataFrame.write_ndjson,
        file=url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        pl.DataFrame.write_parquet,
        file=url.url_path,
        **url.query_params,
    ),
}

PANDAS_DATASTORE_TYPE_TO_READER = {
    DatastoreType.CSV: lambda url: functools.partial(
        pd.read_csv,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.FEATHER: lambda url: functools.partial(
        pd.read_feather,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.HDF: lambda url: functools.partial(
        pd.read_hdf,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.HTML: lambda url: functools.partial(
        pd.read_html,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        pd.read_json,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.ORC: lambda url: functools.partial(
        pd.read_orc,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        pd.read_parquet,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.XML: lambda url: functools.partial(
        pd.read_xml,
        url.url_path,
        **url.query_params,
    ),
}

PANDAS_DATASTORE_TYPE_TO_WRITER = {
    DatastoreType.CSV: lambda url: functools.partial(
        pd.DataFrame.to_csv,
        path_or_buf=url.url_path,
        **url.query_params,
    ),
    DatastoreType.FEATHER: lambda url: functools.partial(
        pd.DataFrame.to_feather,
        path=url.url_path,
        **url.query_params,
    ),
    DatastoreType.HDF: lambda url: functools.partial(
        pd.DataFrame.to_hdf,
        path_or_buf=url.url_path,
        **url.query_params,
    ),
    DatastoreType.HTML: lambda url: functools.partial(
        pd.DataFrame.to_html,
        buf=url.url_path,
        **url.query_params,
    ),
    DatastoreType.JSON: lambda url: functools.partial(
        pd.DataFrame.to_json,
        path_or_buf=url.url_path,
        **url.query_params,
    ),
    DatastoreType.ORC: lambda url: functools.partial(
        pd.DataFrame.to_orc,
        path=url.url_path,
        **url.query_params,
    ),
    DatastoreType.PARQUET: lambda url: functools.partial(
        pd.DataFrame.to_parquet,
        path=url.url_path,
        **url.query_params,
    ),
    DatastoreType.XML: lambda url: functools.partial(
        pd.DataFrame.to_xml,
        path_or_buffer=url.url_path,
        **url.query_params,
    ),
}

NUMPY_DATASTORE_TYPE_TO_READER = {
    DatastoreType.BINARY: lambda url: functools.partial(
        np.fromfile,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.CSV: lambda url: functools.partial(
        np.genfromtxt,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.NUMPY: lambda url: functools.partial(
        np.load,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.TEXT: lambda url: functools.partial(
        np.loadtxt,
        url.url_path,
        **url.query_params,
    ),
}

NUMPY_DATASTORE_TYPE_TO_WRITER = {
    DatastoreType.CSV: lambda url: functools.partial(
        np.savetxt,
        url.url_path,
        delimiter=",",
        **url.query_params,
    ),
    DatastoreType.NUMPY: lambda url: functools.partial(
        np.savez_compressed,
        url.url_path,
        **url.query_params,
    ),
    DatastoreType.TEXT: lambda url: functools.partial(
        np.savetxt,
        url.url_path,
        **url.query_params,
    ),
}

DATASET_TYPE_TO_DATASTORE_TYPE_READER_RESOLVER = {
    DatasetType.RAY_DATASET: RAY_DATASTORE_TYPE_TO_READER,
    DatasetType.DAFT: DAFT_DATASTORE_TYPE_TO_READER,
    DatasetType.PANDAS: PANDAS_DATASTORE_TYPE_TO_READER,
    DatasetType.POLARS: POLARS_DATASTORE_TYPE_TO_READER,
    DatasetType.PYARROW: PYARROW_DATASTORE_TYPE_TO_READER,
    DatasetType.NUMPY: NUMPY_DATASTORE_TYPE_TO_READER,
}

DATASET_TYPE_TO_DATASTORE_TYPE_WRITER_RESOLVER = {
    DatasetType.RAY_DATASET: RAY_DATASTORE_TYPE_TO_WRITER,
    DatasetType.DAFT: DAFT_DATASTORE_TYPE_TO_WRITER,
    DatasetType.PANDAS: PANDAS_DATASTORE_TYPE_TO_WRITER,
    DatasetType.POLARS: POLARS_DATASTORE_TYPE_TO_WRITER,
    DatasetType.PYARROW: PYARROW_DATASTORE_TYPE_TO_WRITER,
    DatasetType.NUMPY: NUMPY_DATASTORE_TYPE_TO_WRITER,
}


class DeltaCatUrl:
    """
    Class for parsing DeltaCAT URLs, which are used to unambiguously locate
    any internal object(s) already registered in a DeltaCAT catalog, or external
    object(s) that could be registered in a DeltaCAT catalog.

    Valid DeltaCAT URLs that reference internal catalog objects registered in a
    DeltaCAT catalog include:

    dc://<catalog>/[namespace]/[table]/[tableversion]/[stream]/[partition]/[delta]
    namespace://<namespace>/[table]/[tableversion]/[stream]/[partition]/[delta]
    table://<table>/[tableversion]/[stream]/[partition]/[delta]

    Where <arg> is a required part of the URL and [arg] is an optional part of
    the URL.

    Valid DeltaCAT URLs that reference external objects include most types
    readable into any supported DeltaCAT dataset type (e.g., Ray Data, Daft,
    PyArrow, Pandas, Numpy). External object URLs take the form
    <DatastoreType>+<URL> or, to be more explicit,
    <DatastoreType>+<scheme>://<path> where `DatastoreType` is any value
    from :class:`deltacat.types.media.DatastoreType`

    To reference a file on local disk, replace <scheme>:// with "file" or
    "local". To read an absolute local file path, use "file:///" or
    "local:///". To read a local file path relative to the current working
    directory, use "local://".

    audio+<scheme>://<path>?param1=val1&param2=val2&...
    avro+<scheme>://<path>?param1=val1&param2=val2&...
    binary+<scheme>://<path>?param1=val1&param2=val2&...
    csv+<scheme>://<path>?param1=val1&param2=val2&...
    deltalake+<scheme>://<path>?param1=val1&param2=val2&...
    deltasharing+<scheme>://<path>?param1=val1&param2=val2&...
    feather+<scheme>://<path>?param1=val1&param2=val2&...
    hdf+<scheme>://<path>?param1=val1&param2=val2&...
    html+<scheme>://<path>?param1=val1&param2=val2&...
    hudi+<scheme>://<path>?param1=val1&param2=val2&...
    images+<scheme>://<path>?param1=val1&param2=val2&...
    json+<scheme>://<path>?param1=val1&param2=val2&...
    lance+<scheme>://<path>?param1=val1&param2=val2&...
    numpy+<scheme>://<path>?param1=val1&param2=val2&...
    orc+<scheme>://<path>?param1=val1&param2=val2&...
    parquet+<scheme>://<path>?param1=val1&param2=val2&...
    text+<scheme>://<path>?param1=val1&param2=val2&...
    tfrecords+<scheme>://<path>?param1=val1&param2=val2&...
    text+<scheme>://<path>?param1=val1&param2=val2&...
    warc+<scheme>://<path>?param1=val1&param2=val2&...
    videos+<scheme>://<path>?param1=val1&param2=val2&...
    webdataset+<scheme>://<path>?param1=val1&param2=val2&...
    xml+<scheme>://<path>?param1=val1&param2=val2&...

    Some DeltaCAT URLs reference special types of external objects
    locatable via custom URLs that don't conform to the usual
    <DatastoreType>+<URL> convention shown above, like:

    <mongodb_uri>?database=<db_name>&collection=<collection_name>&...
    bigquery://<project_id>/<dataset>?param1=val1&...
    <clickhouse_dsn>?table=<table_name>?param1=val1&...
    databricks://<warehouse_id>?param1=val1&...
    iceberg://<table_identifier>?param1=val1&...

    Note that, for reads, each of the above URLs typically resolves directly
    to the equivalent :class:`deltacat.types.media.DatasetType` reader. For
    example, if Ray Data is the dataset type then the equivalent
    ray.data.read_{} API is used. In this case, a read referencing a URL of the
    form "audio+file:///my/audio.mp4" would resolve to a call to
    ray.data.read_audio("/my/audio.mp4").
    """

    # Auto-resolved DeltaCAT catalog path default identifiers
    DELTACAT_URL_DEFAULT_CATALOG = "default"
    DELTACAT_URL_DEFAULT_NAMESPACE = "default"
    DELTACAT_URL_DEFAULT_TABLE_VERSION = "default"
    DELTACAT_URL_DEFAULT_STREAM = "default"

    def __init__(
        self,
        url: str,
    ):
        # TODO(pdames): Handle wildcard `*` and `**` at end of url.
        self.catalog_name = None
        self.parsed = urlparse(url, allow_fragments=False)  # support '#' in path
        self.url = self.parsed.geturl()
        path = self.parsed.path
        # Remove leading/trailing slashes and split the path into elements
        self.path_elements = [
            element for element in path.strip("/").split("/") if path and element
        ]
        # Split the scheme into the root DeltaCAT scheme and the path scheme
        self.scheme_elements = self.parsed.scheme.split("+")
        self.datastore_type = DatastoreType(self.scheme_elements[0])
        if len(self.scheme_elements) == 2:
            # Remove the source/sink type from the scheme.
            self.parsed = self.parsed._replace(scheme=self.scheme_elements[1])
            # Save the URL path to read/write w/o the source/sink type.
            self.url_path = urlunparse(self.parsed)
        elif len(self.scheme_elements) > 2:
            raise ValueError(f"Invalid DeltaCAT URL: {url}")
        self.query_params = parse_qs(self.parsed.query) if self.parsed.query else {}
        if self.datastore_type == DatastoreType.DELTACAT:
            self.catalog_name = self.parsed.netloc
            self.unresolved_namespace = (
                self.path_elements[0] if self.path_elements else None
            )
            self.table = self.path_elements[1] if len(self.path_elements) > 1 else None
            self.unresolved_table_version = (
                self.path_elements[2] if len(self.path_elements) > 2 else None
            )
            self.unresolved_stream = (
                self.path_elements[3] if len(self.path_elements) > 3 else None
            )
            self.partition = (
                self.path_elements[4] if len(self.path_elements) > 4 else None
            )
            self.delta = self.path_elements[5] if len(self.path_elements) > 5 else None
            self._resolve_deltacat_path_identifiers()
        elif self.datastore_type == DatastoreType.DELTACAT_NAMESPACE:
            self.catalog_name = DeltaCatUrl.DELTACAT_URL_DEFAULT_CATALOG
            self.unresolved_namespace = self.parsed.netloc
            self.table = self.path_elements[0] if self.path_elements else None
            self.unresolved_table_version = (
                self.path_elements[1] if len(self.path_elements) > 1 else None
            )
            self.unresolved_stream = (
                self.path_elements[2] if len(self.path_elements) > 2 else None
            )
            self.partition = (
                self.path_elements[3] if len(self.path_elements) > 3 else None
            )
            self.delta = self.path_elements[4] if len(self.path_elements) > 4 else None
            self._resolve_deltacat_path_identifiers()
        elif self.datastore_type == DatastoreType.DELTACAT_TABLE:
            self.catalog_name = DeltaCatUrl.DELTACAT_URL_DEFAULT_CATALOG
            self.unresolved_namespace = DeltaCatUrl.DELTACAT_URL_DEFAULT_NAMESPACE
            self.table = self.parsed.netloc
            self.unresolved_table_version = (
                self.path_elements[0] if self.path_elements else None
            )
            self.unresolved_stream = (
                self.path_elements[1] if len(self.path_elements) > 1 else None
            )
            self.partition = (
                self.path_elements[2] if len(self.path_elements) > 2 else None
            )
            self.delta = self.path_elements[3] if len(self.path_elements) > 3 else None
            self._resolve_deltacat_path_identifiers()

    def is_deltacat_catalog_url(self):
        return bool(self.catalog_name)

    def resolve_catalog(self):
        if self.catalog_name:
            if self.catalog_name.lower() == DeltaCatUrl.DELTACAT_URL_DEFAULT_CATALOG:
                self.catalog = None
            self.catalog: CatalogProperties = dc.get_catalog(self.catalog_name).inner
            if not isinstance(self.catalog, CatalogProperties):
                raise ValueError(
                    f"Expected catalog `{self.catalog_name}` to be a DeltaCAT "
                    f"catalog but found: {self.catalog}"
                )

    def _resolve_deltacat_path_identifiers(self):
        dc.raise_if_not_initialized()
        self.namespace = self.table_version = self.stream = None
        if self.unresolved_namespace:
            if (
                self.unresolved_namespace.lower()
                == DeltaCatUrl.DELTACAT_URL_DEFAULT_NAMESPACE
            ):
                self.namespace = DEFAULT_NAMESPACE
            else:
                self.namespace = self.unresolved_namespace
        if (
            self.unresolved_table_version
            and self.unresolved_table_version.lower()
            != DeltaCatUrl.DELTACAT_URL_DEFAULT_TABLE_VERSION
        ):
            self.table_version = self.unresolved_table_version
        if self.unresolved_stream:
            if (
                self.unresolved_stream.lower()
                == DeltaCatUrl.DELTACAT_URL_DEFAULT_STREAM
            ):
                self.stream = StreamFormat.DELTACAT
            else:
                self.stream = StreamFormat(self.unresolved_stream)

    def __str__(self):
        return self.url

    def __repr__(self):
        return self.url


def _list_table_versions(table: Table, catalog: CatalogProperties):
    return metastore.list_table_versions(
        namespace=table.namespace,
        table_name=table.table_name,
        catalog=catalog,
    )


def _list_streams(table_version: TableVersion, catalog: CatalogProperties):
    return metastore.list_streams(
        namespace=table_version.namespace,
        table_name=table_version.table_name,
        table_version=table_version.table_version,
        catalog=catalog,
    )


class DeltaCatUrlReader:
    def __init__(
        self,
        url: DeltaCatUrl,
        dataset_type: DatasetType = DatasetType.RAY_DATASET,
    ):
        self._url = url
        if url.is_deltacat_catalog_url():
            url.resolve_catalog()
            self._reader = DeltaCatUrlReader.resolve_dc_reader(url)
            self._listers = DeltaCatUrlReader.resolve_dc_listers(url)
        else:
            self._reader = DeltaCatUrlReader.dataset_and_datastore_type_to_reader(
                dataset_type,
                url.datastore_type,
            )

    @property
    def url(self) -> DeltaCatUrl:
        return self._url

    @property
    def listers(
        self,
    ) -> List[
        Tuple[
            Callable[[Any], ListResult[Metafile]],
            str,
            Callable[[Metafile], Union[Metafile, str]],
        ]
    ]:
        return self._listers

    def read(self, *args, **kwargs) -> Dataset:
        if self._url.is_deltacat_catalog_url():
            return self._reader(*args, **kwargs)
        else:
            return self._reader(self._url)(*args, **kwargs)

    @staticmethod
    def resolve_dc_reader(url: DeltaCatUrl) -> Callable:
        if url.delta:
            return functools.partial(
                metastore.get_delta,
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                partition_values=json.loads(url.partition),
                stream_position=url.delta,
                catalog=url.catalog,
            )
        if url.partition:
            return functools.partial(
                metastore.get_partition,
                stream_locator=StreamLocator.at(
                    namespace=url.namespace,
                    table_name=url.table,
                    table_version=url.table_version,
                    stream_id=None,
                    stream_format=url.stream,
                ),
                partition_values=json.loads(url.partition),
                catalog=url.catalog,
            )
        if url.unresolved_stream:
            return functools.partial(
                metastore.get_stream,
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                stream_format=url.stream,
                catalog=url.catalog,
            )
        if url.unresolved_table_version:
            return functools.partial(
                metastore.get_table_version,
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                catalog=url.catalog,
            )
        if url.table:
            return functools.partial(
                metastore.get_table,
                namespace=url.namespace,
                table_name=url.table,
                catalog=url.catalog,
            )
        if url.unresolved_namespace:
            return functools.partial(
                metastore.get_namespace,
                namespace=url.namespace,
                catalog=url.catalog,
            )
        if url.catalog_name:
            return functools.partial(
                dc.get_catalog,
                name=url.catalog_name,
            )
        raise ValueError("No DeltaCAT object to read.")

    @staticmethod
    def resolve_dc_listers(
        url: DeltaCatUrl,
    ) -> List[
        Tuple[
            Callable[[Any], ListResult[Metafile]],
            Optional[str],
            Optional[Callable[[Metafile], Union[Metafile, str]]],
        ]
    ]:
        if url.partition:
            partition_locator = PartitionLocator.at(
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                stream_id=None,
                stream_format=url.stream,
                partition_values=json.loads(url.partition),
                partition_id=None,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                partition_like=partition_locator,
                catalog=url.catalog,
            )
            return [(delta_lister, None, None)]
        if url.unresolved_stream:
            stream_locator = StreamLocator.at(
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                stream_id=None,
                stream_format=url.stream,
            )
            stream = Stream.of(
                locator=stream_locator,
                partition_scheme=None,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                stream=stream,
                catalog=url.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=url.catalog,
            )
            return [
                (partition_lister, None, None),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if url.unresolved_table_version:
            stream_lister = functools.partial(
                metastore.list_streams,
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                catalog=url.catalog,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=url.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=url.catalog,
            )
            return [
                (stream_lister, None, None),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if url.table:
            table_version_lister = functools.partial(
                metastore.list_table_versions,
                namespace=url.namespace,
                table_name=url.table,
                catalog=url.catalog,
            )
            stream_lister = functools.partial(
                metastore.list_streams,
                namespace=url.namespace,
                table_name=url.table,
                catalog=url.catalog,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=url.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=url.catalog,
            )
            return [
                (table_version_lister, None, None),
                (stream_lister, "table_version", lambda x: x.table_version),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if url.unresolved_namespace:
            table_lister = functools.partial(
                metastore.list_tables,
                namespace=url.namespace,
                catalog=url.catalog,
            )
            table_version_lister = functools.partial(
                metastore.list_table_versions,
                namespace=url.namespace,
                catalog=url.catalog,
            )

            def stream_lister_with_table_version(table_version, **kwargs):
                return metastore.list_streams(
                    namespace=url.namespace,
                    table_name=table_version.table_name,
                    table_version=table_version.table_version,
                    catalog=url.catalog,
                    **kwargs,
                )

            stream_lister = stream_lister_with_table_version
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=url.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=url.catalog,
            )
            return [
                (table_lister, None, None),
                (table_version_lister, "table_name", lambda x: x.table_name),
                (stream_lister, "table_version", lambda x: x),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        if url.catalog_name:
            namespace_lister = functools.partial(
                metastore.list_namespaces,
                catalog=url.catalog,
            )
            table_lister = functools.partial(
                metastore.list_tables,
                catalog=url.catalog,
            )
            table_version_lister = functools.partial(
                _list_table_versions,
                catalog=url.catalog,
            )
            stream_lister = functools.partial(
                _list_streams,
                catalog=url.catalog,
            )
            partition_lister = functools.partial(
                metastore.list_stream_partitions,
                catalog=url.catalog,
            )
            delta_lister = functools.partial(
                metastore.list_partition_deltas,
                catalog=url.catalog,
            )
            return [
                (namespace_lister, None, None),
                (table_lister, "namespace", lambda x: x.namespace),
                (table_version_lister, "table", lambda x: x),
                (stream_lister, "table_version", lambda x: x),
                (partition_lister, "stream", lambda x: x),
                (delta_lister, "partition_like", lambda x: x),
            ]
        raise ValueError("No DeltaCAT objects to list.")

    @staticmethod
    def dataset_and_datastore_type_to_reader(
        dataset_type: DatasetType,
        datastore_type: DatastoreType,
    ):
        reader_resolver = DATASET_TYPE_TO_DATASTORE_TYPE_READER_RESOLVER.get(
            dataset_type
        )
        if reader_resolver is None:
            raise ValueError(
                f"Unsupported dataset type: {dataset_type}. "
                f"Supported dataset types: {[dt.name for dt in DatasetType]}"
            )
        reader = reader_resolver.get(datastore_type)
        if reader is None:
            raise ValueError(
                f"Dataset type `{dataset_type} has no reader for "
                f"datastore type: `{datastore_type}`."
                f"Supported datastore types: {[k.name for k in reader_resolver.keys()]}"
            )
        return reader


def _stage_and_commit_stream(
    stream: Stream,
    *args,
    **kwargs,
) -> Stream:
    """
    Helper method to stage and commit a stream (e.g., as part of a copy
    operation from another catalog). The committed stream will be assigned a
    different unique ID than the input stream.
    """
    stream = metastore.stage_stream(
        namespace=stream.namespace,
        table_name=stream.table_name,
        table_version=stream.table_version,
        stream_format=StreamFormat(stream.stream_format),
        *args,
        **kwargs,
    )
    return metastore.commit_stream(
        stream=stream,
        *args,
        **kwargs,
    )


def _stage_and_commit_partition(
    partition: Partition,
    *args,
    **kwargs,
) -> Partition:
    """
    Helper method to stage and commit a partition (e.g., as part of a copy
    operation from another catalog). The committed partition will be assigned a
    different unique ID than the input partition.
    """
    stream = metastore.get_stream(
        namespace=partition.namespace,
        table_name=partition.table_name,
        table_version=partition.table_version,
        stream_format=StreamFormat(
            partition.stream_format or StreamFormat.DELTACAT.value
        ),
        *args,
        **kwargs,
    )
    partition = metastore.stage_partition(
        stream=stream,
        partition_values=partition.partition_values,
        partition_scheme_id=partition.partition_scheme_id,
        *args,
        **kwargs,
    )
    return metastore.commit_partition(
        partition=partition,
        *args,
        **kwargs,
    )


def _copy_source_manifest_entries_to_destination(
    delta: Delta,
    dest_catalog: CatalogProperties,
    source_catalog: Optional[CatalogProperties],
) -> Delta:
    """
    Copy source catalog delta manifest entries to the same path in the destination catalog.
    """
    # This is part of a cross-catalog copy.
    # Copy source manifest entries to the same path in the destination catalog.
    dest_filesystem = dest_catalog.filesystem
    source_filesystem = source_catalog.filesystem

    # Copy each file to the same relative path in destination catalog
    # TODO(pdames): Parallelize copy.
    logger.info(
        f"Copying {len(delta.manifest.entries)} manifest entries from source catalog {source_catalog.root} to destination catalog {dest_catalog.root}."
    )
    for entry in delta.manifest.entries:
        # Copy source to same relative path in destination
        source_url = reconstruct_manifest_entry_url(entry, catalog=source_catalog).url
        dest_url = reconstruct_manifest_entry_url(entry, catalog=dest_catalog).url

        # Ensure destination directory exists
        dest_dir = posixpath.dirname(dest_url)
        dest_filesystem.create_dir(dest_dir, recursive=True)
        if type(source_filesystem) is type(dest_filesystem):
            # source/dest of same filesystem type can be copied directly
            dest_filesystem.copy_file(source_url, dest_url)
        else:
            # different source/dest filesystem types need separate reader/writer
            with source_filesystem.open_input_stream(source_url) as src:
                with dest_filesystem.open_output_stream(dest_url) as dst:
                    dst.write(src.read())


def _stage_and_commit_delta(
    delta: Delta,
    dest_catalog: CatalogProperties,
    source_catalog: Optional[CatalogProperties],
    *args,
    **kwargs,
) -> Delta:
    """
    Helper method to stage and commit a delta (e.g., as part of a copy operation from another catalog).
    """
    if source_catalog and source_catalog.root != dest_catalog.root and delta.manifest:
        # Stage the delta in the dest catalog by copying its manifest entries to the destination.
        _copy_source_manifest_entries_to_destination(
            delta=delta,
            dest_catalog=dest_catalog,
            source_catalog=source_catalog,
        )
    return metastore.commit_delta(
        delta=delta,
        catalog=dest_catalog,
        *args,
        **kwargs,
    )


class DeltaCatUrlWriter:
    def __init__(
        self,
        url: DeltaCatUrl,
        dataset_type: DatasetType = DatasetType.RAY_DATASET,
        metafile: Optional[Metafile] = None,
    ):
        self._url = url
        self._metafile = metafile

        if url.is_deltacat_catalog_url():
            if url.path_elements:
                url.resolve_catalog()
            self._writer = DeltaCatUrlWriter.resolve_dc_writer(url, metafile or {})
        else:
            self._writer = DeltaCatUrlWriter.dataset_and_datastore_type_to_writer(
                dataset_type,
                url.datastore_type,
            )

    @property
    def url(self) -> DeltaCatUrl:
        return self._url

    @property
    def metafile(self) -> Metafile:
        return self._metafile

    def write(self, suffix: str = "", *args, **kwargs) -> Union[Metafile, str]:
        if self._url.is_deltacat_catalog_url():
            return self._writer(*args, **kwargs)
        else:
            dest_url = DeltaCatUrl(f"{self._url.url}{suffix}")
            self._writer(dest_url)(*args, **kwargs)
            return dest_url.url_path

    @staticmethod
    def resolve_dc_writer(
        url: DeltaCatUrl,
        metafile: Metafile,
    ) -> Callable:
        if url.delta:
            delta: Delta = Delta(
                Metafile.based_on(
                    other=metafile,
                    new_id=url.delta,
                )
            )
            delta.locator = DeltaLocator.at(
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                stream_id=None,
                stream_format=url.stream,
                partition_values=json.loads(url.partition),
                partition_id=None,
                stream_position=int(url.delta),
            )
            # TODO(pdames): Also allow shallow copies? For deltas whose
            #  manifests reference local files, shallow delta copies may be
            #  invalid in the target catalog, and should be blocked or
            #  converted to a deep copy automatically.
            return functools.partial(
                _stage_and_commit_delta,
                delta=delta,
                dest_catalog=url.catalog,
                source_catalog=metafile.catalog,
            )
        if url.partition:
            partition: Partition = Partition(metafile)
            partition.locator = PartitionLocator.at(
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                stream_id=None,
                stream_format=url.stream,
                partition_values=json.loads(url.partition),
                partition_id=None,
            )
            return functools.partial(
                _stage_and_commit_partition,
                partition=partition,
                catalog=url.catalog,
            )
        if url.unresolved_stream:
            stream: Stream = Stream(metafile)
            stream.locator = StreamLocator.at(
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
                stream_id=None,
                stream_format=url.stream,
            )
            return functools.partial(
                _stage_and_commit_stream,
                stream=stream,
                catalog=url.catalog,
            )
        if url.unresolved_table_version:
            table_version: TableVersion = TableVersion(metafile)
            table_version.locator = TableVersionLocator.at(
                namespace=url.namespace,
                table_name=url.table,
                table_version=url.table_version,
            )
            return functools.partial(
                metastore.create_table_version,
                namespace=table_version.namespace,
                table_name=table_version.table_name,
                table_version=table_version.table_version,
                lifecycle_state=table_version.state,
                schema=table_version.schema,
                partition_scheme=table_version.partition_scheme,
                sort_keys=table_version.sort_scheme,
                table_version_description=table_version.description,
                table_version_properties=table_version.properties,
                supported_content_types=table_version.content_types,
                catalog=url.catalog,
            )
        if url.table:
            table: Table = Table(metafile)
            table.locator = TableLocator.at(
                namespace=url.namespace,
                table_name=url.table,
            )
            return functools.partial(
                metastore.create_table,
                namespace=table.namespace,
                table_name=table.table_name,
                description=table.description,
                properties=table.properties,
                catalog=url.catalog,
            )
        if url.unresolved_namespace:
            namespace: Namespace = Namespace(metafile)
            namespace.locator = NamespaceLocator.of(
                namespace=url.namespace,
            )
            return functools.partial(
                metastore.create_namespace,
                namespace=url.namespace,
                properties=namespace.properties,
                catalog=url.catalog,
            )
        if url.catalog_name:
            return functools.partial(
                dc.put_catalog,
                name=url.catalog_name,
            )
        raise ValueError("No DeltaCAT object to write.")

    @staticmethod
    def dataset_and_datastore_type_to_writer(
        dataset_type: DatasetType,
        datastore_type: DatastoreType,
    ):
        writer_resolver = DATASET_TYPE_TO_DATASTORE_TYPE_WRITER_RESOLVER.get(
            dataset_type
        )
        if writer_resolver is None:
            raise ValueError(
                f"Unsupported dataset type: {dataset_type}. "
                f"Supported dataset types: {[dt.name for dt in DatasetType]}"
            )
        writer = writer_resolver.get(datastore_type)
        if writer is None:
            raise ValueError(
                f"Dataset type `{dataset_type} has no writer for "
                f"datastore type: `{datastore_type}`."
                f"Supported datastore types: {[k.name for k in writer_resolver.keys()]}"
            )
        return writer
