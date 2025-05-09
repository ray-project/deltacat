from enum import Enum
from typing import Set


class ContentType(str, Enum):
    """
    Enumeration used to resolve the entity-body Media Type (formerly known as
    MIME type) in an HTTP request.

    https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17

    https://www.iana.org/assignments/media-types/media-types.xhtml
    """

    # IANA registered types
    AVRO = "application/avro"
    BINARY = "application/octet-stream"
    CSV = "text/csv"
    HDF = "application/x-hdf"
    HTML = "text/html"
    JSON = "application/json"
    TEXT = "text/plain"
    WEBDATASET = "application/x-web-dataset"
    XML = "text/xml"

    # unregistered types
    FEATHER = "application/feather"
    ION = "application/x-amzn-ion"
    ORC = "application/orc"
    PARQUET = "application/parquet"
    PSV = "text/psv"
    TSV = "text/tsv"
    UNESCAPED_TSV = "application/x-amzn-unescaped-tsv"


class ContentEncoding(str, Enum):
    """
    Enumeration used as a modifier for :class:`deltacat.types.media.ContentType`
    to indicate that additional encodings have been applied to the entity-body
    Media Type in an HTTP request.

    https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11

    http://www.iana.org/assignments/http-parameters/http-parameters.xhtml#content-coding
    """

    # IANA registered encodings
    GZIP = "gzip"
    DEFLATE = "deflate"
    IDENTITY = "identity"
    ZSTD = "zstd"

    # unregistered encodings
    BZIP2 = "bzip2"
    SNAPPY = "snappy"


DELIMITED_TEXT_CONTENT_TYPES: Set[str] = {
    ContentType.UNESCAPED_TSV.value,
    ContentType.TSV.value,
    ContentType.CSV.value,
    ContentType.PSV.value,
}

TABULAR_CONTENT_TYPES: Set[str] = {
    ContentType.UNESCAPED_TSV.value,
    ContentType.TSV.value,
    ContentType.CSV.value,
    ContentType.PSV.value,
    ContentType.PARQUET.value,
    ContentType.ORC.value,
    ContentType.FEATHER.value,
    ContentType.AVRO.value,
}

EXPLICIT_COMPRESSION_CONTENT_TYPES: Set[str] = {
    ContentType.UNESCAPED_TSV.value,
    ContentType.TSV.value,
    ContentType.CSV.value,
    ContentType.PSV.value,
    ContentType.JSON.value,
}


class DatasetType(str, Enum):
    """
    Enumeration used to identify the in-memory local or distributed dataset
    to be used for file IO, queries, and data transformation. Typically used
    together with :class:`deltacat.types.media.DatastoreType` to resolve the
    compute layer that will be responsible for reading, transforming, and
    writing data to a given datastore.
    """

    # local
    NUMPY = "numpy"  # numpy.ndarray
    PANDAS = "pandas"  # pandas.DataFrame
    POLARS = "polars"  # polars.DataFrame
    PYARROW = "pyarrow"  # pyarrow.Table
    PYARROW_PARQUET = "pyarrow_parquet"  # pyarrow.parquet.ParquetFile

    # distributed
    DAFT = "daft"  # daft.DataFrame
    RAY_DATASET = "ray_dataset"  # ray.data.Dataset

    @staticmethod
    def distributed():
        return {
            DatasetType.DAFT,
            DatasetType.RAY_DATASET,
        }

    @staticmethod
    def local():
        return {
            DatasetType.NUMPY,
            DatasetType.PANDAS,
            DatasetType.POLARS,
            DatasetType.PYARROW,
            DatasetType.PYARROW_PARQUET,
        }


# deprecated by DatasetType - populated dynamically for backwards compatibility
TableType = Enum(
    "TableType",
    {d.name: d.value for d in DatasetType.local()},
)

# deprecated by DatasetType - populated dynamically for backwards compatibility
DistributedDatasetType = Enum(
    "DistributedDatasetType",
    {d.name: d.value for d in DatasetType.distributed()},
)


# deprecated by DatasetType.local() and DatasetType.distributed()
# kept for backwards compatibility
class StorageType(str, Enum):
    LOCAL = "local"
    DISTRIBUTED = "distributed"


class DatastoreType(str, Enum):
    """
    Enumeration used to identify the type of reader required to connect to and
    correctly interpret data stored at a given path. Typically used together
    with :class:`deltacat.types.media.DatasetType` to resolve a reader or
    writer for that data store. Note that, although some overlap exists between
    enum values here and in :class:`deltacat.types.media.ContentType`, each
    enum serve a different purpose. The purpose of
    :class:`deltacat.types.media.ContentType` is to resolve the MIME type for
    specific types of files, and may be used together with multi-content-type
    datastore types to describe the specific file types read/written to that
    datastore (e.g., Iceberg, Hudi, Delta Lake, Audio, Images, Video, etc.)
    """

    # DeltaCAT Catalog Datasets
    DELTACAT = "dc"
    DELTACAT_NAMESPACE = "namespace"
    DELTACAT_TABLE = "table"
    DELTACAT_TABLE_VERSION = "tableversion"
    DELTACAT_STREAM = "stream"
    DELTACAT_PARTITION = "partition"
    DELTACAT_DELTA = "delta"

    # External Datasets
    AUDIO = "audio"
    AVRO = "avro"
    BIGQUERY = "bigquery"
    BINARY = "binary"
    CSV = "csv"
    CLICKHOUSE = "clickhouse"
    DATABRICKS_TABLES = "databricks"
    DELTA_LAKE = "deltalake"
    DELTA_SHARING = "deltasharing"
    FEATHER = "feather"
    HDF = "hdf"
    HTML = "html"
    HUDI = "hudi"
    ICEBERG = "iceberg"
    IMAGES = "images"
    JSON = "json"
    LANCE = "lance"
    MONGO = "mongodb"
    NUMPY = "numpy"
    ORC = "orc"
    PARQUET = "parquet"
    TEXT = "text"
    TFRECORDS = "tfrecords"
    VIDEOS = "videos"
    WARC = "warc"
    WEBDATASET = "webdataset"
    XML = "xml"
