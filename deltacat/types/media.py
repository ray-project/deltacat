from enum import Enum
from typing import Set, Dict


class ContentType(str, Enum):
    """
    Enumeration used to resolve a file's entity-body Media Type (formerly known
    as MIME type). All content types here are writeable by at least one
    :class:`deltacat.types.media.DatasetType`. The Media Type is used as the
    content type of each :class:`deltacat.storage.model.manifest.ManifestEntry`
    written by that dataset type.

    https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17

    https://www.iana.org/assignments/media-types/media-types.xhtml
    """

    # IANA registered types
    AVRO = "application/avro"
    BINARY = "application/octet-stream"
    CSV = "text/csv"
    JSON = "application/json"

    # unregistered types
    FEATHER = "application/feather"
    ORC = "application/orc"
    PARQUET = "application/parquet"
    PSV = "text/psv"
    TSV = "text/tsv"
    UNESCAPED_TSV = "application/x-amzn-unescaped-tsv"


class ContentEncoding(str, Enum):
    """
    Enumeration used as a modifier for :class:`deltacat.types.media.ContentType`
    to indicate that additional encodings have been applied to the entity-body
    Media Type.

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


# Map of file extensions to content types
EXT_TO_CONTENT_TYPE: Dict[str, ContentType] = {
    ".parquet": ContentType.PARQUET,
    ".pq": ContentType.PARQUET,
    ".csv": ContentType.CSV,
    ".tsv": ContentType.TSV,
    ".psv": ContentType.PSV,
    ".json": ContentType.JSON,
    ".feather": ContentType.FEATHER,
    ".avro": ContentType.AVRO,
    ".orc": ContentType.ORC,
}

# Inverse map of content types to file extensions
CONTENT_TYPE_TO_EXT: Dict[ContentType, str] = {
    v: k for k, v in EXT_TO_CONTENT_TYPE.items()
}

# Map of file extensions to content encodings
EXT_TO_CONTENT_ENCODING: Dict[str, ContentEncoding] = {
    ".gz": ContentEncoding.GZIP,
    ".bz2": ContentEncoding.BZIP2,
    ".zst": ContentEncoding.ZSTD,
    ".sz": ContentEncoding.SNAPPY,
    ".zz": ContentEncoding.DEFLATE,
    ".zip": ContentEncoding.DEFLATE,
}

# Inverse map of content encodings to file extensions
CONTENT_ENCODING_TO_EXT: Dict[ContentEncoding, str] = {
    v: k for k, v in EXT_TO_CONTENT_ENCODING.items()
}

SCHEMA_CONTENT_TYPES: Set[str] = {
    ContentType.PARQUET.value,
    ContentType.ORC.value,
    ContentType.FEATHER.value,
    ContentType.AVRO.value,
}

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

    def readable_content_types(self) -> Set[ContentType]:
        # if this is DAFT then it can read PARQUET, JSON, and CSV
        if self == DatasetType.DAFT:
            return {
                ContentType.PARQUET,
                ContentType.JSON,
                ContentType.CSV,
                ContentType.PSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
            }
        if self == DatasetType.RAY_DATASET:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
                ContentType.FEATHER,
            }
        if self == DatasetType.PYARROW:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.PANDAS:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.POLARS:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.NUMPY:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.PYARROW_PARQUET:
            return {ContentType.PARQUET}
        raise ValueError(f"No readable content types for {self}")

    def writable_content_types(self) -> Set[ContentType]:
        if self == DatasetType.PYARROW:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.PANDAS:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.POLARS:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.RAY_DATASET:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.JSON,
            }
        if self == DatasetType.DAFT:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.JSON,
            }
        if self == DatasetType.NUMPY:
            return {
                ContentType.CSV,
                ContentType.TSV,
                ContentType.UNESCAPED_TSV,
                ContentType.PSV,
                ContentType.PARQUET,
                ContentType.FEATHER,
                ContentType.JSON,
                ContentType.AVRO,
                ContentType.ORC,
            }
        if self == DatasetType.PYARROW_PARQUET:
            return {}
        raise ValueError(f"No writable content types for {self}")

    def can_read(self, content_type: ContentType) -> bool:
        return content_type in self.readable_content_types()

    def can_write(self, content_type: ContentType) -> bool:
        return content_type in self.writable_content_types()


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


DATASET_TYPE_TO_SUPPORTED_READ_CONTENT_TYPES: Dict[DatasetType, Set[str]] = {
    DatasetType.DAFT: {
        ContentType.CSV,
        ContentType.PARQUET,
        ContentType.JSON,
    },
    DatasetType.RAY_DATASET: {
        ContentType.CSV,
        ContentType.TSV,
        ContentType.UNESCAPED_TSV,
        ContentType.PSV,
        ContentType.PARQUET,
        ContentType.JSON,
        ContentType.AVRO,
        ContentType.ORC,
        ContentType.FEATHER,
    },
}


class DatastoreType(str, Enum):
    """
    Enumeration used to identify the type of reader required to connect to and
    correctly interpret data stored at a given path. Typically used together
    with :class:`deltacat.types.media.DatasetType` to resolve a reader or
    writer for that data store. Note that, although some overlap exists between
    enum values here and in :class:`deltacat.types.media.ContentType`, each
    enum serve a different purpose. The purpose of
    :class:`deltacat.types.media.ContentType` is to resolve a file's MIME type,
    and may be used together with datastores that support storing different
    file types to describe the specific file type read/written from/to that
    datastore (e.g., DeltaCAT, Iceberg, Hudi, Delta Lake, Audio, Images, Video,
    etc.)
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
