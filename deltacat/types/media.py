from enum import Enum
from typing import Dict, Set


class ContentType(str, Enum):
    # See also:
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    # https://www.iana.org/assignments/media-types/media-types.xhtml

    # IANA registered types
    CSV = "text/csv"
    JSON = "application/json"

    # unregistered types
    TSV = "text/tsv"
    PSV = "text/psv"
    PARQUET = "application/parquet"
    ORC = "application/orc"
    FEATHER = "application/feather"
    UNESCAPED_TSV = "application/x-amzn-unescaped-tsv"
    ION = "application/x-amzn-ion"


class ContentEncoding(str, Enum):
    # See also:
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11
    # http://www.iana.org/assignments/http-parameters/http-parameters.xhtml#content-coding

    # IANA registered encodings
    GZIP = "gzip"
    DEFLATE = "deflate"
    IDENTITY = "identity"
    ZSTD = "zstd"

    # unregistered encodings
    BZIP2 = "bzip2"
    SNAPPY = "snappy"


class TableType(str, Enum):
    PYARROW = "pyarrow"
    PANDAS = "pandas"
    NUMPY = "numpy"
    PYARROW_PARQUET = "pyarrow_parquet"


class DistributedDatasetType(str, Enum):
    DAFT = "daft"
    RAY_DATASET = "ray_dataset"


class SchemaType(str, Enum):
    ARROW = "arrow"


class StorageType(str, Enum):
    LOCAL = "local"
    DISTRIBUTED = "distributed"


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
}

EXPLICIT_COMPRESSION_CONTENT_TYPES: Set[str] = {
    ContentType.UNESCAPED_TSV.value,
    ContentType.TSV.value,
    ContentType.CSV.value,
    ContentType.PSV.value,
    ContentType.JSON.value,
}

CONTENT_TYPE_TO_USER_KWARGS_KEY: Dict[str, str] = {
    ContentType.UNESCAPED_TSV.value: "unescaped_tsv",
    ContentType.TSV.value: "csv",
    ContentType.CSV.value: "csv",
    ContentType.PSV.value: "csv",
    ContentType.PARQUET.value: "parquet",
    ContentType.FEATHER.value: "feather",
    ContentType.ORC.value: "orc",
    ContentType.JSON.value: "json",
}
