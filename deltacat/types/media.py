from enum import Enum
from typing import Set, Dict


class ContentType(Enum):
    # See also: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    UNESCAPED_TSV = "application/x-amzn-unescaped-tsv"
    TSV = "text/tsv"
    CSV = "text/csv"
    PSV = "text/psv"
    PARQUET = "application/parquet"
    RC = "application/rc"
    SEQUENCE = "application/sequence"
    ORC = "application/orc"
    ION = "application/x-amzn-ion"
    JSON = "application/json"
    FEATHER = "application/feather"


class ContentEncoding(Enum):
    # See also: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.11
    GZIP = "gzip"
    DEFLATE = "deflate"
    IDENTITY = "identity"
    BZIP2 = "bzip2"
    SNAPPY = "snappy"


class TableType(Enum):
    PYARROW = 1
    PANDAS = 2
    NUMPY = 3


DELIMITED_TEXT_CONTENT_TYPES: Set[str] = {
    ContentType.UNESCAPED_TSV.value,
    ContentType.TSV.value,
    ContentType.CSV.value,
    ContentType.PSV.value
}


EXPLICIT_COMPRESSION_CONTENT_TYPES: Set[str] = {
    ContentType.UNESCAPED_TSV.value,
    ContentType.TSV.value,
    ContentType.CSV.value,
    ContentType.PSV.value,
    ContentType.JSON.value
}

CONTENT_TYPE_TO_USER_KWARGS_KEY: Dict[str, str] = {
    ContentType.UNESCAPED_TSV.value: "csv",
    ContentType.TSV.value: "csv",
    ContentType.CSV.value: "csv",
    ContentType.PSV.value: "csv",
    ContentType.PARQUET.value: "parquet",
    ContentType.FEATHER.value: "feather",
    ContentType.ORC.value: "orc",
    ContentType.JSON.value: "json",
}
