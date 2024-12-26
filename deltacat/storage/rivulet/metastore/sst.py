from dataclasses import dataclass
from typing import Protocol, Any, List

from deltacat.storage.rivulet.fs.input_file import InputFile
from deltacat.storage.rivulet.fs.output_file import OutputFile


@dataclass(frozen=True)
class SSTableRow:
    """
    Row of Sorted String Table.

    The metadata for a SSTable row referencing a offset-range of a data file containing data sorted by primary key.

    Note that the actual file format for SSTables can omit some of these fields (e.g. key_end and offset_end) taking
    advantage of sorted nature of file.
    """

    key_min: Any
    """The first primary key found in referenced data range, inclusive."""
    key_max: Any
    """the last primary key found in referenced data range, inclusive"""
    uri: str
    """The URI of the data file containing this row's data.
    Will be format dependent, e.g. file://<absolute_path> or s3://<bucket>/<key>
    Note that if uri_prefix is specified in SSTable, this will just be a postfix
    """
    offset_start: int
    """
    offset start for data range within uri.
    Note this offset is format dependent - e.g. for Parquet it will be zero-indexed row group
    For other formats it will be byte offset into file
    """
    offset_end: int
    """
    offset end for data range within uri.
    """

    """The starting offset into the data file for data referenced by this row.
    Note that offset is format dependent.
        E.g. for parquet files it is row group, for other formats it is byte offset
    """


@dataclass(frozen=True)
class SSTable:
    """
    In memory representation of Sorted String Table

    List of references to data file ranges with statistics to enable pruning by primary key.
    """

    rows: List[SSTableRow]
    """Sorted List of rows by key"""
    min_key: Any
    """Minimum observed primary key across all rows."""
    max_key: Any
    """Maximum observed primary key across all rows."""


class SSTWriter(Protocol):
    """
    interface for writing SST files

    Rows may be added iteratively. Input rows (within add_rows batch or across batches) MUST be sorted
        by key_min
    """

    def write(self, file: OutputFile, rows: List[SSTableRow]) -> None:
        """
        Writes SST file
        """
        ...


class SSTReader(Protocol):
    """
    interface for reading SST files
    """

    def read(self, file: InputFile) -> SSTable:
        ...
