from typing import Protocol, Iterable, List, Union, Any, Dict

from deltacat.storage.rivulet.metastore.sst import SSTableRow
import pyarrow as pa

MEMTABLE_DATA = Union[Iterable[Dict[str, Any]], pa.Table]


class DataSerializer(Protocol):
    """
    Interface for writing data only.

    As data is written, it must emit sufficient metadata to build SSTable
    Each format will have a specific data writer (e.g. ParquetDataWriter)

    TODO future improvements:
        1. How does data writer control how it chooses to write to existing files vs new files?
            For now, we will not expose this configuration and always write each batch to
            a new file
        2. Related to 1, how should we expose URI(s) to write to? Probably DataWriter can
            use FileProvider and needs to know relevant ids like task ID.
    """

    def flush_batch(self, sorted_records: MEMTABLE_DATA) -> List[SSTableRow]:
        """
        Flushes rows to file, and return appropriate metadata to build SSTable

        TODO future improvements
         1. Finalize type for input records (instead of MvpRow)

            Options could be:
            (a) Something like Iceberg "StructLike" which allows flexible integrations without memcopy for row-oriented formats, e.g. can make Spark InternalRow structlike
            (b) use arrow. We will probably use arrow for writing parquet, although
                probably it isn't ideal for row oriented formats
         2. Keep in mind, most implementation of DataWriter will be written in rust.

        :param sorted_records: Records sorted by key
        :return: metadata used to build SS Table
        """
        ...
