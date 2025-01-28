from abc import ABC, abstractmethod
from typing import Iterator, List, Any
import pyarrow as pa

from deltacat.storage.rivulet.metastore.sst import SSTableRow
from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.serializer import DataSerializer, MEMTABLE_DATA
from deltacat.storage.rivulet.fs.file_provider import FileProvider


class ArrowSerializer(DataSerializer, ABC):
    """
    Utility class which can serialize data by first converting to arrow as intermediate format
    and then using the provided serialization function
    """

    def __init__(self, file_provider: FileProvider, schema: Schema):
        self.schema = schema
        self.file_provider = file_provider
        self.arrow_schema = self.schema.to_pyarrow()

    @abstractmethod
    def serialize(self, table: pa.Table) -> List[SSTableRow]:
        """
        Write an Arrow table to the specified output file

        :param table: PyArrow table to write
        :return: Number of row groups in the written file
        """
        pass

    def _to_arrow_table(self, sorted_records: MEMTABLE_DATA) -> pa.Table:
        """
        Convert input records to an Arrow table
        """
        if isinstance(sorted_records, pa.Table):
            return sorted_records
        elif isinstance(sorted_records, (Iterator, List)):
            return pa.Table.from_pylist(sorted_records, schema=self.arrow_schema)
        else:
            raise ValueError(f"Unsupported record type: {type(sorted_records)}")

    def _get_min_max_key(self, table: pa.Table) -> (Any, Any):
        """
        Get min and max values for the merge key from the table
        """
        key_col = table[self.schema.get_merge_key()]
        return key_col[0].as_py(), key_col[len(key_col) - 1].as_py()

    def flush_batch(self, sorted_records: MEMTABLE_DATA) -> List[SSTableRow]:
        """
        Write records to new parquet file as row group
        For now, we will depend on pyarrow to write to parquet

        :param sorted_records: record batch in SORTED ORDER
        :return: metadata for constructing SSTable
        """
        if not sorted_records:
            return []

        table = self._to_arrow_table(sorted_records)
        return self.serialize(table)

    def write(self, sorted_records: MEMTABLE_DATA) -> List[SSTableRow]:
        """
        Write records using the provided serialization function

        :param sorted_records: record batch in SORTED ORDER
        :return: metadata for constructing SSTable
        """
        if not sorted_records:
            return []

        table = self._to_arrow_table(sorted_records)
        return self.serialize(table)
