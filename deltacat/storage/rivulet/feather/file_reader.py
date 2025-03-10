from __future__ import annotations

from typing import Optional

import pyarrow.ipc
from pyarrow import RecordBatch, RecordBatchFileReader

from deltacat.storage.rivulet.fs.file_provider import FileProvider
from deltacat.storage.rivulet.metastore.sst import SSTableRow
from deltacat.storage.rivulet.reader.data_reader import (
    RowAndKey,
    FileReader,
    FILE_FORMAT,
)
from deltacat.storage.rivulet.reader.pyarrow_data_reader import RecordBatchRowIndex
from deltacat.storage.rivulet.schema.schema import Schema


class FeatherFileReader(FileReader[RecordBatchRowIndex]):
    """
    Feather file reader. This class is not thread safe

    This is mostly a copy-pasta from ParquetFileReader
    TODO can consider abstracting code between this and ParquetFileReader
    """

    def __init__(
        self,
        sst_row: SSTableRow,
        file_provider: FileProvider,
        primary_key: str,
        schema: Schema,
    ):
        self.sst_row = sst_row
        self.input = file_provider.provide_input_file(sst_row.uri)

        self.key = primary_key
        self.feather_file = sst_row.uri

        self.schema = schema

        # Iterator from pyarrow iter_batches API call. Pyarrow manages state of traversal within parquet row groups

        """
        These variables keep state about where the iterator is current at. They are initialized in __enter__()
        """
        self._curr_batch: RecordBatch | None = None
        self._feather_reader: RecordBatchFileReader | None = None
        # Arrow only lets you read feather files chunk by chunk using
        # RecordBatchFileReader.get_batch(index)
        self._curr_batch_index = 0
        self._curr_row_offset = 0
        self._pk_col = None

    def peek(self) -> Optional[RowAndKey[FILE_FORMAT]]:
        """
        Peek next record

        Note that there is an edge case where peek() is called on the bounary between record batches
        This only happens curr_row_offset == curr_batch.num_rows, meaning next() or peek() would need to advance
        to the next record batch. When this happens, peek() increments _curr_batch and sets _curr_row_offset to 0

        :return: Optional of RowAndPrimaryKey
        """
        if not self.__is_initialized():
            raise RuntimeError(
                "ParquetFileReader must be initialized with __enter__ before reading"
            )

        if self.__need_to_advance_record_batch():
            try:
                self.__advance_record_batch()
            except StopIteration:
                return None

        pk = self._pk_col[self._curr_row_offset].as_py()
        return RowAndKey(
            RecordBatchRowIndex(self._curr_batch, self._curr_row_offset), pk
        )

    def __next__(self) -> RowAndKey[FILE_FORMAT]:
        if not self.__is_initialized():
            raise RuntimeError(
                "ParquetFileReader must be initialized with __enter__ before reading"
            )

        if self.__need_to_advance_record_batch():
            self.__advance_record_batch()
            pk = self._pk_col[0].as_py()
            return RowAndKey(RecordBatchRowIndex(self._curr_batch, 0), pk)
        else:
            pk = self._pk_col[self._curr_row_offset].as_py()
            offset = self._curr_row_offset
            self._curr_row_offset += 1
            return RowAndKey(RecordBatchRowIndex(self._curr_batch, offset), pk)

    def __enter__(self):
        with self.input.open() as f:
            self._feather_reader = pyarrow.ipc.RecordBatchFileReader(f)
            self.__advance_record_batch()

    def __exit__(self, __exc_type, __exc_value, __traceback):
        self.close()
        # return False to propagate up error messages
        return False

    def close(self):
        # no op
        return

    def __is_initialized(self):
        return self._curr_batch and self._pk_col

    def __need_to_advance_record_batch(self):
        return not self._curr_row_offset < self._curr_batch.num_rows

    def __advance_record_batch(self):
        """
        Advance to next record batch
        :raise StopIteration: If there are no more record batches
        """
        try:
            self._curr_batch = self._feather_reader.get_batch(self._curr_batch_index)
            self._curr_batch_index += 1
            self._curr_row_offset = 0
            self._pk_col = self._curr_batch[self.key]
            # Filter the batch to only include fields in the schema
            # Pyarrow select will throw a ValueError if the field is not in the schema
            fields = [
                field
                for field in self.schema.keys()
                if field in self._curr_batch.schema.names
            ]
            self._curr_batch = self._curr_batch.select(fields)
        except ValueError:
            raise StopIteration(f"Ended iteration at batch {self._curr_batch_index}")
