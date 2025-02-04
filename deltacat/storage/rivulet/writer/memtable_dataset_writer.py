from __future__ import annotations

import threading
from threading import Thread
from typing import Any, List, Set, Protocol, TypeVar, Dict, Iterable

from pyarrow import RecordBatch, Table
from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.rivulet.metastore.delta import ManifestIO, DeltacatManifestIO

from deltacat.storage.rivulet import Schema
from deltacat.storage.rivulet.metastore.json_sst import JsonSstWriter
from deltacat.storage.rivulet.serializer import MEMTABLE_DATA, DataSerializer
from deltacat.storage.rivulet.serializer_factory import DataSerializerFactory
from deltacat.storage.rivulet.writer.dataset_writer import DatasetWriter, DATA
from deltacat.storage.rivulet.metastore.sst import SSTWriter
from deltacat.storage.rivulet.fs.file_provider import FileProvider

INPUT_ROW = TypeVar("INPUT_ROW")


class Memtable(Protocol[INPUT_ROW]):
    """
    Protocol defining the interface for a memtable that can store and sort records of type T.
    """

    def add_record(self, record: INPUT_ROW) -> bool:
        """
        Add a record to the memtable.

        Args:
            record: The record to add of type INPUT_ROW

        Returns:
            bool: True if the memtable is full after adding the record, False otherwise
        """
        ...

    def get_sorted_records(self, schema: Schema) -> MEMTABLE_DATA:
        """
        Get all records in the memtable in sorted order.

        Returns:
            List[T]: A list of sorted records
        """
        ...


class DictMemTable(Memtable[Dict[str, Any]]):
    """
    Unit of in memory buffering of sorted records before records are written to file

    TODO future improvements:
        1. build b+ tree of record indexes on insertion
             OR If we end up using arrow as intermediate format, we can use
          pyarrow compute sort
        2. Probably we will re-write in rust
    """

    def __init__(self, merge_key: str):
        self.row_size = 0
        self.merge_key = merge_key

        self._records: List[Dict[str, Any]] = []
        self.lock = threading.Lock()

    def add_record(self, record: Dict[str, Any]):
        with self.lock:
            self._records.append(record)
            self.row_size += 1

        if self.row_size >= MemtableDatasetWriter.MAX_ROW_SIZE:
            return True
        return False

    def get_sorted_records(self, schema: Schema) -> List[Dict[str, Any]]:
        """
        Gets sorted records

        :return: iterator over sorted record
        """
        with self.lock:
            self._records.sort(key=lambda x: x.__getitem__(self.merge_key))
            return self._records


class RecordBatchMemTable(Memtable[RecordBatch]):
    """
    Note that this will not respect max row size.
    """

    def __init__(self, merge_key: str):
        self.row_size = 0
        self.merge_key = merge_key

        # list of full record batches in memtable
        self._records_batches: List[RecordBatch] = []
        self.lock = threading.Lock()

    def add_record(self, record: RecordBatch):
        with self.lock:
            self._records_batches.append(record)
            self.row_size += record.num_rows

        if self.row_size >= MemtableDatasetWriter.MAX_ROW_SIZE:
            return True
        return False

    def get_sorted_records(self, schema: Schema) -> Table:
        """
        Gets sorted records

        :return: iterator over sorted record
        """
        with self.lock:
            # Note that we are providing schema so that pyarrow does not infer it
            table = Table.from_batches(self._records_batches, schema.to_pyarrow())
            return table.sort_by(self.merge_key)


class MemtableDatasetWriter(DatasetWriter):
    # Note that this max row size is not respected when PyArrow RecordBatches are used
    # In that case, the entire record batch is written within one memtable even if the row count overflows
    MAX_ROW_SIZE = 1000000
    """
    Buffers data into rotating memtables. When a memtable reaches a certain size, it is flushed to disk and a new memtable is allocated

    Uses DataWriter which will be format specific for writing data
    Uses MetadataWriter for writing metadata

    TODO Future Improvements
    1. Maybe we should re-write this class in Rust (pending testing)
    """

    def __init__(
        self,
        file_provider: FileProvider,
        schema: Schema,
        locator: PartitionLocator,
        file_format: str | None = None,
        sst_writer: SSTWriter = None,
        manifest_io: ManifestIO = None,
    ):

        if not sst_writer:
            sst_writer = JsonSstWriter()
        if not manifest_io:
            manifest_io = DeltacatManifestIO(file_provider.uri, locator)

        self.schema = schema

        self.file_provider = file_provider
        self.data_serializer: DataSerializer = DataSerializerFactory.get_serializer(
            self.schema, self.file_provider, file_format
        )
        self.sst_writer = sst_writer
        self.manifest_io = manifest_io

        self._sst_files: Set[str] = set()
        self.__curr_memtable = None
        self.__open_memtables = []
        self.__rlock = threading.RLock()
        self.__open_threads: List[Thread] = []
        self._locator = locator

    def write_dict(self, record: Dict[str, Any]) -> None:

        # Construct memtable if doesn't exist. If previous memtable wrong type, rotate
        memtable_ctor = lambda: DictMemTable(self.schema.get_merge_key())
        if not self.__curr_memtable:
            self.__curr_memtable = memtable_ctor()
        try:
            isinstance(self.__curr_memtable, DictMemTable)
        except TypeError:
            self.__rotate_memtable(memtable_ctor)

        # Write record(s). If memtable is full, rotate
        if self.__curr_memtable.add_record(record):
            self.__rotate_memtable(memtable_ctor)

    def write_record_batch(self, record: RecordBatch) -> None:
        # Construct memtable if doesn't exist. If previous memtable wrong type, rotate
        memtable_ctor = lambda: RecordBatchMemTable(self.schema.get_merge_key())
        if not self.__curr_memtable:
            self.__curr_memtable = memtable_ctor()

        try:
            isinstance(self.__curr_memtable, RecordBatchMemTable)
        except TypeError:
            self.__rotate_memtable(memtable_ctor)

        # Write record(s). If memtable is full, rotate
        if self.__curr_memtable.add_record(record):
            self.__rotate_memtable(memtable_ctor)

    def write(self, data: DATA) -> None:
        if isinstance(data, RecordBatch):
            self.write_record_batch(data)
        elif isinstance(data, Iterable):
            for x in data:
                if isinstance(x, dict):
                    self.write_dict(x)
                elif isinstance(x, RecordBatch):
                    self.write_record_batch(x)
                else:
                    raise ValueError(
                        f"Iterable contained unsupported type {type(x).__name__}."
                        f" Supported data types to write are: {DATA}"
                    )
        else:
            raise ValueError(
                f"Unsupported data type {type(data).__name__}. Supported data types to write are: {DATA}"
            )

    def flush(self) -> str:
        """
        Explicitly flush any data and metadata and commit to dataset
        """
        self.__flush_memtable(self.__curr_memtable)
        for thread in [t for t in self.__open_threads if t.is_alive()]:
            thread.join()

        manifest_location = self.__write_manifest_file()
        self._sst_files.clear()

        return manifest_location

    def __enter__(self) -> Any:
        """
        Enter and exit method allows python "with" statement
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Closes all open memtables and ensures all data is flushed.
        """
        self.flush()
        # return False to propogate up error messages
        return False

    def __rotate_memtable(self, memtable_constructor_closure):
        """
        Replace the active memtable
        :return:
        """
        with self.__rlock:
            self.__flush_memtable(self.__curr_memtable)
            self.__curr_memtable = memtable_constructor_closure()
            self.__open_memtables.append(self.__curr_memtable)

            # Reap dead threads
            self.__open_threads = [t for t in self.__open_threads if t.is_alive()]

    def __flush_memtable(self, memtable):
        thread = threading.Thread(target=self.__flush_memtable_async, args=(memtable,))
        thread.start()
        with self.__rlock:
            self.__open_threads.append(thread)

    def __flush_memtable_async(self, memtable: Memtable):
        """
        Flushes data and metadata for a given memtable
        Called asynchronously in background thread
        """
        if not memtable:
            return

        sst_metadata_list = self.data_serializer.flush_batch(
            memtable.get_sorted_records(self.schema)
        )

        # short circuit if no data/metadata written
        if not sst_metadata_list:
            with self.__rlock:
                self.__open_memtables.remove(memtable)
            return

        # Write SST. Each memtable is going to have a dedicated L0 SST file because that is the unit at which
        # we have contiguously sorted data
        sst_file = self.file_provider.provide_l0_sst_file()

        with self.__rlock:
            self.sst_writer.write(sst_file, sst_metadata_list)
            self._sst_files.add(sst_file.location)

            if memtable in self.__open_memtables:
                self.__open_memtables.remove(memtable)

    def __write_manifest_file(self) -> str:
        """
        Write the manifest file to the filesystem at the given URI.
        """
        return self.manifest_io.write(list(self._sst_files), self.schema, 0)
