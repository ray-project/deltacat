from abc import ABC, abstractmethod
from enum import Enum
from typing import Iterable, Dict, Any, Optional, Generator, TypeVar, Generic
import pyarrow as pa


class WriteMode(str, Enum):
    """
    WriteMode defines the semantics of how incoming writes are merged with existing data

    The low level sematantics and behavior of write operations may still vary based on different table formats

    Not all table formats support all modes. TODO mechanism to signal through write interface which modes are supported

    APPEND: add new data files. Newly inserted data is not merged or upserted with relation to existing data

    OVERWRITE: add new data files and delete existing data files. Existing data files which are overwritten
        are defined via an overwrite filter. For instance - overwrite existing data files by table partition

    UPSERT: records in new data files are merged with existing data files using table's merge keys

    DELETE: Equality based delete of matching records. The logic to determine which records to delete
       based on incoming data, e.g. by primary key, by partial record match, is specific to each table format
    """
    APPEND = "append"
    UPSERT = "upsert"
    OVERWRITE = "overwrite"
    DELETE = "delete"


class WriteOptions(Dict[str, Any]):
    """
    Global options for writing.

    In a distributed setting, WriteOptions should be initialized before writing data files and
    passed to each worker
    """
    def __init__(self, write_mode: WriteMode = "append", **kwargs):
        super().__init__()
        self.write_mode = write_mode

    @property
    def write_mode(self) -> str:
        return self["write_mode"]

    @write_mode.setter
    def write_mode(self, write_mode):
        self["write_mode"] = write_mode



WRITE_RESULT = TypeVar('WRITE_RESULT', bound=Dict[str, Any])
WRITE_OPTIONS = TypeVar('WRITE_OPTIONS', bound=Dict[str, Any])



class Writer(Generic[WRITE_RESULT, WRITE_OPTIONS], ABC):
    """
    Each Writer instance is responsible for writing and finalizing
    a subset of data and metadata.

    If there's a global commit step, the partial metadata from each writer
    is posted to a shared place for a final aggregator to use.

    TODO writer should return whether it supports finalize_local or not
    TODO writer should return if write_batches will also commit metadata
    """

    @abstractmethod
    def write_batches(
            self,
            record_batches: Iterable[pa.RecordBatch],
            write_options: WRITE_OPTIONS,
    ) -> Generator[WRITE_RESULT, None, None]:
        """
        Writes data files in batches.

        Return necessary metadata describing what was produced. The exact spec of this metadat
        will be specific to the table format being used (e.g.: Iceberg vs. native deltacat).

        The implementation of this is responsible for all table format level considerations like as partitioning,
        bucketing, etc.

        This function SHOULD NOT expose data written to readers before finalize_local/finalize_global are called.
        For table formats which do not support ACID compliance, the expectation is to write data files only in
        write_batches then write metafiles in finalize_*. This means that the output of finalize_batches must be
        preserved to finalize commit, or else there will be orphaned data files.
        """
        pass

    @abstractmethod
    def finalize_local(self, write_metadata: Iterable[WRITE_RESULT]) -> Any:
        """
        Finalize the segment-level commit. This method will collect all WRITE_RESULTs from write_batches
        and publish a partial commit or otherwise collect data to pass to finalize_global if commit must be done
        by global coordinator.

        This MAY terminate transaction, depending on whether a given writer impl will commit one transaction globally
        or one transaction per local worker.
        """
        pass

    @abstractmethod
    def commit(self,
               write_metadata: Iterable[WRITE_RESULT],
               *args,
               **kwargs) -> Any:
        """
        Finalize and commit transaction across all batches and all local workers. Expected to be invoked from head node

        This MUST commit any open transactions. For table formats supporting ACID transactions, the writer is expected
        to call finalize_global after writing.

        This MAY perform other synchronous clean up steps which block commit. For instance, it may compact small files
        written by workers into larger files before final commit.
        """
