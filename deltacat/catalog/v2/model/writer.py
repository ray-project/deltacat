from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any, List, Optional, Generator, TypeVar, Generic
import pyarrow as pa


class WriteOptions:
    """
    Global options for writing.

    In a distributed setting, WriteOptions should be initialized before writing data files and
    passed to each worker

    # TODO make Dict and follow pattern like in Metafile.py
    """
    def __init__(
            self,
            table_location: str,
            format: str = "parquet",
            mode: str = "append",
            transaction_id: Optional[str] = None,
            custom_params: Optional[Dict[str, Any]] = None
    ):
        self.table_location = table_location
        self.format = format
        self.mode = mode
        self.transaction_id = transaction_id
        self.custom_params = custom_params or {}


class WriteResultMetadata(List[Dict[str, Any]]):
    """
    Class representing metadata collected during data file write, E.g.: [{"file_path": "...", "num_rows": ...}, ...]

    Each table format may subclass WriteResultMetadata
    """
    pass

# Write result subtype specific to a given data format
WRITE_RESULT = TypeVar('WRITE_RESULT', bound=WriteResultMetadata)

class Writer(Generic[WRITE_RESULT], ABC):
    """
    Each Writer instance is responsible for writing and finalizing
    a subset of data and metadata.

    If there's a global commit step, the partial metadata from each writer
    is posted to a shared place for a final aggregator to use.
    """
    @abstractmethod
    def write_batches(
            self,
            record_batches: Iterable[pa.RecordBatch]
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
    def finalize_global(self,
                        write_metadata: Iterable[WRITE_RESULT],
                        *args,
                        **kwargs) -> Any:
        """
        Finalize transaction across all batches and all local workers. Expected to be invoked from head node

        This MUST commit any open transactions. For table formats supporting ACID transactions, the writer is expected
        to call finalize_global after writing.

        This MAY perform other synchronous clean up steps which block commit. For instance, it may compact small files
        written by workers into larger files before final commit.
        """


