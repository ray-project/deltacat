import typing
from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    Protocol,
    Generator,
    Any,
    TypeVar,
    Type,
    Generic,
    List,
    Iterator,
    Optional,
)

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.metastore.sst import SSTableRow

FILE_FORMAT = TypeVar("FILE_FORMAT")
MEMORY_FORMAT = TypeVar("MEMORY_FORMAT")

T = TypeVar("T")


@dataclass
class RowAndPrimaryKey(Generic[FILE_FORMAT]):
    """
    Named tuple for a record batch with an index into a specific row
    Note that record batches store data by column, so the row index should be
    used to index into each column array
    """

    row: FILE_FORMAT
    primary_key: Any


class FileReader(
    Protocol[FILE_FORMAT],
    Iterator[RowAndPrimaryKey[FILE_FORMAT]],
    typing.ContextManager,
):
    """
    Interface for reading specific file

    TODO (IO abstraction) we will need to think about how various IO interfaces (S3, filesystem, memory)
      plug into this.
    """

    @abstractmethod
    def __init__(
        self, sst_row: SSTableRow, file_store: FileStore, primary_key: str
    ) -> None:
        """
        Required constructor (see: FileReaderRegistrar)

        :param sst_row: SSTableRow containing file metadata
        :param file_store: Object providing file access
        """
        ...

    @abstractmethod
    def peek(self) -> Optional[RowAndPrimaryKey[FILE_FORMAT]]:
        """
        Peek at the next RowAndPrimaryKey without advancing the iterator
        :return: Optional of RowAndPrimaryKey
        """
        ...

    @abstractmethod
    def __next__(self) -> RowAndPrimaryKey[FILE_FORMAT]:
        """
        Fetch the next RowAndPrimaryKey and advance iterator
        """
        ...

    @abstractmethod
    def close(self):
        """
        Explicit add close so that resources can be cleaned up outside the ContextManager.

        We expect that callers opening the reader can EITHER use a with statement or call __enter__()
        Callers closing the reader can EITHER explicitly call close() or have with statement manage calling __exit__
        """
        ...


class DataReader(Protocol[FILE_FORMAT]):
    """
    Interface for reading specific file formats
    A DatasetReader uses a different DataReader for each format

    TODO (IO abstraction) we will need to think about how various IO interfaces (S3, filesystem, memory)
      plug into this.
    """

    @abstractmethod
    def deserialize_records(
        self, records: FILE_FORMAT, output_type: Type[MEMORY_FORMAT]
    ) -> Generator[MEMORY_FORMAT, None, None]:
        """
        Deserialize records into the specified format.

        Note that output_type gets set based on what a DataScan converts results to,
            e.g. to_arrow, to_dict

        :param records: Input data (generated by generate_records method)
        :param output_type: Type to deserialize into
        :returns: A generator yielding records of the specified type.
        """
        ...

    @abstractmethod
    def join_deserialize_records(
        self,
        records: List[FILE_FORMAT],
        output_type: Type[MEMORY_FORMAT],
        join_key: str,
    ) -> Generator[MEMORY_FORMAT, None, None]:
        """
        Deserialize records into the specified format.

        Note that output_type gets set based on what a DataScan converts results to,
            e.g. to_arrow, to_dict

        :param records: Multiple records which should be merged into final output record
            Note this is a list instead of a set to not enforce hashability
        :param join_key name of field to join across record. This field must be present on all records
        :param output_type: Type to deserialize into
        :returns: A generator yielding records of the specified type.
        """
        ...
