from typing import Protocol, Iterable, Union, Any, Dict
import pyarrow as pa

DATA = Union[Iterable[Dict[str, Any]], Iterable[pa.RecordBatch], pa.RecordBatch]


class DatasetWriter(Protocol):
    """
    Top level interface for writing records to rivulet dataset. This is used by dataset.py

    This writes both data AND metadata (SSTs, manifests).

    The general paradigm is that records are written iteratively through write or write_batch. At configurable intervals (based on record count or size), data and metadata gets flushed.

    When the user either closes the dataset writer or calls commit(), this triggers all buffered data and metadata to be flushed.
    """

    def write(self, record: DATA) -> None:
        ...

    def flush(self) -> str:
        """
        Explicitly flush any data and metadata and commit to dataset

        This is a blocking operation

        :return: URI of manifest written for commit
        """
        ...
