from typing import Iterable, Dict, Any, Optional, Generator

import pyarrow as pa
from deltacat.dev.data_access_layer.storage.writer import (
    Writer,
    WriteOptions,
    WriteMode,
)
from deltacat.storage.rivulet.dataset import Dataset as RivuletDataset


class RivuletWriteOptions(WriteOptions):
    """
    Rivulet-specific write options
    """

    def __init__(
        self,
        write_mode: WriteMode = "upsert",
        file_format: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(write_mode=write_mode, **kwargs)
        self.file_format = file_format

    @property
    def file_format(self) -> Optional[str]:
        return self.get("file_format")

    @file_format.setter
    def file_format(self, file_format: Optional[str]):
        if file_format:
            self["file_format"] = file_format


class RivuletWriteResultMetadata(Dict[str, Any]):
    """
    Class representing metadata collected during Rivulet data file write.

    Rivulet does not support collecting metadata and independently committing as a transaction.
    It only supports buffering state in a dataset_writer before calling .commit

    Therefore, this is empty

    TODO how will consumer RivuletWriter understand that commits must happen locally? Or, how will we share
    state of rivulet dataset_writer across nodes on Ray?
    """


class RivuletWriter(Writer[RivuletWriteResultMetadata, RivuletWriteOptions]):
    """
    Rivulet implementation of Writer interface for the data access layer

    TODO currently, this writer is coupled directly to the Rivulet Dataset class and not to a DeltaCAT stream.
    Either in this interface or the data access storage interface, we will allow users to instantiate a rivulet writer
    from a deltacat table.
    """

    def __init__(
        self,
        dataset: RivuletDataset,
        *args,
        file_format: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize a RivuletWriter

        Args:
            dataset: rivulet dataset
            file_format: format to write. See Rivulet dataset.py. Options are [parquet, feather].
            filesystem: Optional filesystem to use
        """
        self.dataset = dataset
        self.dataset_writer = self.dataset.writer(file_format)

    def write_batches(
        self,
        record_batches: Iterable[pa.RecordBatch],
        write_options: RivuletWriteOptions,
    ) -> Generator[RivuletWriteResultMetadata, None, None]:
        """
        Write data files from record batches

        Returns:
            Generator of RivuletWriteResultMetadata for each batch
        """

        # Only supporting upsert mode
        if write_options.write_mode != WriteMode.UPSERT:
            raise NotImplementedError(
                f"Received write mode {write_options.write_mode}. "
                f"Rivulet writer currently only supports write mode UPSERT"
            )

        # Write batches to dataset writer without flushing yet
        for batch in record_batches:
            self.dataset_writer.write(batch)

        yield RivuletWriteResultMetadata()

    def commit(
        self, write_metadata: Iterable[RivuletWriteResultMetadata], *args, **kwargs
    ) -> Dict[str, Any]:
        """
        Finalize and commit transaction across all batches

        For Rivulet, this writes the manifest file for all collected SST files.
        """
        # Flushing dataset writer effectively commits data by writing manifest
        self.dataset_writer.flush()
        return {}
