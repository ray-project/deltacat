import posixpath
from typing import Iterable, Dict, Any, List, Optional, Generator

import pyarrow
import pyarrow as pa
from deltacat.dev.data_access_layer.storage.writer import (
    WriteOptions,
    WriteMode,
    Writer,
)
from deltacat.utils.filesystem import resolve_path_and_filesystem
from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
)
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.table import Table as IcebergTable


class IcebergWriteOptions(WriteOptions):
    def __init__(self, write_mode: WriteMode = "append", **kwargs):
        super().__init__(write_mode, **kwargs)


class IcebergWriteResultMetadata(Dict[str, Any]):
    """
    Class representing metadata collected during Iceberg data file write.

    Basically just a wrapper for DataFile
    """

    @property
    def append_data_files(self) -> Iterable[DataFile]:
        return self["append_data_files"]

    @append_data_files.setter
    def append_data_files(self, data_files: Iterable[DataFile]):
        self["append_data_files"] = data_files


class IcebergWriter(Writer[IcebergWriteResultMetadata, IcebergWriteOptions]):
    """
    Prototype implementation of Writer interface for iceberg

    This prototype does NOT support many features, like bucketing. Consider it a placeholder to test integrations
    with the writer interface. We will revisit this class and build out more features in the future

    """

    def __init__(
        self,
        catalog: PyIcebergCatalog,
        table_identifier: str,
        *args,
        fs: pyarrow.fs.FileSystem = None,
        partition_by: Optional[List[str]] = None,
        **kwargs,
    ):
        """
        Initialize an IcebergWriter.

        TODO Currently, this takes a PyIcebergCatalog and iceberg table identifier.
        Future iterations will flesh out the story for how iceberg-backed. DeltaCAT streams can use this directly from a deltaCAT table.

        Args:
            catalog: PyIceberg catalog instance
            table_identifier: Full table identifier (e.g., "database.table")
            write_options: Write configuration options
            partition_by: List of columns to partition by
        """
        self.catalog = catalog
        self.table_identifier = table_identifier
        self.partition_by = partition_by or []

        # Load the table
        self.table: IcebergTable = catalog.load_table(table_identifier)
        self.table_schema = self.table.metadata.schema()
        self.spec_id = self.table.metadata.default_spec_id

        # Store partition spec for creating records
        self.partition_spec = self.table.metadata.spec()

        # Set write location, based on either write options or table's path

        table_location, filesystem = resolve_path_and_filesystem(self.table.location())
        self.filesystem = fs if fs is not None else filesystem

        self.data_location = posixpath.join(table_location, "data")
        # Create data directory (this succeeds if dir already exists)
        self.filesystem.create_dir(self.data_location)

    def _write_batches_through_pyiceberg(
        self,
        batches: Iterable[pa.RecordBatch],
        partition_values: Optional[Dict[str, Any]],
    ) -> Iterable[DataFile]:
        """
        Write a single batch to a Parquet file using PyIceberg's mechanisms.

        TODO support using pyarrow filesystem in this method. Currently it instantiates the default PyArrowFileIO.
        To property use the user-configured filesystem, we need to build a shim from pyarrow filesystem to
        the iceberg FileIO interface (and required interfaces like OutputFile, InputFile, etc). We may consider
        working with py-iceberg to get this shim in their repository.

        TODO understand if we can implement this without calling an internal pyiceberg method, which is brittle
        The alternatives evaluated are:
        1. Implement this directly by writing data files through arrow, then introspecting parquet metadata
        to populate data files. This implementation risks getting out of sync with canonical pyiceberg
        logic

        2. Use Daft's `recordbatch_io.write_iceberg`. This method essentially does what we need - writes iceberg data files without actually committing metadata. It operations on a Daft MicroPartition, and it's not clear whether we can safely instantiate a MicroPartition external to Daft.

        Args:
            batch: PyArrow RecordBatch to write
            partition_values: Dictionary of partition values

        Returns:
            Iterable of iceberg DataFiles, representing data files which were just written
        """
        # Create a table from the batch
        table = pa.Table.from_batches(batches)

        datafiles = _dataframe_to_data_files(
            self.table.metadata,
            table,
            # TODO use user configured pyarrow filesystem
            PyArrowFileIO(),
        )

        return datafiles

    def write_batches(
        self,
        record_batches: Iterable[pa.RecordBatch],
        write_options: IcebergWriteOptions,
    ) -> Generator[IcebergWriteResultMetadata, None, None]:
        """
        Write data files from record batches

        Returns:
            Generator of WriteResultMetadata for each batch
        """
        # TODO support other write options
        if write_options.write_mode != WriteMode.APPEND:
            raise NotImplementedError(
                f"Received write mode {write_options.write_mode}. "
                f"Iceberg writer currently only supports write mode APPEND"
            )

        # TODO support partitioning
        data_files = self._write_batches_through_pyiceberg(record_batches, None)

        result_metadata = IcebergWriteResultMetadata()
        result_metadata.append_data_files = data_files
        yield result_metadata

    def commit(
        self,
        write_metadata: Generator[IcebergWriteResultMetadata, None, None],
        *args,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Finalize transaction across all batches and workers

        This will commit the transaction to the Iceberg table
        """

        # Start a transaction
        with self.table.transaction() as tx:

            # Ensure name mapping is set
            # TODO do we need this line?
            if self.table.metadata.name_mapping() is None:
                tx.set_properties(
                    **{
                        "schema.name-mapping.default": self.table.metadata.schema().name_mapping.model_dump_json()
                    }
                )

            # Create an append snapshot update
            with tx.update_snapshot().fast_append() as update_snapshot:
                for wm in write_metadata:
                    for data_file in wm.append_data_files:
                        update_snapshot.append_data_file(data_file)

        # TODO what relevant metadata should this return?
        return {}
