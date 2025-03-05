import os
import uuid
from typing import Iterable, Dict, Any, List, Optional, Generator, TypeVar, Generic
import pyarrow as pa
import pyarrow.parquet as pq
from deltacat.catalog.v2.model.writer import WriteResultMetadata, Writer, WriteOptions
from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pyiceberg.io.pyarrow import (
    _check_pyarrow_schema_compatible,
    data_file_statistics_from_parquet_metadata,
    compute_statistics_plan,
    parquet_path_to_id_mapping,
)
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
)
from pyiceberg.typedef import Record


class IcebergWriteResultMetadata(WriteResultMetadata):
    """
    Class representing metadata collected during Iceberg data file write
    """
    pass


class IcebergWriter(Writer[IcebergWriteResultMetadata]):
    """
    Iceberg implementation of Writer interface
    """

    def __init__(
            self,
            catalog: PyIcebergCatalog,
            table_identifier: str,
            write_options: WriteOptions,
            partition_by: Optional[List[str]] = None
    ):
        """
        Initialize an IcebergWriter

        Args:
            catalog: PyIceberg catalog instance
            table_identifier: Full table identifier (e.g., "database.table")
            write_options: Write configuration options
            partition_by: List of columns to partition by
        """
        self.catalog = catalog
        self.table_identifier = table_identifier
        self.write_options = write_options
        self.partition_by = partition_by or []

        # Load the table
        self.table = catalog.load_table(table_identifier)
        self.table_schema = self.table.metadata.schema()
        self.spec_id = self.table.metadata.default_spec_id

        # Store partition spec for creating records
        self.partition_spec = self.table.metadata.spec(self.spec_id) if self.spec_id is not None else None

        # Generate transaction ID if not provided
        if not self.write_options.transaction_id:
            self.write_options.transaction_id = str(uuid.uuid4())

    def _get_partition_values(self, batch: pa.RecordBatch) -> Dict[str, Any]:
        """
        Extract partition values from the batch based on the partition spec

        For simplicity, this assumes all rows in a batch have the same partition values
        """
        if not self.partition_by or not batch.num_rows:
            return {}

        partition_values = {}
        for field in self.partition_by:
            if field in batch.schema.names:
                # Get the first value for the partition field
                # This assumes all rows in a batch have the same partition values
                column_idx = batch.schema.get_field_index(field)
                value = batch.column(column_idx)[0].as_py()
                partition_values[field] = value

        return partition_values

    def _write_batch_to_file(self, batch: pa.RecordBatch, partition_values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Write a single batch to a Parquet file and return file metadata
        """
        # Convert batch to table
        table = pa.Table.from_batches([batch])

        # Create directory structure for partitions if needed
        partition_path = ""
        if partition_values:
            partition_parts = []
            for field, value in partition_values.items():
                partition_parts.append(f"{field}={value}")
            partition_path = "/".join(partition_parts)

        # Create data path
        data_path = self.write_options.table_location
        if not data_path.endswith("/"):
            data_path += "/"

        if partition_path:
            data_path += partition_path + "/"

        # Ensure directory exists
        os.makedirs(data_path, exist_ok=True)

        # Generate unique file name
        file_name = f"data-{uuid.uuid4()}.parquet"
        file_path = data_path + file_name

        # Write the table to a Parquet file
        pq.write_table(table, file_path)

        # Get file size
        file_size = os.path.getsize(file_path)

        return {
            "file_path": file_path,
            "file_size": file_size,
            "num_rows": batch.num_rows,
            "partition_values": partition_values
        }

    def _create_data_file(self, file_metadata: Dict[str, Any]) -> DataFile:
        """
        Create a DataFile object from file metadata
        """
        # Get the file path
        file_path = file_metadata["file_path"]

        # Read the Parquet metadata to get statistics
        parquet_metadata = pq.read_metadata(file_path)

        # Create partition record
        partition_values = file_metadata.get("partition_values", {})
        partition_record = Record(**partition_values) if partition_values else None

        # Check schema compatibility
        _check_pyarrow_schema_compatible(
            self.table_schema,
            parquet_metadata.schema.to_arrow_schema()
        )

        # Get statistics
        statistics = data_file_statistics_from_parquet_metadata(
            parquet_metadata=parquet_metadata,
            stats_columns=compute_statistics_plan(self.table_schema, self.table.metadata.properties),
            parquet_column_mapping=parquet_path_to_id_mapping(self.table_schema),
        )

        # Create DataFile object
        data_file = DataFile(
            content=DataFileContent.DATA,
            file_path=file_path,
            file_format=FileFormat.PARQUET,
            partition=partition_record,
            file_size_in_bytes=file_metadata["file_size"],
            record_count=file_metadata["num_rows"],
            sort_order_id=None,
            spec_id=self.spec_id,
            **statistics.to_serialized_dict(),
        )

        return data_file

    def write_batches(
            self,
            record_batches: Iterable[pa.RecordBatch]
    ) -> Generator[IcebergWriteResultMetadata, None, None]:
        """
        Write data files from record batches

        Returns:
            Generator of WriteResultMetadata for each batch
        """
        for batch in record_batches:
            if batch.num_rows == 0:
                continue

            # Get partition values
            partition_values = self._get_partition_values(batch)

            # Write batch to file
            file_metadata = self._write_batch_to_file(batch, partition_values)

            # Create and yield result metadata
            result = IcebergWriteResultMetadata([file_metadata])
            yield result

    def finalize_local(self, write_metadata: Generator[IcebergWriteResultMetadata, None, None]) -> List[Dict[str, Any]]:
        """
        Finalize the segment-level commit by collecting all metadata

        Returns:
            Aggregated file metadata
        """
        # Collect all file metadata
        all_file_metadata = []
        for metadata_batch in write_metadata:
            for file_metadata in metadata_batch:
                all_file_metadata.append(file_metadata)

        return all_file_metadata

    def finalize_global(
            self,
            write_metadata: Generator[IcebergWriteResultMetadata, None, None],
            *args,
            **kwargs
    ) -> Dict[str, Any]:
        """
        Finalize transaction across all batches and workers

        This will commit the transaction to the Iceberg table
        """
        # Collect all file metadata
        all_file_metadata = []
        for metadata_batch in write_metadata:
            all_file_metadata.extend(metadata_batch)

        # Start a transaction
        with self.table.transaction() as tx:
            # Ensure name mapping is set
            if self.table.metadata.name_mapping() is None:
                tx.set_properties(
                    **{
                        "schema.name-mapping.default": self.table.metadata.schema().name_mapping.model_dump_json()
                    }
                )

            # Create a snapshot update
            with tx.update_snapshot().fast_append() as update_snapshot:
                for file_info in all_file_metadata:
                    # Create DataFile object
                    data_file = self._create_data_file(file_info)

                    # Append to snapshot
                    update_snapshot.append_data_file(data_file)

        # Return information about the commit
        return {
            "transaction_id": self.write_options.transaction_id,
            "table_identifier": self.table_identifier,
            "num_files_committed": len(all_file_metadata),
            "total_rows": sum(file["num_rows"] for file in all_file_metadata)
        }