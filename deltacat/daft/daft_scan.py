from typing import Iterator

from daft import Schema
from daft.daft import (
    StorageConfig,
    PartitionField,
    Pushdowns,
    ScanTask,
    FileFormatConfig,
    ParquetSourceConfig,
)
from daft.io.scan import ScanOperator

from deltacat.catalog.model.table_definition import TableDefinition
from deltacat.daft.model import DaftPartitionKeyMapper


class DeltaCatScanOperator(ScanOperator):
    def __init__(self, table: TableDefinition, storage_config: StorageConfig) -> None:
        super().__init__()
        self.table = table
        self._schema = self._infer_schema()
        self.partition_keys = self._infer_partition_keys()
        self.storage_config = storage_config

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "DeltaCatScanOperator"

    def display_name(self) -> str:
        return f"DeltaCATScanOperator({self.table.table.namespace}.{self.table.table.table_name})"

    def partitioning_keys(self) -> list[PartitionField]:
        return self.partition_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys}",
            f"Storage config = {self.storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        # TODO: implement pushdown predicate on DeltaCAT
        dc_scan_plan = self.table.create_scan_plan()
        scan_tasks = []
        file_format_config = FileFormatConfig.from_parquet_config(
            # maybe this: ParquetSourceConfig(field_id_mapping=self._field_id_mapping)
            ParquetSourceConfig()
        )
        for dc_scan_task in dc_scan_plan.scan_tasks:
            for data_file in dc_scan_task.data_files():
                st = ScanTask.catalog_scan_task(
                    file=data_file.file_path,
                    file_format=file_format_config,
                    schema=self._schema._schema,
                    storage_config=self.storage_config,
                    pushdowns=pushdowns,
                )
                scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True

    def _infer_schema(self) -> Schema:

        if not (
            self.table and self.table.table_version and self.table.table_version.schema
        ):
            raise RuntimeError(
                f"Failed to infer schema for DeltaCAT Table "
                f"{self.table.table.namespace}.{self.table.table.table_name}"
            )

        return Schema.from_pyarrow_schema(self.table.table_version.schema.arrow)

    def _infer_partition_keys(self) -> list[PartitionField]:
        if not (
            self.table
            and self.table.table_version
            and self.table.table_version.partition_scheme
            and self.table.table_version.schema
        ):
            raise RuntimeError(
                f"Failed to infer partition keys for DeltaCAT Table "
                f"{self.table.table.namespace}.{self.table.table.table_name}"
            )

        schema = self.table.table_version.schema
        partition_keys = self.table.table_version.partition_scheme.keys
        if not partition_keys:
            return []

        partition_fields = []
        for key in partition_keys:
            field = DaftPartitionKeyMapper.unmap(key, schema)
            # Assert that the returned value is not None.
            assert field is not None, f"Unmapping failed for key {key}"
            partition_fields.append(field)

        return partition_fields
