from typing import Iterator

from daft import Schema, DataType
from daft.daft import StorageConfig, PartitionField, Pushdowns, ScanTask, PartitionTransform, FileFormatConfig, \
    ParquetSourceConfig
from daft.io.scan import ScanOperator, make_partition_field
from daft.logical.schema import Field

from deltacat.catalog.model.table_definition import TableDefinition


class DeltaCATScanOperator(ScanOperator):
    def __init__(self, table: TableDefinition, storage_config: StorageConfig) -> None:
        super().__init__()
        self.table = table
        self._schema = self._infer_schema()
        self.partition_keys = self._infer_partition_keys()
        self.storage_config = storage_config

    def schema(self) -> Schema:
        return self._schema

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
                    file_format= file_format_config,
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
        # TODO: remove hard coding
        fields: list[Field] = [
            Field.create(name="symbol", dtype=DataType.string()),
            Field.create(name="bid", dtype=DataType.float64()),
            Field.create(name="ask", dtype=DataType.float64()),
        ]
        return Schema._from_fields(fields=fields)

    def _infer_partition_keys(self) -> list[PartitionField]:
        # TODO: remove hard coding
        partition_field = Field.create(name="symbol_bucket", dtype=DataType.int32())
        source_field = Field.create(name="symbol", dtype=DataType.string())
        return [make_partition_field(field=partition_field, source_field=source_field, transform=PartitionTransform.iceberg_bucket(2))]