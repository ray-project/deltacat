import logging
from typing import Optional, Any, Set

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
import deltacat.logs as logs

from deltacat.storage.model.scan.push_down import Pushdown, PartitionFilter
from deltacat.storage.model.scan.scan_plan import ScanPlan
from deltacat.storage.model.scan.scan_task import FileScanTask, DataFile
from deltacat.storage.util.scan_planner import ScanPlanner
from deltacat.experimental.storage.iceberg.impl import _try_load_iceberg_table
from deltacat.experimental.storage.iceberg.visitor import IcebergExpressionVisitor

# Initialize DeltaCAT logger
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class IcebergScanPlanner(ScanPlanner):
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self.expression_visitor = IcebergExpressionVisitor()

    @classmethod
    def _collect_filter_fields(cls, expr: Any) -> Set[str]:
        """
        Collects all field names referenced in the filter expression.

        Args:
            expr: The expression to analyze

        Returns:
            Set of field names referenced in the expression
        """
        fields = set()
        if hasattr(expr, "field"):
            fields.add(expr.field)
        if hasattr(expr, "left"):
            fields.update(cls._collect_filter_fields(expr.left))
        if hasattr(expr, "right"):
            fields.update(cls._collect_filter_fields(expr.right))
        if hasattr(expr, "expr"):
            fields.update(cls._collect_filter_fields(expr.expr))
        if hasattr(expr, "values"):
            for value in expr.values:
                fields.update(cls._collect_filter_fields(value))
        return fields

    def create_scan_plan(
        self,
        table_name: str,
        namespace: Optional[str] = None,
        pushdown: Optional[Pushdown] = None,
    ) -> ScanPlan:
        iceberg_table = _try_load_iceberg_table(
            self.catalog, namespace=namespace, table_name=table_name
        )

        # TODO: implement row, column predicate pushdown to Iceberg

        # Get the partition spec
        partition_spec = iceberg_table.spec()

        # Check if the table is partitioned
        is_partitioned = len(partition_spec.fields) > 0

        scan = iceberg_table.scan()
        if is_partitioned:
            if pushdown and pushdown.partition_filter:
                filter_fields = self._collect_filter_fields(pushdown.partition_filter)
                logger.info(
                    f"Pushdown partition filter is enabled, converting to Iceberg. Fields discovered in filter: {', '.join(sorted(filter_fields))}"
                )
                # Handle partition filter if present, DeltaCAT only supports partition-level filters right now
                iceberg_expression = self._convert_partition_filter(
                    iceberg_table, pushdown.partition_filter
                )
                scan = scan.filter(iceberg_expression)

        file_scan_tasks = []
        for scan_task in scan.plan_files():
            file_scan_tasks.append(FileScanTask([DataFile(scan_task.file.file_path)]))
        return ScanPlan(file_scan_tasks)

    @classmethod
    def _validate_partition_references(
        cls, expr: Any, partition_cols: Set[str]
    ) -> None:
        """
        Validates that the expression only references partition columns.

        Args:
            expr: The expression to validate
            partition_cols: Set of valid partition column names

        Raises:
            ValueError: If the expression references a non-partition column
        """
        if hasattr(expr, "field"):  # Reference type expression
            if expr.field not in partition_cols:
                raise ValueError(
                    f"Filter references non-partition column: {expr.field}. "
                    f"Partition columns are: {partition_cols}"
                )
        # Recursively validate nested expressions
        if hasattr(expr, "left"):
            cls._validate_partition_references(expr.left, partition_cols)
        if hasattr(expr, "right"):
            cls._validate_partition_references(expr.right, partition_cols)
        if hasattr(expr, "expr"):
            cls._validate_partition_references(expr.expr, partition_cols)
        if hasattr(expr, "values"):
            for value in expr.values:
                cls._validate_partition_references(value, partition_cols)

    def _convert_partition_filter(
        self, table: Table, partition_filter: PartitionFilter
    ):
        """
        Convert DeltaCAT partition filter to PyIceberg expression,
        validating that only partition columns are referenced.
        """
        partition_cols = set(field.name for field in table.spec().fields)

        # Validate before converting
        self._validate_partition_references(partition_filter, partition_cols)

        # Convert to PyIceberg expression
        return self.expression_visitor.visit(partition_filter)
