from typing import Optional

from pyiceberg.catalog import Catalog
from deltacat.storage.model.scan.push_down import Pushdown
from deltacat.storage.model.scan.scan_plan import ScanPlan
from deltacat.storage.model.scan.scan_task import FileScanTask, DataFile
from deltacat.storage.util.scan_planner import ScanPlanner
from deltacat.storage.iceberg.impl import _try_load_iceberg_table


class IcebergScanPlanner(ScanPlanner):
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def create_scan_plan(
        self,
        table_name: str,
        namespace: Optional[str] = None,
        pushdown: Optional[Pushdown] = None,
    ) -> ScanPlan:
        iceberg_table = _try_load_iceberg_table(
            self.catalog, namespace=namespace, table_name=table_name
        )
        file_scan_tasks = []
        # TODO: implement predicate pushdown to Iceberg
        for scan_task in iceberg_table.scan().plan_files():
            file_scan_tasks.append(FileScanTask([DataFile(scan_task.file.file_path)]))
        return ScanPlan(file_scan_tasks)
