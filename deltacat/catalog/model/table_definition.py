# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Any

from deltacat.storage import Stream, Table, TableVersion
from deltacat.storage.model.scan.push_down import Pushdown
from deltacat.storage.model.scan.scan_plan import ScanPlan
from deltacat.storage.util.scan_planner import ScanPlanner


class TableDefinition(dict):
    @staticmethod
    def of(
        table: Table,
        table_version: TableVersion,
        stream: Stream,
        native_object: Optional[Any] = None,
        scan_planner: Optional[ScanPlanner] = None,
    ) -> TableDefinition:
        return TableDefinition(
            {
                "table": table,
                "tableVersion": table_version,
                "stream": stream,
                "nativeObject": native_object,
                "scan_planner": scan_planner,
            }
        )

    @property
    def table(self) -> Table:
        return self["table"]

    @property
    def table_version(self) -> TableVersion:
        return self["tableVersion"]

    @property
    def stream(self) -> Stream:
        return self["stream"]

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")

    @property
    def scan_planner(self) -> Optional[ScanPlanner]:
        return self.get("scan_planner")

    def create_scan_plan(self, pushdown: Optional[Pushdown] = None) -> ScanPlan:
        if not self.scan_planner:
            raise RuntimeError(
                f"ScanPlanner is not initialized for table '{self.table.table_name}' "
                f"of namespace '{self.table.namespace}'"
            )
        return self.scan_planner.create_scan_plan(
            table_name=self.table.table_name,
            namespace=self.table.namespace,
            pushdown=pushdown,
        )
