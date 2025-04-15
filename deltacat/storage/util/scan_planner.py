from abc import ABC, abstractmethod
from typing import Optional

from deltacat.storage.model.scan.push_down import Pushdown
from deltacat.storage.model.scan.scan_plan import ScanPlan


class ScanPlanner(ABC):
    @abstractmethod
    def create_scan_plan(
        self,
        table_name: str,
        namespace: Optional[str] = None,
        pushdown: Optional[Pushdown] = None,
    ) -> ScanPlan:
        """Return a ScanPlan for a given DeltaCAT Table after applying pushdown predicates

        Args:
            table: Name of the table
            namespace: Optional namespace of the table. Uses default namespace if not specified.
            pushdown: Pushdown predicates used to filter partitions/data files

        Returns:
            a ScanPlan object containing list of ScanTasks
        """
        pass
