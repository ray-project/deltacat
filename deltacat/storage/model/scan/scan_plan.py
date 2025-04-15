from dataclasses import dataclass

from deltacat.storage.model.scan.scan_task import ScanTask


@dataclass
class ScanPlan:
    """Represents collection of ScanTasks to be passed to compute engine for query planning"""

    scan_tasks: list[ScanTask]
