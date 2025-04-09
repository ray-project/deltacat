from dataclasses import dataclass

from deltacat.storage.model.scan_task import ScanTask

@dataclass
class ScanPlan:
    scan_tasks: list[ScanTask]