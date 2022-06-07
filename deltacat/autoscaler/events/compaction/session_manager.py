from typing import List

from deltacat.autoscaler.events.compaction.process import CompactionProcess
from deltacat.autoscaler.events.session_manager import SessionManager
from deltacat.storage import PartitionLocator


class CompactionSessionManager(SessionManager):
    def launch_stats_metadata_collection(self, source_partition_locators: List[PartitionLocator]):
        raise NotImplementedError("Stats Metadata Collection is not implemented.")

    def launch_compaction(self, compaction_processes: List[CompactionProcess]):
        raise NotImplementedError("Compaction is not implemented.")
