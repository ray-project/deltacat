from deltacat.autoscaler.events.session_manager import SessionManager


class CompactionSessionManager(SessionManager):
    def launch_stats_metadata_collection(self):
        raise NotImplementedError("Stats Metadata Collection is not implemented.")

    def launch_compaction(self):
        raise NotImplementedError("Compaction is not implemented.")
