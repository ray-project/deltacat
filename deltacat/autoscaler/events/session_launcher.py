from typing import Optional


class SessionLauncher:
    def __init__(self,
                 stats_metadata_cluster_config_path: str,
                 compaction_cluster_config_path: str):
        self.stats_metadata_cluster_config_path = stats_metadata_cluster_config_path
        self.compaction_cluster_config_path = compaction_cluster_config_path

    def stats_metadata(self):
        raise NotImplementedError("Stats Metadata Collection is not implemented.")

    def compact(self, session_id: Optional[str] = None):
        raise NotImplementedError("Compaction is not implemented.")

