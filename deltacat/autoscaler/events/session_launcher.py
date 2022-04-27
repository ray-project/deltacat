import uuid


class SessionLauncher:
    def __init__(self,
                 stats_metadata_cluster_config_path: str,
                 compaction_cluster_config_path: str):
        self.stats_metadata_cluster_config_path = stats_metadata_cluster_config_path
        self.compaction_cluster_config_path = compaction_cluster_config_path
        self._session_id = str(uuid.uuid4())

    @property
    def session_id(self):
        return self._session_id

    def stats_metadata(self):
        raise NotImplementedError("Stats Metadata Collection is not implemented.")

    def compact(self):
        raise NotImplementedError("Compaction is not implemented.")

