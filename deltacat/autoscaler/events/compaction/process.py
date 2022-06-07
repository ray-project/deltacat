from typing import Optional

from deltacat.autoscaler.events.compaction.collections.partition_key_value import PartitionKeyValues
from deltacat.storage import PartitionLocator


class CompactionProcess:
    def __init__(self,
                 partition_locator: PartitionLocator,
                 compaction_cluster_config_path: str,
                 hash_bucket_count: Optional[int] = None,
                 last_stream_position_to_compact: Optional[int] = None,
                 partition_key_values: PartitionKeyValues = None,
                 cluster_memory_bytes: Optional[int] = None,
                 input_delta_total_bytes: Optional[int] = None):
        self.partition_locator = partition_locator
        self.compaction_cluster_config_path = compaction_cluster_config_path
        self.hash_bucket_count = hash_bucket_count
        self.last_stream_position_to_compact = last_stream_position_to_compact
        self.partition_values = partition_key_values
        self.cluster_memory_bytes = cluster_memory_bytes
        self.input_delta_total_bytes = input_delta_total_bytes
        self.id = ".".join([pkv.value for pkv in partition_key_values])


