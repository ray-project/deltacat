from typing import Set, NamedTuple, List, Optional, Dict

from deltacat import ContentType, SortKey
from deltacat.autoscaler.events.compaction.collections.partition_key_value import PartitionKeyValues
from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.storage import PartitionLocator, interface as unimplemented_deltacat_storage

import pyarrow as pa


class CompactionInput(NamedTuple):
    source_partition_locator: PartitionLocator
    compacted_partition_locator: PartitionLocator
    primary_keys: Set[str]
    compaction_artifact_s3_bucket: str
    last_stream_position_to_compact: int
    hash_bucket_count: Optional[int] = None
    sort_keys: List[SortKey] = None
    records_per_primary_key_index_file: int = 38_000_000
    records_per_compacted_file: int = 4_000_000
    input_deltas_stats: Optional[Dict[int, DeltaStats]] = None
    min_hash_bucket_chunk_size: int = 0
    compacted_file_content_type: ContentType = ContentType.PARQUET
    delete_prev_primary_key_index: bool = False
    schema_on_read: Optional[pa.schema] = None
    deltacat_storage = unimplemented_deltacat_storage
    partition_key_values: PartitionKeyValues = None
