import importlib
import logging
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
import numpy as np
import pyarrow as pa
import ray
from deltacat import logs
from deltacat.storage import (
    Delta,
    DeltaLocator,
    DeltaType,
    DistributedDataset,
    LifecycleState,
    ListResult,
    LocalDataset,
    LocalTable,
    Manifest,
    ManifestAuthor,
    Namespace,
    Partition,
    SchemaConsistencyType,
    Stream,
    StreamLocator,
    Table,
    TableVersion,
    SortKey,
    PartitionLocator,
)
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
import pyarrow.compute as pc
from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
from deltacat.compute.compactor_v2.model.prepare_delete_input import PrepareDeleteInput
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    group_hash_bucket_indices,
    group_by_pk_hash_bucket,
)
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import StorageType
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics
from deltacat.utils.resources import (
    get_current_process_peak_memory_usage_in_bytes,
    ProcessUtilizationOverTimeRange,
)
from deltacat.constants import BYTES_PER_GIBIBYTE

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

"""
annotated_delta: DeltaAnnotated,
primary_keys: List[str],
num_hash_buckets: int,
num_hash_groups: int,
enable_profiler: Optional[bool] = False,
metrics_config: Optional[MetricsConfig] = None,
read_kwargs_provider: Optional[ReadKwargsProvider] = None,
object_store: Optional[IObjectStore] = None,
deltacat_storage=unimplemented_deltacat_storage,
deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
"""


def drop_earlier_duplicates(table: pa.Table, on: str, sort_col_name: str) -> pa.Table:
    """
    It is important to not combine the chunks for performance reasons.
    """
    if not (on in table.column_names or sort_col_name in table.column_names):
        return table

    selector = table.group_by([on]).aggregate([(sort_col_name, "max")])

    table = table.filter(
        pc.is_in(
            table[sort_col_name],
            value_set=selector[f"{sort_col_name}_max"],
        )
    )
    return table
