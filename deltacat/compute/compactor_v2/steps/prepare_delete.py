import importlib
import logging
from typing import Any
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
import numpy as np
import pyarrow as pa
import ray
from deltacat import logs
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
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
from typing import Optional
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


def prepare_delete(
    annotated_delta: DeltaAnnotated,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
    **kwargs) -> Any:
    """
    go through every file
        build table

    put into object store

    go through annotated delta
    go through compacted table
    """
    logger.info(f"prepare delete: {kwargs=}")
    logger.info(f"pdebug: prepare_delete: {dict(locals())}")
    tables = deltacat_storage.download_delta(
        annotated_delta,
        max_parallelism=1,
        file_reader_kwargs_provider=read_kwargs_provider,
        storage_type=StorageType.LOCAL,
        **deltacat_storage_kwargs,
    )
    dmfe = deltacat_storage.download_delta_manifest_entry(
            annotated_delta,
            entry_index=0,
            file_reader_kwargs_provider=read_kwargs_provider,
            **deltacat_storage_kwargs,
    )
    logger.info(f"pdebug:{tables=}, {dmfe=}")

