import importlib
import logging
from contextlib import nullcontext
import pyarrow.compute as pc
import pyarrow as pa
from typing import List, Optional, Any
from deltacat.types.media import StorageType, ContentType
import ray
from deltacat import logs
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.compute.compactor.model.repartition_result import RePartitionResult
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.storage import Partition
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics, MetricsConfig

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _timed_repartition(
    annotated_delta: DeltaAnnotated,
    column: str,
    filter_value: Any,
    detination_partition: Partition,
    enable_profiler: bool,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
) -> RePartitionResult:

    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"repartition_{worker_id}_{task_id}.bin"
    ) if enable_profiler else nullcontext():
        tables: List[pa.Table] = deltacat_storage.download_delta(
            annotated_delta,
            storage_type=StorageType.LOCAL,
            file_reader_kwargs_provider=read_kwargs_provider,
        )
        cold_tables = []
        hot_tables = []
        total_record_count = 0
        for table in tables:
            total_record_count += len(table)
            cold = table.filter(
                (pc.cast(table[column], pa.int64()) <= pc.scalar(filter_value))
            )
            cold_tables.append(cold)
            hot = table.filter(
                (pc.cast(table[column], pa.int64()) > pc.scalar(filter_value))
            )
            hot_tables.append(hot)
        # TODO(rootliu) set optimal or max number of records per file to defer the performance degradation due to too many small files
        cold_table = pa.concat_tables(cold_tables)
        hot_table = pa.concat_tables(hot_tables)
        cold_delta = deltacat_storage.stage_delta(
            cold_table,
            detination_partition,
            content_type=ContentType.PARQUET,
        )
        hot_delta = deltacat_storage.stage_delta(
            hot_table,
            detination_partition,
            content_type=ContentType.PARQUET,
        )
        assert (
            len(cold_table) + len(hot_table) == total_record_count
        ), "Repartitioned table should have the same number of records as the original table"
        return RePartitionResult(
            cold_delta=cold_delta,
            hot_delta=hot_delta,
        )


@ray.remote
def repartition(
    annotated_delta: DeltaAnnotated,
    column: str,
    filter_value: Any,
    detination_partition: Partition,
    enable_profiler: bool,
    metrics_config: MetricsConfig,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
) -> RePartitionResult:
    logger.info(f"Starting repartition task...")
    repartition_result, duration = timed_invocation(
        func=_timed_repartition,
        annotated_delta=annotated_delta,
        column=column,
        filter_value=filter_value,
        detination_partition=detination_partition,
        enable_profiler=enable_profiler,
        read_kwargs_provider=read_kwargs_provider,
        deltacat_storage=deltacat_storage,
    )
    if metrics_config:
        emit_timer_metrics(
            metrics_name="repartition", value=duration, metrics_config=metrics_config
        )
    return repartition_result
