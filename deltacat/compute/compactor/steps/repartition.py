import importlib
import logging
from contextlib import nullcontext
import pyarrow.compute as pc
import pyarrow as pa
from typing import List, Optional
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
    repartition_type: str,
    repartition_args: dict,
    destination_partition: Partition,
    enable_profiler: bool,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
) -> RePartitionResult:

    if repartition_type == "range":
        # A delta that is partitioned by range is partitioned in such a way
        # that each partition contains rows for which the partitioning expression value lies within a given range for a specified column.
        # For example, if the partitioning column is "date", the partition ranges are "2020-01-01" to "2020-01-10" and "2020-01-11" to "2020-01-20".
        # The partitioned delta will have two partitions, one for each range.
        column: str = repartition_args["column"]
        partition_ranges: List = repartition_args["ranges"]
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
            partitioned_tables = {i: [] for i in range(len(partition_ranges) + 1)}
            total_record_count = 0
            col_name_int64 = f"{column}_int64"
            for table in tables:
                total_record_count += len(table)
                table_new = table.add_column(
                    0,
                    pa.field(col_name_int64, pa.int64()),
                    pc.cast(table[column], pa.int64()),
                )
                for filter_value in partition_ranges:
                    partitioned_tables[partition_ranges.index(filter_value)].append(
                        table_new.filter(
                            (pc.field(col_name_int64) <= pc.scalar(filter_value))
                        )
                    )
                    if filter_value == partition_ranges[-1]:
                        partitioned_tables[partition_ranges.index(filter_value)].append(
                            table_new.filter(
                                (pc.field(col_name_int64) > pc.scalar(filter_value))
                            )
                        )
            # TODO(rootliu) set optimal or max number of records per file to defer the performance degradation due to too many small files
            # iterate the items in partitioned_tables, concat all tables for the same key
            # and then write the result to the storage
            range_table_length = 0
            range_deltas: List[DeltaAnnotated] = []
            for _, value in partitioned_tables.items():
                if len(value) > 0:
                    range_table = pa.concat_tables(value)
                    if len(range_table) > 0:
                        range_table_length += len(range_table)
                        range_delta = deltacat_storage.stage_delta(
                            range_table,
                            destination_partition,
                            content_type=ContentType.PARQUET,
                        )
                        range_deltas.append(range_delta)

            assert (
                range == total_record_count
            ), "Repartitioned table should have the same number of records as the original table"
            return RePartitionResult(
                range_deltas=range_deltas,
            )
    else:
        raise NotImplementedError(
            f"Repartition type {repartition_type} is not supported."
        )


@ray.remote
def repartition(
    annotated_delta: DeltaAnnotated,
    destination_partition: Partition,
    repartition_type: str,
    repartition_args: dict,
    enable_profiler: bool,
    metrics_config: MetricsConfig,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
) -> RePartitionResult:
    logger.info(f"Starting repartition task...")
    repartition_result, duration = timed_invocation(
        func=_timed_repartition,
        annotated_delta=annotated_delta,
        repartition_type=repartition_type,
        repartition_args=repartition_args,
        destination_partition=destination_partition,
        enable_profiler=enable_profiler,
        read_kwargs_provider=read_kwargs_provider,
        deltacat_storage=deltacat_storage,
    )
    if metrics_config:
        emit_timer_metrics(
            metrics_name="repartition", value=duration, metrics_config=metrics_config
        )
    return repartition_result
