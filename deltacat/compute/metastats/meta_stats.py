# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import ray
import os
import functools
import logging

import pathlib

from typing import Dict, Set, List, Optional, Tuple

from deltacat.compute.stats.models.delta_stats import DeltaStats
from ray.types import ObjectRef

from deltacat import logs
from deltacat.constants import BYTES_PER_GIBIBYTE
from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.models.stats_result import StatsResult
from deltacat.compute.metastats.utils.io import read_cached_partition_stats
from deltacat.compute.stats.utils.io import get_deltas_from_range
from deltacat.compute.stats.utils.intervals import merge_intervals, DeltaRange

from deltacat.compute.metastats.utils.ray_utils import ray_up, ray_init, get_head_node_ip, replace_cluster_cfg_vars, ray_down, clean_up_cluster_cfg_file
from deltacat.compute.metastats.utils.constants import MANIFEST_FILE_COUNT_PER_CPU, R5_MEMORY_PER_CPU, HEAD_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO, \
    WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO, STATS_CLUSTER_R5_INSTANCE_TYPE, DEFAULT_JOB_RUN_TRACE_ID, DEFAULT_CPUS_PER_INSTANCE_R5_8XLARGE
from deltacat.compute.metastats.model.stats_cluster_size_estimator import StatsClusterSizeEstimator
from deltacat.compute.metastats.utils.pyarrow_memory_estimation_function import estimation_function

from deltacat.storage import PartitionLocator, DeltaLocator, Delta
from deltacat.storage import interface as unimplemented_deltacat_storage

from deltacat.constants import PYARROW_INFLATION_MULTIPLIER_ALL_COLUMNS
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.compute.metastats.stats import start_stats_collection

from deltacat.utils.performance import timed_invocation

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def collect_metastats(source_partition_locators: List[PartitionLocator],
                      columns: Optional[List[str]] = None,
                      file_count_per_cpu: Optional[int] = MANIFEST_FILE_COUNT_PER_CPU,
                      stat_results_s3_bucket: Optional[str] = None,
                      metastats_results_s3_bucket: Optional[str] = None,
                      deltacat_storage=unimplemented_deltacat_storage,
                      *args,
                      **kwargs) -> Dict[str, Dict[int, DeltaStats]]:

    # TODO: Add CompactionEventDispatcher for metastats collection started event
    stats_res_all_partitions: Dict[str, Dict[int, DeltaStats]] = {}
    stats_res_obj_ref_all_partitions: Dict[str, ObjectRef] = {}
    for partition_locator in source_partition_locators:
        partition_id = partition_locator.partition_id
        if partition_locator.partition_values:
            partition_value_string = "_".join(partition_locator.partition_values)
        else:
            partition_value_string = f"no_partition_value_{partition_id}"
        partition_canonical_string = partition_locator.canonical_string()
        stats_res_obj_ref = collect_from_partition.remote(
            source_partition_locator=partition_locator,
            partition_value_string=partition_value_string,
            partition_canonical_string=partition_canonical_string,
            columns=columns,
            stat_results_s3_bucket=stat_results_s3_bucket,
            metastats_results_s3_bucket=metastats_results_s3_bucket,
            file_count_per_cpu=file_count_per_cpu,
            deltacat_storage=deltacat_storage,
            *args,
            **kwargs
        )
        stats_res_obj_ref_all_partitions[partition_value_string] = stats_res_obj_ref
    for pv, stats_res_obj_ref in stats_res_obj_ref_all_partitions.items():
        stats_res_all_partitions[pv] = ray.get(stats_res_obj_ref)
    # TODO: Add CompactionEventDispatcher for metastats collection completed event

    logger.info(f"stats_res_all_partitions: {stats_res_all_partitions}")

    # For compaction result validation purpose only
    aggregate_partition_stats_for_validation: Dict[str, list] = {}
    for partition_val, delta_stream_range_set in stats_res_all_partitions.items():
        partition_stats_sum_row_count = 0
        partition_pyarrow_sum = 0
        for stream_pos, stats_column_result in delta_stream_range_set.items():
            for cs in stats_column_result.column_stats[0].manifest_stats.stats:
                partition_stats_sum_row_count += cs.get("rowCount")

            for stats in stats_column_result.get("column_stats"):
                partition_pyarrow_sum += stats.get("stats").get("pyarrowTableBytes")
            aggregate_partition_stats_for_validation[partition_val] = [partition_stats_sum_row_count, partition_pyarrow_sum]
        logger.info(f"partitions_stats_result for partition value: {partition_val}: rowCount: {partition_stats_sum_row_count}; pyarrowTableBytes: {partition_pyarrow_sum}")
    return aggregate_partition_stats_for_validation

    # return stats_res_all_partitions

@ray.remote(num_cpus=1)
def collect_from_partition(source_partition_locator: PartitionLocator,
                           partition_value_string,
                           partition_canonical_string,
                           delta_stream_position_range_set: Optional[Set[DeltaRange]] = None,
                           columns: Optional[List[str]] = None,
                           file_count_per_cpu: Optional[int] = MANIFEST_FILE_COUNT_PER_CPU,
                           stat_results_s3_bucket: Optional[str] = None,
                           metastats_results_s3_bucket: Optional[str] = None,
                           deltacat_storage=unimplemented_deltacat_storage,
                           *args,
                           **kwargs) -> ObjectRef[Dict[int, DeltaStats]]:

    if not columns:
        columns = deltacat_storage.get_table_version_column_names(source_partition_locator.namespace,
                                                                  source_partition_locator.table_name,
                                                                  source_partition_locator.table_version)
    deltas = _find_deltas(source_partition_locator,
                 delta_stream_position_range_set,
                 deltacat_storage)

    logger.info(f"Find {len(deltas)} deltas!")
    trace_id = DEFAULT_JOB_RUN_TRACE_ID
    if "trace_id" in kwargs:
        trace_id = kwargs.get("trace_id")
    else:
        logger.warning(f"No job run trace id specified, default to {DEFAULT_JOB_RUN_TRACE_ID}")

    cpus_per_instance = DEFAULT_CPUS_PER_INSTANCE_R5_8XLARGE
    if cpus_per_instance in kwargs:
        cpus_per_instance = kwargs.get("cpus_per_instance")
    else:
        logger.info(f"Stats cluster CPUS per instance not specified, default to {DEFAULT_CPUS_PER_INSTANCE_R5_8XLARGE}")

    stats_res_obj_ref = _start_all_stats_collection_from_deltas(
                                deltas,
                                partition_value_string,
                                partition_canonical_string,
                                columns,
                                trace_id,
                                file_count_per_cpu,
                                cpus_per_instance,
                                stat_results_s3_bucket,
                                metastats_results_s3_bucket,
                                deltacat_storage)
    return stats_res_obj_ref


def _find_deltas(source_partition_locator: PartitionLocator,
                 delta_stream_position_range_set: Optional[Set[DeltaRange]] = None,
                 deltacat_storage=unimplemented_deltacat_storage) -> List[Delta]:

    if delta_stream_position_range_set is None:
        delta_stream_position_range_set = {(None, None)}
    delta_range_lookup_pending: List[ObjectRef[List[Delta]]] = []

    for range_pair in merge_intervals(delta_stream_position_range_set):
        begin, end = range_pair
        promise: ObjectRef[List[Delta]] = get_deltas_from_range.remote(source_partition_locator, begin, end,
                                                                       deltacat_storage)
        delta_range_lookup_pending.append(promise)

    delta_list_by_ranges: List[List[Delta]] = ray.get(delta_range_lookup_pending)
    deltas = [delta for delta_list in delta_list_by_ranges for delta in delta_list]
    return deltas


def _start_all_stats_collection_from_deltas(
        deltas: List[Delta],
        partition_value_string: Optional[str],
        partition_canonical_string: Optional[str],
        columns: Optional[List[str]] = None,
        trace_id: Optional[str] = None,
        file_count_per_cpu: Optional[int] = MANIFEST_FILE_COUNT_PER_CPU,
        cpus_per_instance: Optional[int] = DEFAULT_CPUS_PER_INSTANCE_R5_8XLARGE,
        stat_results_s3_bucket: Optional[str] = None,
        metastats_results_s3_bucket: Optional[str] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> Dict[int, DeltaStats]:

    delta_cache_lookup_pending: List[List[ObjectRef[DeltaStatsCacheResult], Delta]] = []
    delta_stats_compute_list: List[DeltaLocator] = []
    meta_stats_list_ready: List[DeltaLocator] = []
    meta_stats_list_to_compute: List[DeltaLocator] = []

    if stat_results_s3_bucket:
        found_columns_stats_map: Dict[int, List[DeltaStatsCacheResult]] = \
            read_cached_partition_stats(partition_canonical_string, stat_results_s3_bucket)

    delta_cache_res: List[DeltaStats] = []
    for delta in deltas:
        if found_columns_stats_map and delta.stream_position in found_columns_stats_map:
            cached_result = found_columns_stats_map[delta.stream_position]
            if cached_result.hits:
                delta_cache_res.append(cached_result.hits)
                meta_stats_list_ready.append(cached_result.hits.column_stats[0].manifest_stats.delta_locator)

            if cached_result.misses:
                missed_column_names: List[str] = cached_result.misses.column_names
                delta_locator: DeltaLocator = cached_result.misses.delta_locator
                delta_stats_compute_list.append(delta_locator)
                meta_stats_list_to_compute.append(delta_locator)
        else:
            delta_stats_compute_list.append(delta.locator)
            meta_stats_list_to_compute.append(delta.locator)

    logger.info(f"Collecting stats on {len(delta_stats_compute_list)} deltas!")
    delta_stats_compute_res: Dict[int, DeltaStats] = {}
    if delta_stats_compute_list:
        delta_stats_compute_res = _start_metadata_stats_collection(delta_stats_compute_list=delta_stats_compute_list,
                                            meta_stats_list_ready=meta_stats_list_ready,
                                            meta_stats_list_to_compute=meta_stats_list_to_compute,
                                            partition_value_string=partition_value_string,
                                            partition_canonical_string=partition_canonical_string,
                                            columns=columns,
                                            trace_id=trace_id,
                                            file_count_per_cpu=file_count_per_cpu,
                                            cpus_per_instance=cpus_per_instance,
                                            stat_results_s3_bucket=stat_results_s3_bucket,
                                            metastats_results_s3_bucket=metastats_results_s3_bucket,
                                            deltacat_storage=deltacat_storage)

    delta_stream_range_stats: Dict[int, DeltaStats] = {}
    for delta_column_stats in delta_cache_res:
        assert len(delta_column_stats.column_stats) > 0, \
            f"Expected columns of `{delta_column_stats}` to be non-empty"
        stream_position = delta_column_stats.column_stats[0].manifest_stats.delta_locator.stream_position
        delta_stream_range_stats[stream_position] = delta_column_stats

    # stats collection result: if we have cached stats and missed column stats for same delta, stats collection for this delta is still needed
    # and the final result will use the newly collected stats for this delta.
    stats_collection_res: Dict[int, DeltaStats] = {**delta_stream_range_stats, **delta_stats_compute_res}

    return stats_collection_res


def _start_metadata_stats_collection(
        delta_stats_compute_list: List[DeltaLocator],
        meta_stats_list_ready: List[DeltaLocator],
        meta_stats_list_to_compute: List[DeltaLocator],
        partition_value_string: Optional[str],
        partition_canonical_string: Optional[str],
        columns: Optional[List[str]] = None,
        trace_id: Optional[str] = None,
        file_count_per_cpu: Optional[int] = MANIFEST_FILE_COUNT_PER_CPU,
        cpus_per_instance: Optional[int] = DEFAULT_CPUS_PER_INSTANCE_R5_8XLARGE,
        stat_results_s3_bucket: Optional[str] = None,
        metastats_results_s3_bucket: Optional[str] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> Dict[int, DeltaStats]:

    meta_stats_res_ready: Dict[int, int] = {}

    for delta_locator in meta_stats_list_ready:
        delta_meta_count = 0
        manifest = deltacat_storage.get_delta_manifest(delta_locator)
        delta = Delta.of(delta_locator, None, None, None, manifest)

        for entry in delta.manifest.entries:
            delta_meta_count += entry.meta.content_length
        meta_stats_res_ready[delta.stream_position] = delta_meta_count

    first_delta_locator = meta_stats_list_ready[0] if meta_stats_list_ready else meta_stats_list_to_compute[0]
    manifest = deltacat_storage.get_delta_manifest(first_delta_locator)
    content_type = manifest.meta.content_type
    content_encoding = manifest.meta.content_type

    meta_stats_to_compute: Dict[int, int] = {}
    manifest_file_count_to_compute: Dict[int, int] = {}

    for delta_locator in meta_stats_list_to_compute:
        delta_meta_count = 0
        manifest = deltacat_storage.get_delta_manifest(delta_locator)
        delta = Delta.of(delta_locator, None, None, None, manifest)
        file_count = len(delta.manifest.entries)
        manifest_file_count_to_compute[delta.stream_position] = file_count
        for entry in delta.manifest.entries:
            delta_meta_count += entry.meta.content_length
        meta_stats_to_compute[delta.stream_position] = delta_meta_count

    min_cpus = _estimate_cpus_needed(meta_stats_to_compute, R5_MEMORY_PER_CPU, file_count_per_cpu, manifest_file_count_to_compute,
                                     partition_value_string)
    min_workers = int(min_cpus // cpus_per_instance) + 1

    batched_delta_stats_compute_list = _batch_deltas(delta_stats_compute_list,
                                                     file_count_per_cpu,
                                                     cpus_per_instance,
                                                     deltacat_storage,
                                                     content_type,
                                                     content_encoding)

    # out_cluster_cfg = _setup_stats_cluster(min_workers,
    #                                        partition_value_string,
    #                                        trace_id,
    #                                        cpus_per_instance)
    out_cluster_cfg = None
    delta_stats_res: Dict[int, DeltaStats] = _start_stats_cluster(out_cluster_cfg,
                                                                  batched_delta_stats_compute_list,
                                                                  columns,
                                                                  stat_results_s3_bucket,
                                                                  metastats_results_s3_bucket,
                                                                  deltacat_storage,
                                                                  partition_canonical_string)

    return delta_stats_res


def _start_stats_cluster(out_cluster_cfg: str,
                        batched_delta_stats_compute_list: List[DeltaAnnotated],
                        columns: List[str],
                        stat_results_s3_bucket: Optional[str] = None,
                        metastats_results_s3_bucket: Optional[str] = None,
                        deltacat_storage=unimplemented_deltacat_storage,
                        partition_val:Optional[str]="partition_val"):
    # ray_up_latency = timed_invocation(
    #     func=ray_up,
    #     cluster_cfg=out_cluster_cfg
    # )
    # logger.info(f"ray_up_latency: {partition_val}:{ray_up_latency}")

    # head_node_ip = get_head_node_ip(out_cluster_cfg)
    # client = ray_init(head_node_ip, 10001)
    # with client:
    delta_stream_range_stats, stats_collection_latency = timed_invocation(
        func=start_stats_collection,
        batched_delta_stats_compute_list=batched_delta_stats_compute_list,
        columns=columns,
        stat_results_s3_bucket=stat_results_s3_bucket,
        metastats_results_s3_bucket=metastats_results_s3_bucket,
        deltacat_storage=deltacat_storage
    )
    logger.info(f"actual_stats_collection_latency: {partition_val}: {stats_collection_latency}")
    # client.disconnect()
    # ray_down(out_cluster_cfg)
    # clean_up_cluster_cfg_file(out_cluster_cfg)
    return delta_stream_range_stats


def _estimate_cpus_needed(meta_stats_to_compute, memory_gb_per_cpu, file_count_per_cpu, manifest_file_count_to_compute, partition_val):
    content_length_sum = 0
    for val in meta_stats_to_compute.values():
        content_length_sum += val
    manifest_file_count_sum = 0
    for val in manifest_file_count_to_compute.values():
        manifest_file_count_sum += val
    estimated_memory_bytes_needed = content_length_sum * PYARROW_INFLATION_MULTIPLIER_ALL_COLUMNS
    estimated_memory_gib_needed = estimated_memory_bytes_needed / BYTES_PER_GIBIBYTE

    logger.info(f"estimated_memory_gib_needed: {partition_val} : {estimated_memory_gib_needed}")
    logger.info(f"manifest_file_count_sum: {partition_val} : {manifest_file_count_sum}")

    memory_per_cpu_available = memory_gb_per_cpu * (1 - WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO)
    estimator = StatsClusterSizeEstimator.of(memory_per_cpu_available, file_count_per_cpu,
                                            estimated_memory_gib_needed, manifest_file_count_sum)
    min_cpus = StatsClusterSizeEstimator.estimate_cpus_needed(estimator)
    return min_cpus


def _batch_deltas(delta_stats_compute_list,
                  file_count_per_cpu,
                  cpu_per_instance,
                   deltacat_storage,
                   content_type,
                   content_encoding) -> List[DeltaAnnotated]:
    worker_node_mem = cpu_per_instance * (1 - WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO) * BYTES_PER_GIBIBYTE
    delta_list = []

    estimate_based_on_content_length = functools.partial(
        estimation_function, content_type=content_type, content_encoding=content_encoding,
    )

    for delta_locator in delta_stats_compute_list:
        manifest = deltacat_storage.get_delta_manifest(delta_locator)
        delta = Delta.of(delta_locator, None, None, None, manifest)
        delta_annotated = DeltaAnnotated.of(delta)
        delta_list.append(delta_annotated)

    rebatched_da_list = DeltaAnnotated.rebatch(
        delta_list,
        worker_node_mem,
        file_count_per_cpu,
        estimate_based_on_content_length,
    )

    logger.info(f"Rebatched_delta_list_length: {len(rebatched_da_list)}")

    return rebatched_da_list


def _setup_stats_cluster(min_workers, partition_value_string, trace_id, cpus_per_instance):
    stats_cluster_instance_type = int(cpus_per_instance // 4) if cpus_per_instance else STATS_CLUSTER_R5_INSTANCE_TYPE
    stats_cluster_instance_type_str = f"r5.{stats_cluster_instance_type}xlarge".strip()
    parent_dir_path = pathlib.Path(__file__).parent.resolve()
    in_cfg = os.path.join(parent_dir_path, "config", "stats_cluster_example.yaml")
    out_cluster_cfg_file_path = replace_cluster_cfg_vars(
        partition_canonical_string=partition_value_string,
        trace_id=trace_id,
        file_path=in_cfg,
        min_workers=min_workers,
        head_type=stats_cluster_instance_type_str,
        worker_type=stats_cluster_instance_type_str,
        head_object_store_memory_pct=HEAD_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO*100,
        worker_object_store_memory_pct=WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO*100)

    return out_cluster_cfg_file_path
