# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import ray
import os
import functools
import logging

import pathlib

from typing import Dict, Set, List, Optional

from deltacat.compute.stats.models.delta_stats import DeltaStats
from ray.types import ObjectRef

from deltacat import logs
from deltacat.constants import BYTES_PER_GIBIBYTE
from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.utils.io import read_cached_delta_stats, get_deltas_from_range
from deltacat.compute.stats.utils.intervals import merge_intervals, DeltaRange

from deltacat.compute.metastats.utils.ray_utils import ray_up, ray_init, get_head_node_ip, replace_cluster_cfg_vars
from deltacat.compute.metastats.utils.constants import MANIFEST_FILE_COUNT_PER_CPU, R5_MEMORY_PER_CPU, HEAD_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO, \
    WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO, INSTANCE_TYPE_TO_MEMORY_MULTIPLIER, STATS_CLUSTER_R5_INSTANCE_TYPE, DEFAULT_JOB_RUN_TRACE_ID
from deltacat.compute.metastats.model.stats_cluster_size_estimator import StatsClusterSizeEstimator
from deltacat.compute.metastats.utils.pyarrow_memory_estimation_function import estimation_function

from deltacat.storage import PartitionLocator, DeltaLocator, Delta
from deltacat.storage import interface as unimplemented_deltacat_storage

from deltacat.constants import PYARROW_INFLATION_MULTIPLIER_ALL_COLUMNS
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.compute.metastats.stats import start_stats_collection

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
        partition_canonical_string = partition_locator.canonical_string()
        stats_res_obj_ref = collect_from_partition.remote(
            source_partition_locator=partition_locator,
            partition_canonical_string=partition_canonical_string,
            columns=columns,
            stat_results_s3_bucket=stat_results_s3_bucket,
            metastats_results_s3_bucket=metastats_results_s3_bucket,
            file_count_per_cpu=file_count_per_cpu,
            deltacat_storage=deltacat_storage,
            *args,
            **kwargs
        )
        stats_res_obj_ref_all_partitions[partition_locator.partition_id] = stats_res_obj_ref
    for partition_id, stats_res_obj_ref in stats_res_obj_ref_all_partitions.items():
        stats_res_all_partitions[partition_id] = ray.get(stats_res_obj_ref)
    # TODO: Add CompactionEventDispatcher for metastats collection completed event
    return stats_res_all_partitions

@ray.remote
def collect_from_partition(source_partition_locator: PartitionLocator,
                           partition_canonical_string: Optional[str],
                            delta_stream_position_range_set: Optional[Set[DeltaRange]] = None,
                            columns: Optional[List[str]] = None,
                            file_count_per_cpu: Optional[int] = MANIFEST_FILE_COUNT_PER_CPU,
                            stat_results_s3_bucket: Optional[str] = None,
                            metastats_results_s3_bucket: Optional[str] = None,
                            deltacat_storage=unimplemented_deltacat_storage,
                            *args,
                            **kwargs) -> Dict[int, DeltaStats]:

    if not columns:
        columns = deltacat_storage.get_table_version_column_names(source_partition_locator.namespace,
                                                                  source_partition_locator.table_name,
                                                                  source_partition_locator.table_version)
    deltas = _find_deltas(source_partition_locator,
                 delta_stream_position_range_set,
                 deltacat_storage)

    trace_id = DEFAULT_JOB_RUN_TRACE_ID
    if "trace_id" in kwargs:
        trace_id = kwargs.get("trace_id")
    else:
        logger.warning(f"No job run trace id specified, default to {DEFAULT_JOB_RUN_TRACE_ID}")
    return _start_all_stats_collection_from_deltas(
                                deltas,
                                partition_canonical_string,
                                columns,
                                trace_id,
                                file_count_per_cpu,
                                stat_results_s3_bucket,
                                metastats_results_s3_bucket,
                                deltacat_storage)


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
        partition_canonical_string: Optional[str],
        columns: Optional[List[str]] = None,
        trace_id: Optional[str] = None,
        file_count_per_cpu: Optional[int] = MANIFEST_FILE_COUNT_PER_CPU,
        stat_results_s3_bucket: Optional[str] = None,
        metastats_results_s3_bucket: Optional[str] = None,
        deltacat_storage=unimplemented_deltacat_storage) -> Dict[int, DeltaStats]:

    delta_cache_lookup_pending: List[List[ObjectRef[DeltaStatsCacheResult], Delta]] = []
    delta_stats_compute_list: List[DeltaLocator] = []
    meta_stats_list_ready = []
    meta_stats_list_to_compute = []
    for delta in deltas:
        if stat_results_s3_bucket:
            promise: ObjectRef[DeltaStatsCacheResult] = \
                read_cached_delta_stats.remote(delta, columns, stat_results_s3_bucket)
            delta_cache_lookup_pending.append(promise)
            continue

        delta_stats_compute_list.append(delta.locator)

    delta_cache_res: List[DeltaStats] = []
    while delta_cache_lookup_pending:
        ready, delta_cache_lookup_pending = ray.wait(delta_cache_lookup_pending)

        cached_results: List[List[DeltaStatsCacheResult, Delta]] = ray.get(ready)
        for item in cached_results:
            cached_result = item[0]
            if cached_result.hits:
                delta_cache_res.append(cached_result.hits)
                meta_stats_list_ready.append(cached_result.hits.column_stats[0].manifest_stats.delta_locator)

            if cached_result.misses:
                missed_column_names: List[str] = cached_result.misses.column_names
                delta_locator: DeltaLocator = cached_result.misses.delta_locator
                delta_stats_compute_list.append(delta_locator)
                meta_stats_list_to_compute.append(delta_locator)

    if delta_stats_compute_list:
        return _start_metadata_stats_collection(delta_stats_compute_list=delta_stats_compute_list,
                                            meta_stats_list_ready=meta_stats_list_ready,
                                            meta_stats_list_to_compute=meta_stats_list_to_compute,
                                            partition_canonical_string=partition_canonical_string,
                                            columns=columns,
                                            trace_id=trace_id,
                                            file_count_per_cpu=file_count_per_cpu,
                                            stat_results_s3_bucket=stat_results_s3_bucket,
                                            metastats_results_s3_bucket=metastats_results_s3_bucket,
                                            deltacat_storage=deltacat_storage)
    else:
        logger.info(f"DeltaStats for {partition_canonical_string} already collected.")
        delta_stream_range_stats: Dict[int, DeltaStats] = {}
        for delta_column_stats in delta_cache_res:
            assert len(delta_column_stats.column_stats) > 0, \
                f"Expected columns of `{delta_column_stats}` to be non-empty"
            stream_position = delta_column_stats.column_stats[0].manifest_stats.delta_locator.stream_position
            delta_stream_range_stats[stream_position] = delta_column_stats
        return delta_stream_range_stats


def _start_metadata_stats_collection(
        delta_stats_compute_list: List[DeltaLocator],
        meta_stats_list_ready: List[DeltaLocator],
        meta_stats_list_to_compute: List[DeltaLocator],
        partition_canonical_string: Optional[str],
        columns: Optional[List[str]] = None,
        trace_id: Optional[str] = None,
        file_count_per_cpu: Optional[int] = MANIFEST_FILE_COUNT_PER_CPU,
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

    min_cpus = _estimate_cpus_needed(meta_stats_to_compute, R5_MEMORY_PER_CPU, file_count_per_cpu, manifest_file_count_to_compute)
    min_workers = int(min_cpus // INSTANCE_TYPE_TO_MEMORY_MULTIPLIER) + 1

    batched_delta_stats_compute_list = _batch_deltas(delta_stats_compute_list,
                                                     file_count_per_cpu,
                                                     deltacat_storage,
                                                     content_type,
                                                     content_encoding)

    out_cluster_cfg = _setup_stats_cluster(min_workers,
                                           partition_canonical_string,
                                           trace_id)
    delta_stats_res: Dict[int, DeltaStats] = _start_stats_cluster(out_cluster_cfg,
                                                                  batched_delta_stats_compute_list,
                                                                  columns,
                                                                  stat_results_s3_bucket,
                                                                  metastats_results_s3_bucket,
                                                                  deltacat_storage)

    return delta_stats_res


def _start_stats_cluster(out_cluster_cfg: str,
                        batched_delta_stats_compute_list: List[DeltaAnnotated],
                        columns: List[str],
                        stat_results_s3_bucket: Optional[str] = None,
                        metastats_results_s3_bucket: Optional[str] = None,
                        deltacat_storage=unimplemented_deltacat_storage):
    ray_up(out_cluster_cfg)
    head_node_ip = get_head_node_ip(out_cluster_cfg)
    client = ray_init(head_node_ip, 10001)
    with client:
        delta_stream_range_stats: Dict[int, DeltaStats] = start_stats_collection(
            batched_delta_stats_compute_list,
            columns,
            stat_results_s3_bucket,
            metastats_results_s3_bucket,
            deltacat_storage
        )
    client.disconnect()
    return delta_stream_range_stats


def _estimate_cpus_needed(meta_stats_to_compute, memory_per_cpu, file_count_per_cpu, manifest_file_count_to_compute):
    content_length_sum = 0
    for val in meta_stats_to_compute.values():
        content_length_sum += val
    manifest_file_count_sum = 0
    for val in manifest_file_count_to_compute.values():
        manifest_file_count_sum += val
    estimated_memory_bytes_needed = content_length_sum * PYARROW_INFLATION_MULTIPLIER_ALL_COLUMNS
    estimated_memory_gib_needed = estimated_memory_bytes_needed / BYTES_PER_GIBIBYTE
    memory_per_cpu_available = memory_per_cpu * (1 - WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO)
    estimator = StatsClusterSizeEstimator.of(memory_per_cpu_available, file_count_per_cpu,
                                            estimated_memory_gib_needed, manifest_file_count_sum)
    min_cpus = StatsClusterSizeEstimator.estimate_cpus_needed(estimator)
    return min_cpus


def _batch_deltas(delta_stats_compute_list,
                  file_count_per_cpu,
                   deltacat_storage,
                   content_type,
                   content_encoding) -> List[DeltaAnnotated]:
    worker_node_mem = INSTANCE_TYPE_TO_MEMORY_MULTIPLIER * (1 - WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO) * BYTES_PER_GIBIBYTE
    delta_list = []

    estimate_based_on_content_length = functools.partial(
        estimation_function, content_type=content_type, content_encoding=content_encoding,
    )

    for delta_locator in delta_stats_compute_list:
        manifest = deltacat_storage.get_delta_manifest(delta_locator)
        delta = Delta.of(delta_locator, None, None, None, manifest)
        delta_annotated = DeltaAnnotated.of(delta)
        delta_list.append(delta_annotated)

    rebatched_da_list = DeltaAnnotated.rebatch_based_on_memory_and_file_count_limit(
        delta_list,
        worker_node_mem,
        file_count_per_cpu,
        estimate_based_on_content_length,
    )

    return rebatched_da_list


def _setup_stats_cluster(min_workers, partition_canonical_string, trace_id):
    parent_dir_path = pathlib.Path(__file__).parent.resolve()
    in_cfg = os.path.join(parent_dir_path, "config", "stats_cluster_test.yaml")
    out_cluster_cfg_file_path = replace_cluster_cfg_vars(
        partition_canonical_string=partition_canonical_string,
        trace_id=trace_id,
        file_path=in_cfg,
        min_workers=min_workers,
        head_type=f"r5.{STATS_CLUSTER_R5_INSTANCE_TYPE}xlarge",
        worker_type=f"r5.{STATS_CLUSTER_R5_INSTANCE_TYPE}xlarge",
        head_object_store_memory_pct=HEAD_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO,
        worker_object_store_memory_pct=WORKER_NODE_OBJECT_STORE_MEMORY_RESERVE_RATIO*100)

    return out_cluster_cfg_file_path