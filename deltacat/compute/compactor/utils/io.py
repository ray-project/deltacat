import logging
import time
import math
from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.constants import PYARROW_INFLATION_MULTIPLIER, BYTES_PER_MEBIBYTE

from deltacat.storage import PartitionLocator, Delta, \
    interface as unimplemented_deltacat_storage
from deltacat import logs
from deltacat.compute.compactor import DeltaAnnotated

from typing import Dict, List, Optional, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def discover_deltas(
        source_partition_locator: PartitionLocator,
        start_position_exclusive: Optional[int],
        end_position_inclusive: int,
        deltacat_storage=unimplemented_deltacat_storage) -> List[Delta]:

    stream_locator = source_partition_locator.stream_locator
    namespace = stream_locator.namespace
    table_name = stream_locator.table_name
    table_version = stream_locator.table_version
    partition_values = source_partition_locator.partition_values
    deltas_list_result = deltacat_storage.list_deltas(
        namespace,
        table_name,
        partition_values,
        table_version,
        start_position_exclusive,
        end_position_inclusive,
        True,
    )
    deltas = deltas_list_result.all_items()
    if not deltas:
        raise RuntimeError(f"Unexpected Error: Couldn't find any deltas to "
                           f"compact in delta stream position range "
                           f"('{start_position_exclusive}', "
                           f"'{end_position_inclusive}']. Source partition: "
                           f"{source_partition_locator}")
    if start_position_exclusive:
        first_delta = deltas.pop(0)
        logger.info(f"Removed exclusive start delta w/ expected stream "
                    f"position '{start_position_exclusive}' from deltas to "
                    f"compact: {first_delta}")
    logger.info(f"Count of deltas to compact in delta stream "
                f"position range ('{start_position_exclusive}', "
                f"'{end_position_inclusive}']: {len(deltas)}. Source "
                f"partition: '{source_partition_locator}'")
    return deltas


def limit_input_deltas(
        input_deltas: List[Delta],
        cluster_resources: Dict[str, float],
        hash_bucket_count: int,
        min_pk_index_pa_bytes: int,
        user_hash_bucket_chunk_size: int,
        input_deltas_stats: Dict[int, DeltaStats],
        deltacat_storage=unimplemented_deltacat_storage) \
        -> Tuple[List[DeltaAnnotated], int, int]:

    # TODO (pdames): when row counts are available in metadata, use them
    #  instead of bytes - memory consumption depends more on number of
    #  input delta records than bytes.

    # we assume here that we're running on a fixed-size cluster
    # this assumption could be removed, but we'd still need to know the max
    # resources we COULD get for this cluster, and the amount of memory
    # available per CPU should remain fixed across the cluster.
    worker_cpus = int(cluster_resources["CPU"])
    worker_obj_store_mem = float(cluster_resources["object_store_memory"])
    # worker_obj_store_mem = ray_constants.from_memory_units(
    #     cluster_resources["object_store_memory"]
    # )
    if min_pk_index_pa_bytes > 0:
        required_heap_mem_for_dedupe = worker_obj_store_mem - min_pk_index_pa_bytes
        assert required_heap_mem_for_dedupe > 0, \
            f"Not enough required memory available to re-batch input deltas" \
            f"and initiate the dedupe step."
        # Size of batched deltas must also be reduced to have enough space for primary
        # key index files (from earlier compaction rounds) in the dedupe step, since
        # they will be loaded into worker heap memory.
        worker_obj_store_mem = required_heap_mem_for_dedupe

    logger.info(f"Total worker object store memory: {worker_obj_store_mem}")
    worker_obj_store_mem_per_task = worker_obj_store_mem / worker_cpus
    logger.info(f"Worker object store memory/task: "
                f"{worker_obj_store_mem_per_task}")
    worker_task_mem = cluster_resources["memory"]
    logger.info(f"Total worker memory: {worker_task_mem}")
    # TODO (pdames): ensure fixed memory per CPU in heterogenous clusters
    worker_mem_per_task = worker_task_mem / worker_cpus
    logger.info(f"Cluster worker memory/task: {worker_mem_per_task}")

    delta_bytes = 0
    delta_bytes_pyarrow = 0
    delta_manifest_entries = 0
    latest_stream_position = -1
    limited_input_da_list = []

    if input_deltas_stats is None:
        input_deltas_stats = {}

    input_deltas_stats = {int(stream_pos): DeltaStats(delta_stats)
                          for stream_pos, delta_stats in input_deltas_stats.items()}
    for delta in input_deltas:
        manifest = deltacat_storage.get_delta_manifest(delta)
        delta.manifest = manifest
        position = delta.stream_position
        delta_stats = input_deltas_stats.get(delta.stream_position, DeltaStats())
        if delta_stats:
            delta_bytes_pyarrow += delta_stats.stats.pyarrow_table_bytes
        else:
            # TODO (pdames): ensure pyarrow object fits in per-task obj store mem
            logger.warning(
                f"Stats are missing for delta stream position {delta.stream_position}, "
                f"materialized delta may not fit in per-task object store memory.")
        manifest_entries = delta.manifest.entries
        delta_manifest_entries += len(manifest_entries)
        for entry in manifest_entries:
            delta_bytes += entry.meta.content_length
            if not delta_stats:
                delta_bytes_pyarrow = delta_bytes * PYARROW_INFLATION_MULTIPLIER
        latest_stream_position = max(position, latest_stream_position)
        if delta_bytes_pyarrow > worker_obj_store_mem:
            logger.info(
                f"Input deltas limited to "
                f"{len(limited_input_da_list)} by object store mem "
                f"({delta_bytes_pyarrow} > {worker_obj_store_mem})")
            break
        delta_annotated = DeltaAnnotated.of(delta)
        limited_input_da_list.append(delta_annotated)

    logger.info(f"Input deltas to compact this round: "
                f"{len(limited_input_da_list)}")
    logger.info(f"Input delta bytes to compact: {delta_bytes}")
    logger.info(f"Input delta files to compact: {delta_manifest_entries}")
    logger.info(f"Latest input delta stream position: {latest_stream_position}")

    if not limited_input_da_list:
        raise RuntimeError("No input deltas to compact!")

    # TODO (pdames): determine min hash buckets from size of all deltas
    #  (not just deltas for this round)
    min_hash_bucket_count = int(max(
        math.ceil(delta_bytes_pyarrow / worker_obj_store_mem_per_task),
        min(worker_cpus, 256),
    ))
    logger.info(f"Minimum recommended hash buckets: {min_hash_bucket_count}")

    if hash_bucket_count is None:
        # TODO (pdames): calc default hash buckets from table growth rate... as
        #  this stands, we don't know whether we're provisioning insufficient
        #  hash buckets for the next 5 minutes of deltas or more than enough
        #  for the next 10 years
        hash_bucket_count = min_hash_bucket_count
        logger.info(f"Using default hash bucket count: {hash_bucket_count}")

    if hash_bucket_count < min_hash_bucket_count:
        logger.warning(
            f"Provided hash bucket count ({hash_bucket_count}) "
            f"is less than the min recommended ({min_hash_bucket_count}). "
            f"This compaction job run may run out of memory, or run slowly. To "
            f"resolve this problem either specify a larger number of hash "
            f"buckets when running compaction, omit a custom hash bucket "
            f"count when running compaction, or provision workers with more "
            f"task memory per CPU.")

    hash_bucket_chunk_size = user_hash_bucket_chunk_size
    max_hash_bucket_chunk_size = math.ceil(
        worker_obj_store_mem_per_task / PYARROW_INFLATION_MULTIPLIER
    )
    logger.info(f"Max hash bucket chunk size: {max_hash_bucket_chunk_size}")
    if hash_bucket_chunk_size > max_hash_bucket_chunk_size:
        # TODO (pdames): note type of memory to increase (task or object store)
        logger.warning(
            f"Provided hash bucket chunk size "
            f"({user_hash_bucket_chunk_size}) is greater than the max "
            f"recommended ({max_hash_bucket_chunk_size}). This compaction "
            f"job may run out of memory. To resolve this problem either "
            f"specify a smaller hash bucket chunk size when running "
            f"compaction, omit a custom hash bucket chunk size when running "
            f"compaction, or provision workers with more task and object "
            f"store memory per CPU.")
    elif not hash_bucket_chunk_size:
        hash_bucket_chunk_size_load_balanced = max(
            math.ceil(max(delta_bytes, delta_bytes_pyarrow) / worker_cpus),
            BYTES_PER_MEBIBYTE,
        )
        hash_bucket_chunk_size = min(
            max_hash_bucket_chunk_size,
            hash_bucket_chunk_size_load_balanced,
        )
        logger.info(f"Default hash bucket chunk size: {hash_bucket_chunk_size}")

    rebatched_da_list = DeltaAnnotated.rebatch(
        limited_input_da_list,
        hash_bucket_chunk_size,
    )

    logger.info(f"Hash bucket chunk size: {hash_bucket_chunk_size}")
    logger.info(f"Hash bucket count: {hash_bucket_count}")
    logger.info(f"Input uniform delta count: {len(rebatched_da_list)}")

    return rebatched_da_list, hash_bucket_count, latest_stream_position
