import logging
import ray
from collections import defaultdict
from ray import ray_constants
from deltacat.types import media
from deltacat import logs
from deltacat.storage import Delta, DeltaLocator, Partition, \
    PartitionLocator, interface as unimplemented_deltacat_storage
from deltacat.compute.compactor.steps import hash_bucket as hb, dedupe as dd, \
    materialize as mat
from deltacat.compute.compactor import SortKey, PrimaryKeyIndexMeta, \
    PrimaryKeyIndexLocator, PrimaryKeyIndexVersionMeta, \
    PrimaryKeyIndexVersionLocator, MaterializeResult, RoundCompletionInfo, \
    PyArrowWriteResult
from deltacat.compute.compactor.utils import round_completion_file as rcf, io, \
    primary_key_index as pki
from deltacat.types.media import ContentType
from typing import List, Set, Optional, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

_SORT_KEY_NAME_INDEX: int = 0
_SORT_KEY_ORDER_INDEX: int = 1
_PRIMARY_KEY_INDEX_ALGORITHM_VERSION: str = "1.0"


def check_preconditions(
        source_partition_locator: PartitionLocator,
        compacted_partition_locator: PartitionLocator,
        sort_keys: List[SortKey],
        max_records_per_output_file: int,
        new_hash_bucket_count: Optional[int],
        deltacat_storage=unimplemented_deltacat_storage) -> int:

    assert source_partition_locator.partition_values \
           == compacted_partition_locator.partition_values, \
        "In-place compaction must use the same partition values for the " \
        "source and destination."
    assert max_records_per_output_file >= 1, \
        "Max records per output file must be a positive value"
    if new_hash_bucket_count is not None:
        assert new_hash_bucket_count >= 1, \
            "New hash bucket count must be a positive value"
    return SortKey.validate_sort_keys(
        source_partition_locator,
        sort_keys,
        deltacat_storage,
    )


def compact_partition(
        source_partition_locator: PartitionLocator,
        compacted_partition_locator: PartitionLocator,
        primary_keys: Set[str],
        hash_bucket_count: Optional[int],
        compaction_artifact_s3_bucket: str,
        last_stream_position_to_compact: int,
        deltacat_storage=unimplemented_deltacat_storage,
        sort_keys: List[SortKey] = None,
        records_per_primary_key_index_file: int = 38_000_000,
        records_per_compacted_file: int = 4_000_000,
        min_hash_bucket_chunk_size: int = 0,
        compacted_file_content_type: ContentType = ContentType.PARQUET,
        delete_prev_primary_key_index: bool = False):

    logger.info(f"Starting compaction session for: {source_partition_locator}")
    partition = None
    compaction_rounds_executed = 0
    has_next_compaction_round = True
    while has_next_compaction_round:
        has_next_compaction_round, partition = \
            _execute_compaction_round(
                source_partition_locator,
                compacted_partition_locator,
                primary_keys,
                hash_bucket_count,
                compaction_artifact_s3_bucket,
                last_stream_position_to_compact,
                deltacat_storage,
                sort_keys,
                records_per_primary_key_index_file,
                records_per_compacted_file,
                min_hash_bucket_chunk_size,
                compacted_file_content_type,
                delete_prev_primary_key_index,
            )
        if partition:
            compacted_partition_locator = partition.locator
            compaction_rounds_executed += 1
    logger.info(f"Compaction session data processing completed in "
                f"{compaction_rounds_executed} rounds.")
    if partition:
        logger.info(f"Committing compacted partition to: "
                    f"{compacted_partition_locator}")
        partition = deltacat_storage.commit_partition(partition)
        logger.info(f"Committed compacted partition: {partition}")
    logger.info(f"Completed compaction session for: {source_partition_locator}")


def _execute_compaction_round(
        source_partition_locator: PartitionLocator,
        compacted_partition_locator: PartitionLocator,
        primary_keys: Set[str],
        new_hash_bucket_count: Optional[int],
        compaction_artifact_s3_bucket: str,
        last_stream_position_to_compact: int,
        deltacat_storage=unimplemented_deltacat_storage,
        sort_keys: List[SortKey] = None,
        records_per_primary_key_index_file: int = 38_000_000,
        records_per_compacted_file: int = 4_000_000,
        min_hash_bucket_chunk_size: int = 0,
        compacted_file_content_type: ContentType = media.ContentType.PARQUET,
        delete_prev_primary_key_index: bool = False) \
        -> Tuple[bool, Optional[Partition]]:

    if not primary_keys:
        # TODO (pdames): run simple rebatch to reduce all deltas into 1 delta
        #  with normalized manifest entry sizes
        raise NotImplementedError(
            "Compaction only supports tables with 1 or more primary keys")
    if sort_keys is None:
        sort_keys = []
    # TODO (pdames): detect and handle schema evolution (at least ensure that
    #  we don't recompact simple backwards-compatible changes like constraint
    #  widening and null column additions).
    # TODO (pdames): detect and optimize in-place compaction

    # check preconditions before doing any computationally expensive work
    bit_width_of_sort_keys = check_preconditions(
        source_partition_locator,
        compacted_partition_locator,
        sort_keys,
        records_per_compacted_file,
        new_hash_bucket_count,
        deltacat_storage,
    )

    # sort primary keys to produce the same pk digest regardless of input order
    primary_keys = sorted(primary_keys)

    # collect cluster resource stats
    cluster_resources = ray.cluster_resources()
    logger.info(f"Total cluster resources: {cluster_resources}")
    logger.info(f"Available cluster resources: {ray.available_resources()}")
    cluster_cpus = int(cluster_resources["CPU"])
    logger.info(f"Total cluster CPUs: {cluster_cpus}")

    # assign a distinct index to each node in the cluster
    # head_node_ip = urllib.request.urlopen(
    #     "http://169.254.169.254/latest/meta-data/local-ipv4"
    # ).read().decode("utf-8")
    # print(f"head node ip: {head_node_ip}")
    next_node_idx = 0
    node_idx_to_id = {}
    for resource_name in cluster_resources.keys():
        if resource_name.startswith("node:"):
            # if head_node_ip not in resource_name:
            node_idx_to_id[next_node_idx] = resource_name
            next_node_idx += 1
    logger.info(f"Assigned indices to {len(node_idx_to_id)} cluster nodes.")
    logger.info(f"Cluster node indices to resource IDs: {node_idx_to_id}")

    # set max task parallelism equal to total cluster CPUs...
    # we assume here that we're running on a fixed-size cluster - this
    # assumption could be removed but we'd still need to know the maximum
    # "safe" number of parallel tasks that our autoscaling cluster could handle
    max_parallelism = cluster_cpus
    logger.info(f"Max parallelism: {max_parallelism}")

    # get the root path of a compatible primary key index for this round
    compatible_primary_key_index_meta = PrimaryKeyIndexMeta.of(
        compacted_partition_locator,
        primary_keys,
        sort_keys,
        _PRIMARY_KEY_INDEX_ALGORITHM_VERSION,
    )
    compatible_primary_key_index_locator = PrimaryKeyIndexLocator.of(
        compatible_primary_key_index_meta)
    compatible_primary_key_index_root_path = \
        compatible_primary_key_index_locator.primary_key_index_root_path

    # read the results from any previously completed compaction round that used
    # a compatible primary key index
    round_completion_info = rcf.read_round_completion_file(
        compaction_artifact_s3_bucket,
        source_partition_locator,
        compatible_primary_key_index_root_path,
    )

    # read the previous compaction round's hash bucket count, if any
    old_hash_bucket_count = None
    if round_completion_info:
        old_pki_version_locator = round_completion_info\
            .primary_key_index_version_locator
        old_hash_bucket_count = old_pki_version_locator\
            .primary_key_index_version_meta \
            .hash_bucket_count

    # use the new hash bucket count if provided, or fall back to old count
    hash_bucket_count = new_hash_bucket_count \
        if new_hash_bucket_count is not None \
        else old_hash_bucket_count

    # discover input delta files
    high_watermark = round_completion_info.high_watermark \
        if round_completion_info else None
    input_deltas = io.discover_deltas(
        source_partition_locator,
        high_watermark,
        last_stream_position_to_compact,
        deltacat_storage,
    )
    if not input_deltas:
        logger.info("No input deltas found to compact.")
        return False, None

    # limit the input deltas to fit on this cluster and convert them to
    # annotated deltas of equivalent size for easy parallel distribution
    uniform_deltas, hash_bucket_count, last_stream_position_compacted = \
        io.limit_input_deltas(
            input_deltas,
            cluster_resources,
            hash_bucket_count,
            min_hash_bucket_chunk_size,
            deltacat_storage,
        )
    assert hash_bucket_count is not None and hash_bucket_count > 0, \
        f"Unexpected Error: Default hash bucket count ({hash_bucket_count}) " \
        f"is invalid."

    # rehash the primary key index if necessary
    if round_completion_info:
        logger.info(f"Round completion file contents: {round_completion_info}")
        # the previous primary key index is compatible with the current, but
        # will need to be rehashed if the hash bucket count has changed
        if hash_bucket_count != old_hash_bucket_count:
            round_completion_info = pki.rehash(
                node_idx_to_id,
                compaction_artifact_s3_bucket,
                source_partition_locator,
                round_completion_info,
                hash_bucket_count,
                max_parallelism,
                records_per_primary_key_index_file,
                delete_prev_primary_key_index,
            )
    else:
        logger.info(f"No prior round completion file found. Source partition: "
                    f"{source_partition_locator}. Primary key index locator: "
                    f"{compatible_primary_key_index_locator}")

    # parallel step 1:
    # group like primary keys together by hashing them into buckets
    hb_tasks_pending = []
    for i, uniform_delta in enumerate(uniform_deltas):
        # force strict round-robin scheduling of tasks across cluster workers
        node_id = node_idx_to_id[i % len(node_idx_to_id)]
        hb_resources = {node_id: ray_constants.MIN_RESOURCE_GRANULARITY}
        hb_task_promise, _ = hb.hash_bucket \
            .options(resources=hb_resources) \
            .remote(
                uniform_delta,
                primary_keys,
                sort_keys,
                hash_bucket_count,
                max_parallelism,
                deltacat_storage,
            )
        hb_tasks_pending.append(hb_task_promise)
    logger.info(f"Getting {len(hb_tasks_pending)} hash bucket results...")
    hb_results = ray.get(hb_tasks_pending)
    logger.info(f"Got {len(hb_results)} hash bucket results.")
    all_hash_group_idx_to_obj_id = defaultdict(list)
    for hash_group_idx_to_obj_id in hb_results:
        for hash_group_index, object_id in enumerate(hash_group_idx_to_obj_id):
            if object_id:
                all_hash_group_idx_to_obj_id[hash_group_index].append(object_id)
    hash_group_count = dedupe_task_count = len(all_hash_group_idx_to_obj_id)
    logger.info(f"Hash bucket groups created: {hash_group_count}")

    # TODO (pdames): when resources are freed during the last round of hash
    #  bucketing, start running dedupe tasks that read existing dedupe
    #  output from S3 then wait for hash bucketing to finish before continuing

    # create a new stream for this round
    compacted_stream_locator = compacted_partition_locator.stream_locator
    stream = deltacat_storage.get_stream(
        compacted_stream_locator.namespace,
        compacted_stream_locator.table_name,
        compacted_stream_locator.table_version,
    )
    partition = deltacat_storage.stage_partition(
        stream,
        compacted_partition_locator.partition_values,
    )
    new_compacted_partition_locator = partition.locator

    # generate a new primary key index locator for this round
    new_primary_key_index_meta = PrimaryKeyIndexMeta.of(
        new_compacted_partition_locator,
        primary_keys,
        sort_keys,
        _PRIMARY_KEY_INDEX_ALGORITHM_VERSION,
    )
    new_primary_key_index_locator = PrimaryKeyIndexLocator.of(
        new_primary_key_index_meta)
    new_primary_key_index_root_path = new_primary_key_index_locator\
        .primary_key_index_root_path

    # generate a new primary key index version locator for this round
    new_primary_key_index_version_meta = PrimaryKeyIndexVersionMeta.of(
        new_primary_key_index_meta,
        hash_bucket_count,
    )
    new_pki_version_locator = PrimaryKeyIndexVersionLocator.generate(
        new_primary_key_index_version_meta)

    # parallel step 2:
    # discover records with duplicate primary keys in each hash bucket, and
    # identify the index of records to keep or drop based on sort keys
    dd_tasks_pending = []
    dd_stats_promises = []
    num_materialize_buckets = max_parallelism
    record_counts_pending_materialize = \
        dd.RecordCountsPendingMaterialize.remote(dedupe_task_count)
    i = 0
    for hash_group_index, object_ids in all_hash_group_idx_to_obj_id.items():
        # force strict round-robin scheduling of tasks across cluster workers
        node_id = node_idx_to_id[i % len(node_idx_to_id)]
        dd_resources = {node_id: ray_constants.MIN_RESOURCE_GRANULARITY}
        dd_task_promise, _, dd_stat = dd.dedupe \
            .options(resources=dd_resources) \
            .remote(
                compaction_artifact_s3_bucket,
                round_completion_info,
                new_pki_version_locator,
                object_ids,
                sort_keys,
                records_per_primary_key_index_file,
                records_per_compacted_file,
                num_materialize_buckets,
                i,
                delete_prev_primary_key_index,
                record_counts_pending_materialize,
            )
        dd_tasks_pending.append(dd_task_promise)
        dd_stats_promises.append(dd_stat)
        i += 1
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe results...")
    dd_results = ray.get(dd_tasks_pending)
    logger.info(f"Got {len(dd_results)} dedupe results.")
    all_mat_buckets_to_obj_id = defaultdict(list)
    for mat_bucket_idx_to_obj_id in dd_results:
        for bucket_idx, dd_task_index_and_object_id_tuple in \
                mat_bucket_idx_to_obj_id.items():
            all_mat_buckets_to_obj_id[bucket_idx].append(
                dd_task_index_and_object_id_tuple)
    logger.info(f"Getting {len(dd_stats_promises)} dedupe result stat(s)...")
    pki_stats = ray.get(dd_stats_promises)
    logger.info(f"Got {len(pki_stats)} dedupe result stat(s).")

    # TODO(pdames): when resources are freed during the last round of deduping
    #  start running materialize tasks that read materialization source file
    #  tables from S3 then wait for deduping to finish before continuing

    # TODO(pdames): balance inputs to materialization tasks to ensure that each
    #  task has an approximately equal amount of input to materialize

    # TODO(pdames): garbage collect hash bucket output since it's no longer
    #  needed

    # parallel step 3:
    # materialize records to keep by index
    mat_tasks_pending = []
    i = 0
    for bucket_idx, dd_idx_obj_id_tuples in all_mat_buckets_to_obj_id.items():
        # force strict round-robin scheduling of tasks across cluster workers
        node_id = node_idx_to_id[i % len(node_idx_to_id)]
        mat_resources = {node_id: ray_constants.MIN_RESOURCE_GRANULARITY}
        mat_task_promise = mat.materialize \
            .options(resources=mat_resources) \
            .remote(
                source_partition_locator,
                partition,
                bucket_idx,
                dd_idx_obj_id_tuples,
                records_per_compacted_file,
                compacted_file_content_type,
                deltacat_storage,
            )
        mat_tasks_pending.append(mat_task_promise)
        i += 1

    logger.info(f"Getting {len(mat_tasks_pending)} materialize result(s)...")
    mat_results: List[MaterializeResult] = ray.get(mat_tasks_pending)
    logger.info(f"Got {len(mat_results)} materialize result(s).")

    mat_results = sorted(mat_results, key=lambda m: m.task_index)
    deltas = [m.delta for m in mat_results]
    merged_delta = Delta.merge_deltas(deltas)
    compacted_delta = deltacat_storage.commit_delta(merged_delta)
    logger.info(f"Committed compacted delta: {compacted_delta}")

    new_compacted_delta_locator = DeltaLocator.of(
        new_compacted_partition_locator,
        compacted_delta.stream_position,
    )

    round_completion_info = RoundCompletionInfo.of(
        last_stream_position_compacted,
        new_compacted_delta_locator,
        PyArrowWriteResult.union([m.pyarrow_write_result
                                  for m in mat_results]),
        PyArrowWriteResult.union(pki_stats),
        bit_width_of_sort_keys,
        new_pki_version_locator,
    )
    rcf.write_round_completion_file(
        compaction_artifact_s3_bucket,
        source_partition_locator,
        new_primary_key_index_root_path,
        round_completion_info,
    )
    return \
        (last_stream_position_compacted < last_stream_position_to_compact), \
        partition
