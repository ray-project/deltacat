import logging
import ray
from collections import defaultdict
from ray import ray_constants
from deltacat.types import media
from deltacat import logs
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.storage.model import delta_manifest as dm, delta_locator as dl, \
    partition_locator as pl, stream_locator as sl, delta_staging_area as dsa
from deltacat.compute.compactor.steps import hash_bucket as hb, dedupe as dd, \
    materialize as mat
from deltacat.compute.compactor.model import materialize_result as mr, \
    dedupe_result as dr, round_completion_info as rci
from deltacat.compute.compactor.utils import round_completion_file as rcf, io
from deltacat.types.media import ContentType
from deltacat.utils.common import current_time_ms
from typing import Any, Dict, Tuple, Set, List

logger = logs.configure_application_logger(logging.getLogger(__name__))

_SORT_KEY_NAME_INDEX = 0
_SORT_KEY_ORDER_INDEX = 1


def check_preconditions(
        sort_key_names: List[str],
        max_records_per_output_file: int):

    assert len(sort_key_names) == len(set(sort_key_names)), \
        f"Sort key names must be unique: {sort_key_names}"
    assert max_records_per_output_file >= 1, \
        "Max records per output file must be a positive value"


def compact_partition(
        source_partition_locator: Dict[str, Any],
        compacted_partition_locator: Dict[str, Any],
        column_names: List[str],
        primary_keys: Set[str],
        hash_bucket_count: int,
        compaction_artifact_s3_bucket: str,
        last_stream_position_to_compact: int,
        deltacat_storage=unimplemented_deltacat_storage,
        sort_keys: List[Tuple[str, str]] = None,
        records_per_primary_key_index_file: int = 38_000_000,
        records_per_compacted_file: int = 4_000_000,
        min_hash_bucket_chunk_size: int = 0,
        compacted_file_content_type=ContentType.PARQUET):

    logger.info(f"Starting compaction session for: {source_partition_locator}")
    delta_staging_area = None
    compaction_rounds_executed = 0
    has_next_compaction_round = True
    while has_next_compaction_round:
        has_next_compaction_round, delta_staging_area = \
            execute_compaction_round(
                source_partition_locator,
                compacted_partition_locator,
                column_names,
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
            )
        compacted_partition_locator = dsa.get_partition_locator(
            delta_staging_area
        )
        compaction_rounds_executed += 1
    logger.info(f"Compaction session data processing completed in "
                f"{compaction_rounds_executed} rounds.")
    if delta_staging_area:
        logger.info(f"Committing compacted partition to: "
                    f"{compacted_partition_locator}")
        partition = deltacat_storage.commit_partition(delta_staging_area)
        logger.info(f"Committed compacted partition: {partition}")
    logger.info(f"Completed compaction session for: {source_partition_locator}")


def execute_compaction_round(
        source_partition_locator: Dict[str, Any],
        compacted_partition_locator: Dict[str, Any],
        column_names: List[str],
        primary_keys: Set[str],
        hash_bucket_count: int,
        compaction_artifact_s3_bucket: str,
        last_stream_position_to_compact: int,
        deltacat_storage=unimplemented_deltacat_storage,
        sort_keys: List[Tuple[str, str]] = None,
        records_per_primary_key_index_file: int = 38_000_000,
        records_per_compacted_file: int = 4_000_000,
        min_hash_bucket_chunk_size: int = 0,
        compacted_file_content_type=media.ContentType.PARQUET):

    if sort_keys is None:
        sort_keys = []

    # check preconditions before doing any computationally expensive work
    sort_key_names = [sk[_SORT_KEY_NAME_INDEX] for sk in sort_keys]
    check_preconditions(
        sort_key_names,
        records_per_compacted_file,
    )

    # sort primary keys to produce the same pk digest regardless of input order
    primary_keys = sorted(primary_keys)

    # read the results from any previously completed compaction round
    round_completion_info = rcf.read_round_completion_file(
        compaction_artifact_s3_bucket,
        source_partition_locator,
    )
    pki_version = None
    prev_compacted_stream_position = None
    if round_completion_info:
        pki_version = rci.get_primary_key_index_version(round_completion_info)
        delta_locator = rci.get_compacted_delta_locator(round_completion_info)
        prev_compacted_stream_position = dl.get_stream_position(delta_locator)
        logger.info(f"Round completion file contents: {round_completion_info}")
    else:
        logger.info(f"No prior round completion file found.")

    # discover input delta files
    input_deltas = io.discover_deltas(
        source_partition_locator,
        round_completion_info,
        last_stream_position_to_compact,
        deltacat_storage,
    )
    if not input_deltas:
        logger.info("No input deltas found to compact.")
        return False, None

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

    sized_delta_manifests, hash_bucket_count, last_stream_position_compacted = \
        io.limit_input_deltas(
            input_deltas,
            cluster_resources,
            hash_bucket_count,
            min_hash_bucket_chunk_size,
            round_completion_info,
        )

    # first group like primary keys together by hashing them into buckets
    hb_tasks_pending = []
    for i in range(len(sized_delta_manifests)):
        # force strict round-robin scheduling of tasks across cluster workers
        node_id = node_idx_to_id[i % len(node_idx_to_id)]
        hb_resources = {node_id: ray_constants.MIN_RESOURCE_GRANULARITY}
        hb_task_promise, _ = hb.hash_bucket.options(resources=hb_resources)\
            .remote(
                sized_delta_manifests[i],
                column_names,
                primary_keys,
                sort_key_names,
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
        for hash_group_index in range(len(hash_group_idx_to_obj_id)):
            object_id = hash_group_idx_to_obj_id[hash_group_index]
            if object_id:
                all_hash_group_idx_to_obj_id[hash_group_index].append(object_id)
    hash_group_count = dedupe_task_count = len(all_hash_group_idx_to_obj_id)
    logger.info(f"Hash bucket groups created: {hash_group_count}")

    new_compacted_delta_stream_position = current_time_ms()

    compacted_stream_locator = pl.get_stream_locator(
        compacted_partition_locator
    )
    partition_staging_area = deltacat_storage.get_partition_staging_area(
        sl.get_namespace(compacted_stream_locator),
        sl.get_table_name(compacted_stream_locator),
        sl.get_table_version(compacted_stream_locator),
    )
    delta_staging_area = deltacat_storage.stage_partition(
        partition_staging_area,
        pl.get_partition_values(compacted_partition_locator),
    )
    new_compacted_partition_locator = dsa.get_partition_locator(
        delta_staging_area
    )
    new_compacted_delta_locator = dl.of(
        new_compacted_partition_locator,
        new_compacted_delta_stream_position,
    )

    # TODO (pdames): when resources are freed during the last round of hash
    #  bucketing, start running dedupe tasks that read existing dedupe
    #  output from S3 then wait for hash bucketing to finish before continuing

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
        dd_task_promise, _, dd_stat = dd.dedupe.options(resources=dd_resources)\
            .remote(
                compaction_artifact_s3_bucket,
                compacted_partition_locator,
                new_compacted_partition_locator,
                object_ids,
                sort_keys,
                records_per_primary_key_index_file,
                records_per_compacted_file,
                num_materialize_buckets,
                i,
                record_counts_pending_materialize,
                prev_compacted_stream_position,
                pki_version,
            )
        dd_tasks_pending.append(dd_task_promise)
        dd_stats_promises.append(dd_stat)
        i += 1
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe results...")
    dd_results = ray.get(dd_tasks_pending)
    logger.info(f"Got {len(dd_results)} dedupe results.")
    all_mat_buckets_to_obj_id = defaultdict(list)
    for mat_bucket_idx_to_obj_id in dd_results:
        for mat_bucket_index, dd_task_index_and_object_id_tuple in \
                mat_bucket_idx_to_obj_id.items():
            all_mat_buckets_to_obj_id[mat_bucket_index].append(
                dd_task_index_and_object_id_tuple)
    logger.info(f"Getting {len(dd_stats_promises)} dedupe result statistics...")
    dd_stats = ray.get(dd_stats_promises)
    logger.info(f"Got {len(dd_stats)} dedupe result statistics.")

    # TODO(pdames): when resources are freed during the last round of deduping
    #  start running materialize tasks that read materialization source file
    #  tables from S3 then wait for deduping to finish before continuing

    # TODO(pdames): balance inputs to materialization tasks to ensure that each
    #  task has an approximately equal amount of input to materialize

    # TODO(pdames): garbage collect hash bucket output since it's no longer
    #  needed

    mat_tasks_pending = []
    i = 0
    for mat_bucket_index, dedupe_task_index_and_object_id_tuples in all_mat_buckets_to_obj_id.items():
        # force strict round-robin scheduling of tasks across cluster workers
        node_id = node_idx_to_id[i % len(node_idx_to_id)]
        mat_resources = {node_id: ray_constants.MIN_RESOURCE_GRANULARITY}
        mat_task_promise = mat\
            .materialize.options(resources=mat_resources).remote(
                source_partition_locator,
                delta_staging_area,
                mat_bucket_index,
                dedupe_task_index_and_object_id_tuples,
                records_per_compacted_file,
                compacted_file_content_type,
                deltacat_storage,
            )
        mat_tasks_pending.append(mat_task_promise)
        i += 1

    logger.info(f"Getting {len(mat_tasks_pending)} materialize results...")
    mat_results = ray.get(mat_tasks_pending)
    logger.info(f"Got {len(mat_results)} materialize results.")

    delta_manifests = sorted(
        [mr.get_delta_manifest(m) for m in mat_results],
        key=lambda m: mr.get_task_index(m),
    )
    compacted_delta = deltacat_storage.commit_delta(
        dm.merge_delta_manifests(delta_manifests),
        stream_position=new_compacted_delta_stream_position,
    )
    logger.info(f"Committed compacted delta: {compacted_delta}")

    round_completion_info = rci.of(
        hash_bucket_count,
        last_stream_position_compacted,
        new_compacted_delta_locator,
        sum([mr.get_files(m) for m in mat_results]),
        sum([mr.get_file_bytes(m) for m in mat_results]),
        sum([mr.get_pyarrow_bytes(m) for m in mat_results]),
        sum([mr.get_records(m) for m in mat_results]),
        sum([dr.get_files(d) for d in dd_stats]),
        sum([dr.get_file_bytes(d) for d in dd_stats]),
        sum([dr.get_pyarrow_bytes(d) for d in dd_stats]),
        sum([dr.get_records(d) for d in dd_stats]),
        1,
    )
    rcf.write_round_completion_file(
        compaction_artifact_s3_bucket,
        source_partition_locator,
        round_completion_info,
    )
    return (last_stream_position_compacted < last_stream_position_to_compact), \
           delta_staging_area
