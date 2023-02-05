import logging
import functools
import ray
from ray import cloudpickle

from collections import OrderedDict, defaultdict

from deltacat import logs
from deltacat.compute.stats.models.delta_stats import DeltaStats
from deltacat.storage import Delta, DeltaLocator, Partition, \
    PartitionLocator, interface as unimplemented_deltacat_storage
from deltacat.utils.ray_utils.concurrency import invoke_parallel, \
    round_robin_options_provider
from deltacat.utils.ray_utils.runtime import live_node_resource_keys
from deltacat.compute.compactor.steps import hash_bucket as hb, dedupe as dd, \
    materialize as mat, write_pk_index as wpk
from deltacat.compute.compactor import SortKey, PrimaryKeyIndexMeta, \
    PrimaryKeyIndexLocator, PrimaryKeyIndexVersionMeta, \
    PrimaryKeyIndexVersionLocator, RoundCompletionInfo, \
    PyArrowWriteResult, DeltaFileLocator
from deltacat.compute.compactor.utils import round_completion_file as rcf, io, \
    primary_key_index as pki
from deltacat.types.media import ContentType
from deltacat.utils.placement import PlacementGroupConfig
from typing import List, Set, Optional, Tuple, Dict
from deltacat.compute.compactor.utils.materialize_utils import delta_file_locator_to_mat_bucket_index

import pyarrow as pa
import numpy as np

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

_PRIMARY_KEY_INDEX_ALGORITHM_VERSION: str = "1.0"

def _aggregate_record_counts(record_counts: List[Dict[DeltaFileLocator, np.int32]],
                                num_materialize_buckets: int,
                                max_rows_per_mat_file: int) -> Tuple[Dict, Dict]:
    all_record_counts = defaultdict(
        lambda: defaultdict(int))

    logger.info(f"Total record counts to aggregate: {len(record_counts)}")
    tot = 0
    mat_bucket_to_src_df_locator_np = defaultdict(list)
    for dedupe_task_index, src_records_counts in enumerate(record_counts):
        for df_locator, count in src_records_counts.items():
            mat_bucket = delta_file_locator_to_mat_bucket_index(df_locator=df_locator, 
                                                                materialize_bucket_count=num_materialize_buckets)
            tot += 1
            mat_bucket_to_src_df_locator_np[mat_bucket].append(df_locator)
            all_record_counts[df_locator][dedupe_task_index] += count.item()

    # free record counts in the favor of dicts
    del record_counts

    logger.info(f"Total records processed during aggregate: {tot}")
    dest_file_indices = defaultdict(
        lambda: defaultdict(np.int32)
    )
    dest_file_row_indices = defaultdict(
        lambda: defaultdict(np.int32)
    )
    logger.info(f"Total dfls {len(all_record_counts)}")

    file_idx = 0
    prev_file_idx = 0

    logger.info(f"Determining the destinaton file indices and row indices.")
    for mat_bucket in range(num_materialize_buckets):
        mat_bucket_row_idx = 0
        logger.info(f"Aggregating for mat bucket: {mat_bucket}")
        sorted_src_dfls = sorted(mat_bucket_to_src_df_locator_np[mat_bucket])
        logger.info(f"Sorted dfls in {mat_bucket} are {len(sorted_src_dfls)}")
        for src_dfl in sorted_src_dfls:
            sorted_dd_tasks = sorted(all_record_counts[src_dfl].keys())
            for dd_task_idx in sorted_dd_tasks:
                dest_file_row_indices[dd_task_idx][src_dfl] = \
                    np.int32(mat_bucket_row_idx % max_rows_per_mat_file)
                file_idx = prev_file_idx + int(
                    mat_bucket_row_idx / max_rows_per_mat_file
                )
                dest_file_indices[dd_task_idx][src_dfl] = np.int32(file_idx)
                row_count = all_record_counts[src_dfl][dd_task_idx]
                mat_bucket_row_idx += row_count
        prev_file_idx = file_idx + 1

    # clear in the favor of dest_file indices. 
    del all_record_counts
    del mat_bucket_to_src_df_locator_np

    dest_file_indices_ref = defaultdict()
    dest_file_row_indices_ref = defaultdict()

    for dd_task_index, src_dfl_to_indices in dest_file_indices.items():
        logger.info(f"Putting {len(src_dfl_to_indices)} src_dfl_indices in object store")
        object_ref = ray.put(src_dfl_to_indices)
        dest_file_indices_ref[dd_task_index] = cloudpickle.dumps(object_ref)
        del object_ref

    for dd_task_index, src_dfl_to_row_indices in dest_file_row_indices.items():
        logger.info(f"Putting {len(src_dfl_to_row_indices)} src_dfl_row_indices in object store")
        object_ref = ray.put(src_dfl_to_row_indices)
        dest_file_row_indices_ref[dd_task_index] = cloudpickle.dumps(object_ref)
        del object_ref

    return dest_file_indices_ref, dest_file_row_indices_ref

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
        compaction_artifact_s3_bucket: str,
        last_stream_position_to_compact: int,
        hash_bucket_count: Optional[int] = None,
        sort_keys: List[SortKey] = None,
        records_per_primary_key_index_file: int = 38_000_000,
        records_per_compacted_file: int = 4_000_000,
        input_deltas_stats: Dict[int, DeltaStats] = None,
        min_pk_index_pa_bytes: int = 0,
        min_hash_bucket_chunk_size: int = 0,
        compacted_file_content_type: ContentType = ContentType.PARQUET,
        delete_prev_primary_key_index: bool = False,
        read_round_completion: bool = False,
        pg_config: Optional[PlacementGroupConfig] = None,
        schema_on_read: Optional[pa.schema] = None,  # TODO (ricmiyam): Remove this and retrieve schema from storage API
        deltacat_storage=unimplemented_deltacat_storage):

    logger.info(f"Starting compaction session for: {source_partition_locator}")
    partition = None
    compaction_rounds_executed = 0
    has_next_compaction_round = True
    while has_next_compaction_round:
        has_next_compaction_round, new_partition, new_rci = \
            _execute_compaction_round(
                source_partition_locator,
                compacted_partition_locator,
                primary_keys,
                compaction_artifact_s3_bucket,
                last_stream_position_to_compact,
                hash_bucket_count,
                sort_keys,
                records_per_primary_key_index_file,
                records_per_compacted_file,
                input_deltas_stats,
                min_pk_index_pa_bytes,
                min_hash_bucket_chunk_size,
                compacted_file_content_type,
                delete_prev_primary_key_index,
                read_round_completion,
                schema_on_read,
                deltacat_storage=deltacat_storage,
                pg_config=pg_config
            )
        if new_partition:
            partition = new_partition
            compacted_partition_locator = new_partition.locator
            compaction_rounds_executed += 1
        # Take new primary key index sizes into account for subsequent compaction rounds and their dedupe steps
        if new_rci:
            min_pk_index_pa_bytes = new_rci.pk_index_pyarrow_write_result.pyarrow_bytes

    logger.info(f"Partition-{source_partition_locator.partition_values}-> Compaction session data processing completed in "
                f"{compaction_rounds_executed} rounds.")
    if partition:
        logger.info(f"Committing compacted partition to: {partition.locator}")
        partition = deltacat_storage.commit_partition(partition)
        logger.info(f"Committed compacted partition: {partition}")
    logger.info(f"Completed compaction session for: {source_partition_locator}")


def _execute_compaction_round(
        source_partition_locator: PartitionLocator,
        compacted_partition_locator: PartitionLocator,
        primary_keys: Set[str],
        compaction_artifact_s3_bucket: str,
        last_stream_position_to_compact: int,
        new_hash_bucket_count: Optional[int],
        sort_keys: List[SortKey],
        records_per_primary_key_index_file: int,
        records_per_compacted_file: int,
        input_deltas_stats: Dict[int, DeltaStats],
        min_pk_index_pa_bytes: int,
        min_hash_bucket_chunk_size: int,
        compacted_file_content_type: ContentType,
        delete_prev_primary_key_index: bool,
        read_round_completion: bool,
        schema_on_read: Optional[pa.schema],
        deltacat_storage = unimplemented_deltacat_storage,
        pg_config: Optional[PlacementGroupConfig] = None) \
        -> Tuple[bool, Optional[Partition], Optional[RoundCompletionInfo]]:


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

    cluster_resources = ray.cluster_resources()
    logger.info(f"Total cluster resources: {cluster_resources}")
    node_resource_keys = None
    if pg_config: # use resource in each placement group
        cluster_resources = pg_config.resource
        cluster_cpus = cluster_resources['CPU']   
    else: # use all cluster resource
        logger.info(f"Available cluster resources: {ray.available_resources()}")
        cluster_cpus = int(cluster_resources["CPU"])
        logger.info(f"Total cluster CPUs: {cluster_cpus}")
        node_resource_keys = live_node_resource_keys()
        logger.info(f"Found {len(node_resource_keys)} live cluster nodes: "
                   f"{node_resource_keys}") 

    # create a remote options provider to round-robin tasks across all nodes or allocated bundles
    logger.info(f"Setting round robin scheduling with node id:{node_resource_keys}")
    round_robin_opt_provider = functools.partial(
        round_robin_options_provider,
        resource_keys=node_resource_keys,
        pg_config = pg_config.opts if pg_config else None
    )

    # assign a distinct index to each node in the cluster
    # head_node_ip = urllib.request.urlopen(
    #     "http://169.254.169.254/latest/meta-data/local-ipv4"
    # ).read().decode("utf-8")
    # print(f"head node ip: {head_node_ip}")

    # set max task parallelism equal to total cluster CPUs...
    # we assume here that we're running on a fixed-size cluster - this
    # assumption could be removed but we'd still need to know the maximum
    # "safe" number of parallel tasks that our autoscaling cluster could handle
    max_parallelism = int(cluster_cpus)
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
    round_completion_info = None
    if read_round_completion:
        logger.info(f"Reading round completion file for compatible "
                    f"primary key index root path {compatible_primary_key_index_root_path}")
        round_completion_info = rcf.read_round_completion_file(
            compaction_artifact_s3_bucket,
            source_partition_locator,
            compatible_primary_key_index_root_path,
        )
        logger.info(f"Round completion file: {round_completion_info}")

    # read the previous compaction round's hash bucket count, if any
    old_hash_bucket_count = None
    if round_completion_info:
        old_pki_version_locator = round_completion_info\
            .primary_key_index_version_locator
        old_hash_bucket_count = old_pki_version_locator\
            .primary_key_index_version_meta \
            .hash_bucket_count
        min_pk_index_pa_bytes = round_completion_info.pk_index_pyarrow_write_result.pyarrow_bytes

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
        return False, None, None

    # limit the input deltas to fit on this cluster and convert them to
    # annotated deltas of equivalent size for easy parallel distribution

    uniform_deltas, hash_bucket_count, last_stream_position_compacted = \
        io.limit_input_deltas(
            input_deltas,
            cluster_resources,
            hash_bucket_count,
            min_pk_index_pa_bytes,
            min_hash_bucket_chunk_size,
            input_deltas_stats=input_deltas_stats,
            deltacat_storage=deltacat_storage
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
            # TODO(draghave): manually test the path after prior primary key 
            # index was already built 
            round_completion_info = pki.rehash(
                round_robin_opt_provider,
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
    hb_tasks_pending = invoke_parallel(
        items=uniform_deltas,
        ray_task=hb.hash_bucket,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        primary_keys=primary_keys,
        sort_keys=sort_keys,
        num_buckets=hash_bucket_count,
        num_groups=max_parallelism,
        deltacat_storage=deltacat_storage,
    )
    logger.info(f"Getting {len(hb_tasks_pending)} hash bucket results...")
    hb_results = ray.get([t[0] for t in hb_tasks_pending])
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
    num_materialize_buckets = max_parallelism
    logger.info(f"Materialize Bucket Count: {num_materialize_buckets}")
    dd_tasks_pending = invoke_parallel(
        items=all_hash_group_idx_to_obj_id.values(),
        ray_task=dd.dedupe,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        kwargs_provider=lambda index, item: {"dedupe_task_index": index,
                                             "object_ids": item},
        compaction_artifact_s3_bucket=compaction_artifact_s3_bucket,
        round_completion_info=round_completion_info,
        new_primary_key_index_version_locator=new_pki_version_locator,
        sort_keys=sort_keys,
        max_records_per_index_file=records_per_primary_key_index_file,
        num_materialize_buckets=num_materialize_buckets,
        delete_old_primary_key_index=delete_prev_primary_key_index
    )
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe results...")
    dd_results = ray.get([t[0] for t in dd_tasks_pending])
    logger.info(f"Got {len(dd_results)} dedupe results.")

    all_mat_buckets_to_obj_id = defaultdict(list)
    for mat_bucket_idx_to_obj_id in dd_results:
        for bucket_idx, dd_task_index_and_object_id_tuple in \
                mat_bucket_idx_to_obj_id.items():
            all_mat_buckets_to_obj_id[bucket_idx].append(
                dd_task_index_and_object_id_tuple)
    logger.info(f"Getting {len(dd_tasks_pending)} dedupe result stat(s)...")
    logger.info(f"Got {len(dd_tasks_pending)} dedupe result stat(s).")
    logger.info(f"Materialize buckets created: "
                f"{len(all_mat_buckets_to_obj_id)}")

    # -----------
    # Aggregate source file record counts to allow predicting 
    # record indexes. 
    # -----------
    logger.info(f"Aggregating the record counts from dedupe step...")
    source_file_record_counts = ray.get([t[2] for t in dd_tasks_pending])
    logger.info(f"Retrieved the record counts from all dedupe tasks")
    dest_file_indices_ref, dest_file_row_indices_ref = _aggregate_record_counts(
        source_file_record_counts,
        num_materialize_buckets,
        records_per_compacted_file)
    
    logger.info(f"Completed aggregating record counts from dedupe step.")

    deduped_tables_refs = ray.get([t[1] for t in dd_tasks_pending])
    logger.info(f"Writing primary key indexes for {len(deduped_tables_refs)} deduped table groups...")
    
    pk_results_pending = invoke_parallel(
        items=deduped_tables_refs,
        ray_task=wpk.write_pk_index,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        kwargs_provider=lambda index, item: {"dest_file_indices_ref": dest_file_indices_ref[index],
                                             "dest_file_row_indices_ref": dest_file_row_indices_ref[index],
                                             "dedupe_task_index": index,
                                             "deduped_tables_ref": item},
        compaction_artifact_s3_bucket=compaction_artifact_s3_bucket,
        new_primary_key_index_version_locator=new_pki_version_locator,
        max_rows_per_mat_file=records_per_compacted_file,
        max_records_per_index_file=records_per_primary_key_index_file
    )

    logger.info(f"Wrote pki for {len(deduped_tables_refs)} dedupe table groups.")

    if delete_prev_primary_key_index:
        pki.delete_primary_key_index_version(
            compaction_artifact_s3_bucket,
            round_completion_info.primary_key_index_version_locator,
        )

    logger.info(f"Getting {len(pk_results_pending)} pki stats...")
    pki_stats = ray.get(pk_results_pending)
    logger.info(f"Got {len(pki_stats)} pki stats.")

    # TODO(pdames): when resources are freed during the last round of deduping
    #  start running materialize tasks that read materialization source file
    #  tables from S3 then wait for deduping to finish before continuing

    # TODO(pdames): balance inputs to materialization tasks to ensure that each
    #  task has an approximately equal amount of input to materialize

    # TODO(pdames): garbage collect hash bucket output since it's no longer
    #  needed

    # parallel step 3:
    # materialize records to keep by index
    mat_tasks_pending = invoke_parallel(
        items=all_mat_buckets_to_obj_id.items(),
        ray_task=mat.materialize,
        max_parallelism=max_parallelism,
        options_provider=round_robin_opt_provider,
        kwargs_provider=lambda index, mat_bucket_idx_to_obj_id: {
            "mat_bucket_index": mat_bucket_idx_to_obj_id[0],
            "dedupe_task_idx_and_obj_id_tuples": mat_bucket_idx_to_obj_id[1],
        },
        schema=schema_on_read,
        round_completion_info=round_completion_info,
        source_partition_locator=source_partition_locator,
        partition=partition,
        max_records_per_output_file=records_per_compacted_file,
        compacted_file_content_type=compacted_file_content_type,
        deltacat_storage=deltacat_storage,
    )
    logger.info(f"Getting {len(mat_tasks_pending)} materialize result(s)...")
    mat_results = ray.get(mat_tasks_pending)
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
    logger.info(f"partition-{source_partition_locator.partition_values},compacted at:{last_stream_position_compacted}, last position:{last_stream_position_to_compact}")
    return \
        (last_stream_position_compacted < last_stream_position_to_compact), \
        partition, \
        round_completion_info

