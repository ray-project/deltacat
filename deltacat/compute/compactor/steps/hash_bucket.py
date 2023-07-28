import importlib
import logging
import time
from contextlib import nullcontext
from itertools import chain
from typing import Generator, List, Optional, Tuple
import numpy as np
import pyarrow as pa
import ray
from deltacat import logs
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
    SortKey,
    RoundCompletionInfo,
)
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
from deltacat.compute.compactor.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.compute.compactor.utils.primary_key_index import (
    group_hash_bucket_indices,
    group_record_indices_by_hash_bucket,
)
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import StorageType
from deltacat.utils.common import sha1_digest
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics, MetricsConfig
from deltacat.io.object_store import IObjectStore
from deltacat.utils.resources import get_current_node_peak_memory_usage_in_bytes

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

_PK_BYTES_DELIMITER = b"L6kl7u5f"


def _group_by_pk_hash_bucket(
    table: pa.Table, num_buckets: int, primary_keys: List[str]
) -> np.ndarray:
    # generate the primary key digest column
    all_pk_column_fields = []
    for pk_name in primary_keys:
        # casting a primary key column to numpy also ensures no nulls exist
        # TODO (pdames): catch error in cast to numpy and print user-friendly err msg.
        column_fields = table[pk_name].to_numpy()
        all_pk_column_fields.append(column_fields)
    hash_column_generator = _hash_pk_bytes_generator(all_pk_column_fields)
    table = sc.append_pk_hash_column(table, hash_column_generator)

    # drop primary key columns to free up memory
    table = table.drop(primary_keys)

    # group hash bucket record indices
    hash_bucket_to_indices = group_record_indices_by_hash_bucket(
        table,
        num_buckets,
    )

    # generate the ordered record number column
    hash_bucket_to_table = np.empty([num_buckets], dtype="object")
    for hb, indices in enumerate(hash_bucket_to_indices):
        if indices:
            hash_bucket_to_table[hb] = sc.append_record_idx_col(
                table.take(indices),
                indices,
            )
    return hash_bucket_to_table


def _hash_pk_bytes_generator(all_column_fields) -> Generator[bytes, None, None]:
    for field_index in range(len(all_column_fields[0])):
        bytes_to_join = []
        for column_fields in all_column_fields:
            bytes_to_join.append(bytes(str(column_fields[field_index]), "utf-8"))
        yield sha1_digest(_PK_BYTES_DELIMITER.join(bytes_to_join))


def _group_file_records_by_pk_hash_bucket(
    annotated_delta: DeltaAnnotated,
    num_hash_buckets: int,
    primary_keys: List[str],
    sort_key_names: List[str],
    is_src_delta: np.bool_ = True,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    deltacat_storage=unimplemented_deltacat_storage,
) -> Tuple[Optional[DeltaFileEnvelopeGroups], int]:
    # read input parquet s3 objects into a list of delta file envelopes
    delta_file_envelopes, total_record_count = _read_delta_file_envelopes(
        annotated_delta,
        primary_keys,
        sort_key_names,
        read_kwargs_provider,
        deltacat_storage,
    )
    if delta_file_envelopes is None:
        return None, 0

    # group the data by primary key hash value
    hb_to_delta_file_envelopes = np.empty([num_hash_buckets], dtype="object")
    for dfe in delta_file_envelopes:
        hash_bucket_to_table = _group_by_pk_hash_bucket(
            dfe.table,
            num_hash_buckets,
            primary_keys,
        )
        for hb, table in enumerate(hash_bucket_to_table):
            if table:
                if hb_to_delta_file_envelopes[hb] is None:
                    hb_to_delta_file_envelopes[hb] = []
                hb_to_delta_file_envelopes[hb].append(
                    DeltaFileEnvelope.of(
                        stream_position=dfe.stream_position,
                        file_index=dfe.file_index,
                        delta_type=dfe.delta_type,
                        table=table,
                        is_src_delta=is_src_delta,
                        file_record_count=dfe.file_record_count,
                    )
                )
    return hb_to_delta_file_envelopes, total_record_count


def _read_delta_file_envelopes(
    annotated_delta: DeltaAnnotated,
    primary_keys: List[str],
    sort_key_names: List[str],
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
) -> Tuple[Optional[List[DeltaFileEnvelope]], int]:

    columns_to_read = list(chain(primary_keys, sort_key_names))
    # TODO (rootliu) compare performance of column read from unpartitioned vs partitioned file
    # https://arrow.apache.org/docs/python/parquet.html#writing-to-partitioned-datasets
    tables = deltacat_storage.download_delta(
        annotated_delta,
        max_parallelism=1,
        columns=columns_to_read,
        file_reader_kwargs_provider=read_kwargs_provider,
        storage_type=StorageType.LOCAL,
    )
    annotations = annotated_delta.annotations
    assert (
        len(tables) == len(annotations),
        f"Unexpected Error: Length of downloaded delta manifest tables "
        f"({len(tables)}) doesn't match the length of delta manifest "
        f"annotations ({len(annotations)}).",
    )
    if not tables:
        return None, 0

    delta_file_envelopes = []
    total_record_count = 0
    for i, table in enumerate(tables):
        total_record_count += len(table)
        delta_file = DeltaFileEnvelope.of(
            stream_position=annotations[i].annotation_stream_position,
            file_index=annotations[i].annotation_file_index,
            delta_type=annotations[i].annotation_delta_type,
            table=table,
            file_record_count=len(table),
        )
        delta_file_envelopes.append(delta_file)
    return delta_file_envelopes, total_record_count


def _timed_hash_bucket(
    annotated_delta: DeltaAnnotated,
    round_completion_info: Optional[RoundCompletionInfo],
    primary_keys: List[str],
    sort_keys: List[SortKey],
    num_buckets: int,
    num_groups: int,
    enable_profiler: bool,
    read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    object_store: Optional[IObjectStore] = None,
    deltacat_storage=unimplemented_deltacat_storage,
):
    task_id = get_current_ray_task_id()
    worker_id = get_current_ray_worker_id()
    with memray.Tracker(
        f"hash_bucket_{worker_id}_{task_id}.bin"
    ) if enable_profiler else nullcontext():
        sort_key_names = [key.key_name for key in sort_keys]
        if not round_completion_info:
            is_src_delta = True
        else:
            is_src_delta = (
                annotated_delta.locator.partition_locator
                != round_completion_info.compacted_delta_locator.partition_locator
            )
        (
            delta_file_envelope_groups,
            total_record_count,
        ) = _group_file_records_by_pk_hash_bucket(
            annotated_delta,
            num_buckets,
            primary_keys,
            sort_key_names,
            is_src_delta,
            read_kwargs_provider,
            deltacat_storage,
        )
        hash_bucket_group_to_obj_id, _ = group_hash_bucket_indices(
            delta_file_envelope_groups, num_buckets, num_groups, object_store
        )

        peak_memory_usage_bytes = get_current_node_peak_memory_usage_in_bytes()
        return HashBucketResult(
            hash_bucket_group_to_obj_id,
            np.int64(total_record_count),
            np.double(peak_memory_usage_bytes),
            np.double(0.0),
            np.double(time.time()),
        )


@ray.remote
def hash_bucket(
    annotated_delta: DeltaAnnotated,
    round_completion_info: Optional[RoundCompletionInfo],
    primary_keys: List[str],
    sort_keys: List[SortKey],
    num_buckets: int,
    num_groups: int,
    enable_profiler: bool,
    metrics_config: MetricsConfig,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    object_store: Optional[IObjectStore],
    deltacat_storage=unimplemented_deltacat_storage,
) -> HashBucketResult:

    logger.info(f"Starting hash bucket task...")
    hash_bucket_result, duration = timed_invocation(
        func=_timed_hash_bucket,
        annotated_delta=annotated_delta,
        round_completion_info=round_completion_info,
        primary_keys=primary_keys,
        sort_keys=sort_keys,
        num_buckets=num_buckets,
        num_groups=num_groups,
        enable_profiler=enable_profiler,
        read_kwargs_provider=read_kwargs_provider,
        object_store=object_store,
        deltacat_storage=deltacat_storage,
    )

    emit_metrics_time = 0.0
    if metrics_config:
        emit_result, latency = timed_invocation(
            func=emit_timer_metrics,
            metrics_name="hash_bucket",
            value=duration,
            metrics_config=metrics_config,
        )
        emit_metrics_time = latency

    logger.info(f"Finished hash bucket task...")
    return HashBucketResult(
        hash_bucket_result[0],
        hash_bucket_result[1],
        hash_bucket_result[2],
        np.double(emit_metrics_time),
        hash_bucket_result[4],
    )
