import botocore
import logging
import tenacity
from typing import Dict, Optional, List, Tuple, Any
from deltacat import logs
from deltacat.compute.compactor_v2.model.merge_file_group import (
    LocalMergeFileGroupsProvider,
)
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.types.partial_download import PartialParquetParameters
from deltacat.storage import (
    Manifest,
    ManifestEntry,
    interface as unimplemented_deltacat_storage,
)
from deltacat.compute.compactor.model.delta_annotated import DeltaAnnotated
from deltacat.compute.compactor.model.round_completion_info import RoundCompletionInfo
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    hash_group_index_to_hash_bucket_indices,
)
from deltacat.compute.compactor_v2.constants import (
    PARQUET_TO_PYARROW_INFLATION,
)
from daft.exceptions import DaftTransientError


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _get_parquet_type_params_if_exist(
    entry: ManifestEntry,
) -> Optional[PartialParquetParameters]:
    if (
        entry.meta
        and entry.meta.content_type == ContentType.PARQUET
        and entry.meta.content_encoding == ContentEncoding.IDENTITY
        and entry.meta.content_type_parameters
    ):
        for type_params in entry.meta.content_type_parameters:
            if isinstance(type_params, PartialParquetParameters):
                return type_params
    return None


def _calculate_parquet_column_size(
    type_params: PartialParquetParameters, columns: List[str]
):
    column_size = 0.0
    for rg in type_params.row_groups_to_download:
        columns_found = 0
        row_group_meta = type_params.pq_metadata.row_group(rg)
        for col in range(row_group_meta.num_columns):
            column_meta = row_group_meta.column(col)
            if column_meta.path_in_schema in columns:
                columns_found += 1
                column_size += column_meta.total_uncompressed_size
        assert columns_found == len(columns), (
            "Columns not found in the parquet data as "
            f"{columns_found} != {len(columns)}"
        )
    return column_size * PARQUET_TO_PYARROW_INFLATION


def get_task_options(
    cpu: float, memory: float, ray_custom_resources: Optional[Dict] = None
) -> Dict:

    # NOTE: With DEFAULT scheduling strategy in Ray 2.20.0, autoscaler does
    # not spin up enough nodes fast and hence we see only approximately
    # 20 tasks get scheduled out of 100 tasks in queue. Hence, we use SPREAD
    # which is also ideal for merge and hash bucket tasks.
    # https://docs.ray.io/en/latest/ray-core/scheduling/index.html
    task_opts = {"num_cpus": cpu, "memory": memory, "scheduling_strategy": "SPREAD"}

    if ray_custom_resources:
        task_opts["resources"] = ray_custom_resources

    task_opts["max_retries"] = 3

    # List of possible botocore exceptions are available at
    # https://github.com/boto/botocore/blob/develop/botocore/exceptions.py
    task_opts["retry_exceptions"] = [
        botocore.exceptions.ConnectionError,
        botocore.exceptions.HTTPClientError,
        ConnectionError,
        TimeoutError,
        DaftTransientError,
        tenacity.RetryError,
    ]

    return task_opts


def estimate_manifest_entry_size_bytes(
    entry: ManifestEntry, previous_inflation: float, **kwargs
) -> float:
    if entry.meta.source_content_length:
        return entry.meta.source_content_length

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if type_params:
        return type_params.in_memory_size_bytes * PARQUET_TO_PYARROW_INFLATION

    return entry.meta.content_length * previous_inflation


def estimate_manifest_entry_num_rows(
    entry: ManifestEntry,
    average_record_size_bytes: float,
    previous_inflation: float,
    **kwargs,
) -> int:
    if entry.meta.record_count:
        return entry.meta.record_count

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if type_params:
        return type_params.num_rows

    total_size_bytes = estimate_manifest_entry_size_bytes(
        entry=entry, previous_inflation=previous_inflation, **kwargs
    )

    return int(total_size_bytes / average_record_size_bytes)


def estimate_manifest_entry_column_size_bytes(
    entry: ManifestEntry, columns: Optional[List[str]] = None
) -> Optional[float]:
    if not columns:
        return 0

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if type_params and type_params.pq_metadata:
        return _calculate_parquet_column_size(type_params=type_params, columns=columns)

    return None


def hash_bucket_resource_options_provider(
    index: int,
    item: DeltaAnnotated,
    previous_inflation: float,
    average_record_size_bytes: float,
    total_memory_buffer_percentage: int,
    primary_keys: List[str] = None,
    ray_custom_resources: Optional[Dict] = None,
    memory_logs_enabled: Optional[bool] = None,
    **kwargs,
) -> Dict:
    debug_memory_params = {"hash_bucket_task_index": index}
    size_bytes = 0.0
    num_rows = 0
    total_pk_size = 0

    if not item.manifest or not item.manifest.entries:
        logger.debug(
            f"[Hash bucket task {index}]: No manifest entries, skipping memory allocation calculation"
        )
        return {"CPU": 0.01}

    for entry in item.manifest.entries:
        entry_size = estimate_manifest_entry_size_bytes(
            entry=entry, previous_inflation=previous_inflation
        )
        num_rows += estimate_manifest_entry_num_rows(
            entry=entry,
            previous_inflation=previous_inflation,
            average_record_size_bytes=average_record_size_bytes,
        )
        size_bytes += entry_size

        if primary_keys:
            pk_size = estimate_manifest_entry_column_size_bytes(
                entry=entry,
                columns=primary_keys,
            )

            if pk_size is None:
                total_pk_size += entry_size
            else:
                total_pk_size += pk_size

    # total size + pk size + pyarrow-to-numpy conversion + pk hash column + hashlib inefficiency + hash bucket index column
    # Refer to hash_bucket step for more details.
    total_memory = (
        size_bytes
        + total_pk_size
        + total_pk_size
        + num_rows * 20
        + num_rows * 20
        + num_rows * 4
    )
    debug_memory_params["size_bytes"] = size_bytes
    debug_memory_params["num_rows"] = num_rows
    debug_memory_params["total_pk_size"] = total_pk_size
    debug_memory_params["total_memory"] = total_memory

    debug_memory_params["previous_inflation"] = previous_inflation
    debug_memory_params["average_record_size_bytes"] = average_record_size_bytes

    # Consider buffer
    total_memory = total_memory * (1 + total_memory_buffer_percentage / 100.0)
    debug_memory_params["total_memory_with_buffer"] = total_memory
    logger.debug_conditional(
        f"[Hash bucket task {index}]: Params used for calculating hash bucketing memory: {debug_memory_params}",
        memory_logs_enabled,
    )

    return get_task_options(0.01, total_memory, ray_custom_resources)


def merge_resource_options_provider(
    index: int,
    item: Tuple[int, List],
    num_hash_groups: int,
    hash_group_size_bytes: Dict[int, int],
    hash_group_num_rows: Dict[int, int],
    total_memory_buffer_percentage: int,
    round_completion_info: Optional[RoundCompletionInfo] = None,
    compacted_delta_manifest: Optional[Manifest] = None,
    ray_custom_resources: Optional[Dict] = None,
    primary_keys: Optional[List[str]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict] = {},
    memory_logs_enabled: Optional[bool] = None,
    **kwargs,
) -> Dict:
    debug_memory_params = {"merge_task_index": index}
    hb_group_idx = item[0]

    data_size = hash_group_size_bytes.get(hb_group_idx, 0)
    num_rows = hash_group_num_rows.get(hb_group_idx, 0)
    debug_memory_params["data_size_from_hash_group"] = data_size
    debug_memory_params["num_rows_from_hash_group"] = num_rows

    # upper bound for pk size of incremental
    pk_size_bytes = data_size
    incremental_index_array_size = num_rows * 4

    return get_merge_task_options(
        index,
        hb_group_idx,
        data_size,
        pk_size_bytes,
        num_rows,
        num_hash_groups,
        total_memory_buffer_percentage,
        incremental_index_array_size,
        debug_memory_params,
        ray_custom_resources,
        round_completion_info=round_completion_info,
        compacted_delta_manifest=compacted_delta_manifest,
        primary_keys=primary_keys,
        deltacat_storage=deltacat_storage,
        deltacat_storage_kwargs=deltacat_storage_kwargs,
        memory_logs_enabled=memory_logs_enabled,
    )


def local_merge_resource_options_provider(
    estimated_da_size: float,
    estimated_num_rows: int,
    total_memory_buffer_percentage: int,
    round_completion_info: Optional[RoundCompletionInfo] = None,
    compacted_delta_manifest: Optional[Manifest] = None,
    ray_custom_resources: Optional[Dict] = None,
    primary_keys: Optional[List[str]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict] = {},
    memory_logs_enabled: Optional[bool] = None,
    **kwargs,
) -> Dict:
    index = hb_group_idx = LocalMergeFileGroupsProvider.LOCAL_HASH_BUCKET_INDEX
    debug_memory_params = {"merge_task_index": index}

    # upper bound for pk size of incremental
    pk_size_bytes = estimated_da_size
    incremental_index_array_size = estimated_num_rows * 4

    return get_merge_task_options(
        index=index,
        hb_group_idx=hb_group_idx,
        data_size=estimated_da_size,
        pk_size_bytes=pk_size_bytes,
        num_rows=estimated_num_rows,
        num_hash_groups=1,
        incremental_index_array_size=incremental_index_array_size,
        total_memory_buffer_percentage=total_memory_buffer_percentage,
        debug_memory_params=debug_memory_params,
        ray_custom_resources=ray_custom_resources,
        round_completion_info=round_completion_info,
        compacted_delta_manifest=compacted_delta_manifest,
        primary_keys=primary_keys,
        deltacat_storage=deltacat_storage,
        deltacat_storage_kwargs=deltacat_storage_kwargs,
        memory_logs_enabled=memory_logs_enabled,
    )


def get_merge_task_options(
    index: int,
    hb_group_idx: int,
    data_size: float,
    pk_size_bytes: float,
    num_rows: int,
    num_hash_groups: int,
    total_memory_buffer_percentage: int,
    incremental_index_array_size: int,
    debug_memory_params: Dict[str, Any],
    ray_custom_resources: Optional[Dict],
    round_completion_info: Optional[RoundCompletionInfo] = None,
    compacted_delta_manifest: Optional[Manifest] = None,
    primary_keys: Optional[List[str]] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict] = {},
    memory_logs_enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    if (
        round_completion_info
        and compacted_delta_manifest
        and round_completion_info.hb_index_to_entry_range
    ):

        previous_inflation = (
            round_completion_info.compacted_pyarrow_write_result.pyarrow_bytes
            / round_completion_info.compacted_pyarrow_write_result.file_bytes
        )
        debug_memory_params["previous_inflation"] = previous_inflation

        average_record_size = (
            round_completion_info.compacted_pyarrow_write_result.pyarrow_bytes
            / round_completion_info.compacted_pyarrow_write_result.records
        )
        debug_memory_params["average_record_size"] = average_record_size

        iterable = hash_group_index_to_hash_bucket_indices(
            hb_group_idx, round_completion_info.hash_bucket_count, num_hash_groups
        )

        for hb_idx in iterable:
            if round_completion_info.hb_index_to_entry_range.get(str(hb_idx)) is None:
                continue

            entry_start, entry_end = round_completion_info.hb_index_to_entry_range[
                str(hb_idx)
            ]
            for entry_index in range(entry_start, entry_end):
                entry = compacted_delta_manifest.entries[entry_index]

                current_entry_size = estimate_manifest_entry_size_bytes(
                    entry=entry, previous_inflation=previous_inflation
                )
                current_entry_rows = estimate_manifest_entry_num_rows(
                    entry=entry,
                    average_record_size_bytes=average_record_size,
                    previous_inflation=previous_inflation,
                )

                data_size += current_entry_size
                num_rows += current_entry_rows

                if primary_keys:
                    pk_size = estimate_manifest_entry_column_size_bytes(
                        entry=entry,
                        columns=primary_keys,
                    )

                    if pk_size is None:
                        pk_size_bytes += current_entry_size
                    else:
                        pk_size_bytes += pk_size

    # total data downloaded + primary key hash column + pyarrow-to-numpy conversion
    # + primary key column + hashlib inefficiency + dict size for merge + incremental index array size
    total_memory = (
        data_size
        + pk_size_bytes
        + pk_size_bytes
        + num_rows * 20
        + num_rows * 20
        + num_rows * 20
        + incremental_index_array_size
    )
    debug_memory_params["data_size"] = data_size
    debug_memory_params["num_rows"] = num_rows
    debug_memory_params["pk_size_bytes"] = pk_size_bytes
    debug_memory_params["incremental_index_array_size"] = incremental_index_array_size
    debug_memory_params["total_memory"] = total_memory

    total_memory = total_memory * (1 + total_memory_buffer_percentage / 100.0)
    debug_memory_params["total_memory_with_buffer"] = total_memory
    logger.debug_conditional(
        f"[Merge task {index}]: Params used for calculating merge memory: {debug_memory_params}",
        memory_logs_enabled,
    )

    return get_task_options(0.01, total_memory, ray_custom_resources)
