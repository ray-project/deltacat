import logging
import functools
from deltacat.storage import (
    PartitionLocator,
    Delta,
    interface as unimplemented_deltacat_storage,
)
from deltacat import logs
from deltacat.compute.compactor.utils import io as io_v1
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from typing import Dict, List, Optional, Any
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.compute.compactor_v2.utils.task_options import (
    estimate_manifest_entry_size_bytes,
)
from deltacat.compute.compactor_v2.utils.content_type_params import (
    append_content_type_params,
)
from deltacat.utils.metrics import metrics
from deltacat.compute.compactor_v2.constants import DISCOVER_DELTAS_METRIC_PREFIX
from deltacat.compute.resource_estimation.manifest import (
    does_require_content_type_params,
)
from deltacat.compute.resource_estimation.model import OperationType

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@metrics(prefix=DISCOVER_DELTAS_METRIC_PREFIX)
def discover_deltas(
    source_partition_locator: PartitionLocator,
    last_stream_position_to_compact: int,
    rebase_source_partition_locator: Optional[PartitionLocator] = None,
    rebase_source_partition_high_watermark: Optional[int] = None,
    rcf_high_watermark: Optional[int] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = {},
    list_deltas_kwargs: Optional[Dict[str, Any]] = {},
) -> List[Delta]:

    previous_compacted_high_watermark = (
        rebase_source_partition_high_watermark or rcf_high_watermark
    )

    delta_source_partition_locator = (
        rebase_source_partition_locator or source_partition_locator
    )

    result = []

    delta_source_incremental_deltas = io_v1._discover_deltas(
        delta_source_partition_locator,
        previous_compacted_high_watermark,
        last_stream_position_to_compact,
        deltacat_storage,
        deltacat_storage_kwargs,
        list_deltas_kwargs,
    )

    result.extend(delta_source_incremental_deltas)
    logger.info(
        f"Length of input deltas from delta source table is {len(delta_source_incremental_deltas)}"
        f" from ({previous_compacted_high_watermark}, {last_stream_position_to_compact}]"
    )

    if rebase_source_partition_locator:
        previous_compacted_deltas = io_v1._discover_deltas(
            source_partition_locator,
            None,
            None,
            deltacat_storage,
            deltacat_storage_kwargs,
            list_deltas_kwargs,
        )

        result.extend(previous_compacted_deltas)

        logger.info(
            f"Length of input deltas from previous compacted table is {len(previous_compacted_deltas)}"
            f" from ({None}, {None}]"
        )

    return result


def create_uniform_input_deltas(
    input_deltas: List[Delta],
    hash_bucket_count: int,
    compaction_audit: CompactionSessionAuditInfo,
    compact_partition_params: CompactPartitionParams,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = {},
) -> List[DeltaAnnotated]:

    delta_bytes = 0
    delta_manifest_entries_count = 0
    estimated_da_bytes = 0
    input_da_list = []
    for delta in input_deltas:
        if (
            compact_partition_params.enable_input_split
            or does_require_content_type_params(
                compact_partition_params.resource_estimation_method
            )
        ):
            logger.debug(
                f"Delta with locator: {delta.locator} requires content type params..."
            )
            append_content_type_params(
                delta=delta,
                deltacat_storage=deltacat_storage,
                deltacat_storage_kwargs=deltacat_storage_kwargs,
                task_max_parallelism=compact_partition_params.task_max_parallelism,
                max_parquet_meta_size_bytes=compact_partition_params.max_parquet_meta_size_bytes,
                file_reader_kwargs_provider=compact_partition_params.read_kwargs_provider,
            )

        manifest_entries = delta.manifest.entries
        delta_manifest_entries_count += len(manifest_entries)

        for entry_index in range(len(manifest_entries)):
            entry = manifest_entries[entry_index]
            delta_bytes += entry.meta.content_length
            estimated_da_bytes += estimate_manifest_entry_size_bytes(
                entry=entry,
                operation_type=OperationType.PYARROW_DOWNLOAD,
                estimate_resources_params=compact_partition_params.estimate_resources_params,
            )

        delta_annotated = DeltaAnnotated.of(delta)
        input_da_list.append(delta_annotated)

    logger.info(f"Input deltas to compact this round: " f"{len(input_da_list)}")
    logger.info(f"Input delta bytes to compact: {delta_bytes}")
    logger.info(f"Input delta files to compact: {delta_manifest_entries_count}")

    size_estimation_function = functools.partial(
        estimate_manifest_entry_size_bytes,
        operation_type=OperationType.PYARROW_DOWNLOAD,
        estimate_resources_params=compact_partition_params.estimate_resources_params,
    )
    rebatched_da_list = DeltaAnnotated.rebatch(
        input_da_list,
        min_delta_bytes=compact_partition_params.min_delta_bytes_in_batch,
        min_file_counts=compact_partition_params.min_files_in_batch,
        estimation_function=size_estimation_function,
        enable_input_split=compact_partition_params.enable_input_split,
    )

    compaction_audit.set_input_size_bytes(delta_bytes)
    compaction_audit.set_input_file_count(delta_manifest_entries_count)
    compaction_audit.set_estimated_in_memory_size_bytes_during_discovery(
        estimated_da_bytes
    )

    logger.info(f"Hash bucket count: {hash_bucket_count}")
    logger.info(f"Input uniform delta count: {len(rebatched_da_list)}")

    return rebatched_da_list
