import logging

from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor_v2.model.merge_file_group import (
    LocalMergeFileGroupsProvider,
)
from deltacat.compute.compactor_v2.model.merge_input import MergeInput
import pyarrow as pa
from deltacat import logs
from typing import List, Optional

from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from deltacat.compute.compactor import (
    RoundCompletionInfo,
    DeltaAnnotated,
)

from deltacat.types.tables import TABLE_CLASS_TO_SIZE_FUNC

from deltacat.utils.performance import timed_invocation
from deltacat.storage import (
    Partition,
)
from deltacat.compute.compactor_v2.deletes.delete_strategy import (
    DeleteStrategy,
)
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def materialize(
    input: MergeInput,
    task_index: int,
    compacted_tables: List[pa.Table],
) -> MaterializeResult:
    compacted_table = pa.concat_tables(compacted_tables)
    if input.compacted_file_content_type in DELIMITED_TEXT_CONTENT_TYPES:
        # TODO (rkenmi): Investigate if we still need to convert this table to pandas DataFrame
        # TODO (pdames): compare performance to pandas-native materialize path
        df = compacted_table.to_pandas(split_blocks=True, self_destruct=True)
        compacted_table = df
    delta, stage_delta_time = timed_invocation(
        input.deltacat_storage.stage_delta,
        compacted_table,
        input.write_to_partition,
        max_records_per_entry=input.max_records_per_output_file,
        content_type=input.compacted_file_content_type,
        s3_table_writer_kwargs=input.s3_table_writer_kwargs,
        **input.deltacat_storage_kwargs,
    )
    compacted_table_size = TABLE_CLASS_TO_SIZE_FUNC[type(compacted_table)](
        compacted_table
    )
    logger.debug(
        f"Time taken for materialize task"
        f" to upload {len(compacted_table)} records"
        f" of size {compacted_table_size} is: {stage_delta_time}s"
    )
    manifest = delta.manifest
    manifest_records = manifest.meta.record_count
    assert manifest_records == len(compacted_table), (
        f"Unexpected Error: Materialized delta manifest record count "
        f"({manifest_records}) does not equal compacted table record count "
        f"({len(compacted_table)})"
    )
    materialize_result = MaterializeResult.of(
        delta=delta,
        task_index=task_index,
        # TODO (pdames): Generalize WriteResult to contain in-memory-table-type
        #  and in-memory-table-bytes instead of tight coupling to paBytes
        pyarrow_write_result=PyArrowWriteResult.of(
            len(manifest.entries),
            TABLE_CLASS_TO_SIZE_FUNC[type(compacted_table)](compacted_table),
            manifest.meta.content_length,
            len(compacted_table),
        ),
    )
    logger.info(f"Materialize result: {materialize_result}")
    return materialize_result


def generate_local_merge_input(
    params: CompactPartitionParams,
    annotated_deltas: List[DeltaAnnotated],
    compacted_partition: Partition,
    round_completion_info: Optional[RoundCompletionInfo],
    delete_strategy: Optional[DeleteStrategy] = None,
    delete_file_envelopes: Optional[DeleteFileEnvelope] = None,
):
    """
    Generates a merge input for local deltas that do not reside in the Ray object store and
    have not been subject to the hash bucketing process.

    Args:
        params: parameters for compacting a partition
        annotated_deltas: a list of annotated deltas
        compacted_partition: the compacted partition to write to
        round_completion_info: keeps track of high watermarks and other metadata from previous compaction rounds

    Returns:
        A MergeInput object

    """
    return MergeInput.of(
        merge_file_groups_provider=LocalMergeFileGroupsProvider(
            annotated_deltas,
            read_kwargs_provider=params.read_kwargs_provider,
            deltacat_storage=params.deltacat_storage,
            deltacat_storage_kwargs=params.deltacat_storage_kwargs,
        ),
        write_to_partition=compacted_partition,
        compacted_file_content_type=params.compacted_file_content_type,
        primary_keys=params.primary_keys,
        sort_keys=params.sort_keys,
        drop_duplicates=params.drop_duplicates,
        max_records_per_output_file=params.records_per_compacted_file,
        enable_profiler=params.enable_profiler,
        metrics_config=params.metrics_config,
        s3_table_writer_kwargs=params.s3_table_writer_kwargs,
        read_kwargs_provider=params.read_kwargs_provider,
        round_completion_info=round_completion_info,
        object_store=params.object_store,
        deltacat_storage=params.deltacat_storage,
        deltacat_storage_kwargs=params.deltacat_storage_kwargs,
        delete_strategy=delete_strategy,
        delete_file_envelopes=delete_file_envelopes,
        disable_copy_by_reference=params.disable_copy_by_reference,
    )
