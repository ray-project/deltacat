import importlib
import logging
from typing import Any
from deltacat.compute.compactor_v2.model.hash_bucket_input import HashBucketInput
import numpy as np
import pyarrow as pa
import ray
from typing import Tuple, List
from deltacat import logs
from deltacat.storage import (
    Delta,
    DeltaLocator,
    DeltaType,
    DistributedDataset,
    LifecycleState,
    ListResult,
    LocalDataset,
    LocalTable,
    Manifest,
    ManifestAuthor,
    Namespace,
    Partition,
    SchemaConsistencyType,
    Stream,
    StreamLocator,
    Table,
    TableVersion,
    SortKey,
    PartitionLocator,
)
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
import pyarrow.compute as pc
from deltacat.storage import (
    DeltaType,
)
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
from deltacat.compute.compactor_v2.model.prepare_delete_input import PrepareDeleteInput
from deltacat.compute.compactor_v2.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor_v2.utils.primary_key_index import (
    group_hash_bucket_indices,
    group_by_pk_hash_bucket,
)
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import StorageType
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
    get_current_ray_worker_id,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.performance import timed_invocation
from deltacat.utils.metrics import emit_timer_metrics
from deltacat.utils.resources import (
    get_current_process_peak_memory_usage_in_bytes,
    ProcessUtilizationOverTimeRange,
)
from deltacat.constants import BYTES_PER_GIBIBYTE

if importlib.util.find_spec("memray"):
    import memray

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

"""
annotated_delta: DeltaAnnotated,
primary_keys: List[str],
num_hash_buckets: int,
num_hash_groups: int,
enable_profiler: Optional[bool] = False,
metrics_config: Optional[MetricsConfig] = None,
read_kwargs_provider: Optional[ReadKwargsProvider] = None,
object_store: Optional[IObjectStore] = None,
deltacat_storage=unimplemented_deltacat_storage,
deltacat_storage_kwargs: Optional[Dict[str, Any]] = None,
"""


def append_spos_col(table: pa.Table, delta_stream_position: int) -> pa.Table:
    table = table.append_column(
        "spos", pa.array(np.repeat(delta_stream_position, len(table)))
    )
    return table


def drop_earlier_duplicates(table: pa.Table, on: str, sort_col_name: str) -> pa.Table:
    """
    It is important to not combine the chunks for performance reasons.
    """
    logger.info(f"pdebug: drop_earlier_duplicates: {dict(locals())}")
    if not (on in table.column_names or sort_col_name in table.column_names):
        return table

    selector = table.group_by([on]).aggregate([(sort_col_name, "max")])

    table = table.filter(
        pc.is_in(
            table[sort_col_name],
            value_set=selector[f"{sort_col_name}_max"],
        )
    )
    logger.info(f"pdebug: drop_earlier_duplicates: {dict(locals())}")
    return table


def prepare_delete(input: PrepareDeleteInput) -> Tuple[Any, List[str]]:
    """
    go through every file
        build table of primary keys and delete columns

    put into object store

    go through annotated delta
    go through compacted table
    """
    delete_delta_spos_list = []
    upserts_in_interval = []
    deletes_in_interval = []
    # spos_of_deltas_affected_by_deletes_and_delete_bundle = []
    round_completion_info = input.round_completion_info
    all_upserts_affected_by_deletes_by_spos = []
    for idx, annotated_delta in enumerate(input.annotated_deltas):
        annotations = annotated_delta.annotations
        delta_stream_position = annotations[0].annotation_stream_position
        delta_type = annotations[0].annotation_delta_type
        tables = input.deltacat_storage.download_delta(
                annotated_delta,
                max_parallelism=1,
                file_reader_kwargs_provider=input.read_kwargs_provider,
                columns=input.delete_columns,
                storage_type=StorageType.LOCAL,
            **input.deltacat_storage_kwargs,
            )
        if delta_type is DeltaType.UPSERT:
            for idx, table in enumerate(tables):
                tables[idx] = append_spos_col(table, delta_stream_position)
            upserts_in_interval.extend(tables)
        elif delta_type is DeltaType.DELETE:
            for idx, table in enumerate(tables):
                tables[idx] = append_spos_col(table, delta_stream_position)
            delete_delta_spos_list.append(delta_stream_position)
            deletes_in_interval.extend(tables)
    if not upserts_in_interval:
        return
    if not deletes_in_interval:
        return
    upsert_concat_table: pa.Table = pa.concat_tables(upserts_in_interval)
    delete_concat_table: pa.Table = pa.concat_tables(deletes_in_interval)
    logger.info(
        f"pdebug:before_filter_out_deletes {upsert_concat_table.to_pydict()=} \n\n {delete_concat_table.to_pydict()=}"
    )
    # filter out deletes
    upsert_concat_table = upsert_concat_table.filter(
        pc.is_in(
            upsert_concat_table[input.delete_columns[0]],
            value_set=delete_concat_table[input.delete_columns[0]],
            skip_nulls=True,
        )
    )
    logger.info(
        f"pdebug:after_filter_out_deletes {upsert_concat_table.to_pydict()=} \n\n {delete_concat_table.to_pydict()=}"
    )
    all_upserts_affected_by_deletes_by_spos.append(upsert_concat_table)
    all_upserts_affected_by_deletes_by_spos.append(delete_concat_table)
    # if round_completion_info:
    #     all_compacted_table = []
    #     compacted_delta_locator = round_completion_info.compacted_delta_locator
    #     compacted_table_stream_pos = (
    #         round_completion_info.compacted_delta_locator.stream_position
    #     )
    #     previous_compacted_delta_manifest = input.deltacat_storage.get_delta_manifest(
    #         compacted_delta_locator,
    #         **input.deltacat_storage_kwargs,
    #     )
    #     logger.info(f"pdebug: compacted_stream_pos: {compacted_table_stream_pos=}")
    #     for file_idx, _ in enumerate(previous_compacted_delta_manifest.entries):
    #         compacted_table = input.deltacat_storage.download_delta_manifest_entry(
    #             compacted_delta_locator,
    #             entry_index=file_idx,
    #             columns=input.delete_columns,
    #             file_reader_kwargs_provider=input.read_kwargs_provider,
    #             **input.deltacat_storage_kwargs,
    #         )
    #         all_compacted_table.append(compacted_table)
    #     compacted_concat_table: pa.Table = pa.concat_tables(all_compacted_table)
    #     compacted_concat_table = append_spos_col(
    #         compacted_concat_table, compacted_table_stream_pos
    #     )
    #     logger.info(
    #         f"pdebug: {upsert_concat_table.to_pydict()=} {delete_concat_table.to_pydict()=}, {compacted_concat_table.to_pydict()=}"
    #     )
    #     compacted_concat_table = compacted_concat_table.filter(
    #         pc.is_in(
    #             compacted_concat_table[input.delete_columns[0]],
    #             value_set=delete_concat_table[input.delete_columns[0]],
    #             skip_nulls=True,
    #         )
    #     )
    #     all_upserts_affected_by_deletes_by_spos.append(compacted_concat_table)
    spos_of_deltas_affected_by_deletes_and_delete_bundle = pa.concat_tables(all_upserts_affected_by_deletes_by_spos)
    logger.info(f"pdebug: {spos_of_deltas_affected_by_deletes_and_delete_bundle=}")
    obj_ref = ray.put(spos_of_deltas_affected_by_deletes_and_delete_bundle)
    return obj_ref, delete_delta_spos_list


def prepare_delete2(input: PrepareDeleteInput) -> Tuple[Any, List[str]]:
    logger.info(f"pdebug: prepare_delete: {dict(locals())}")
    delete_delta_spos = []
    all_deletes = []
    for i, annotated_delta in enumerate(input.annotated_deltas):
        annotations = annotated_delta.annotations
        delta_stream_position = annotations[0].annotation_stream_position
        delta_type = annotations[0].annotation_delta_type
        logger.info(
            f"pdebug:prepare_delete:{i=}: {annotations=} {delta_stream_position=}, {delta_type.value=}"
        )
        delete_tables = input.deltacat_storage.download_delta(
            annotated_delta,
            max_parallelism=1,
            file_reader_kwargs_provider=input.read_kwargs_provider,
            columns=input.delete_columns,
            storage_type=StorageType.LOCAL,
            **input.deltacat_storage_kwargs,
        )
        logger.info(f"pdebug: {i}. delete_incremental table -> {delete_tables=}")
        for i, table in enumerate(delete_tables):
            delete_tables[i] = append_spos_col(table, delta_stream_position)
        delete_delta_spos.append(delta_stream_position)
        all_deletes.extend(delete_tables)
    all_deletes = pa.concat_tables(all_deletes)
    obj_ref = ray.put(all_deletes)
    return obj_ref, delete_delta_spos
    # compacted_table = pa.concat_tables(table_acc)
    # annotated_delta = input.annotated_deltas
    # logger.info(f"pdebug: prepare_delete: {input.annotated_deltas=}, {type(input.annotated_deltas)=}")
    # annotations = input.annotated_deltas.annotations
    # delta_stream_position = annotations[0].annotation_stream_position
    # delta_type = annotations[0].annotation_delta_type
    # if delta_type != DeltaType.DELETE:
    #     logger.info("pdebug: noop - didn't find a delete:")
    # else:
    #     delete_columns = ["col_1"]
    #     all_tables = []
    #     incremental_delete_table = deltacat_storage.download_delta(
    #         annotated_delta,
    #         max_parallelism=1,
    #         file_reader_kwargs_provider=read_kwargs_provider,
    #         columns=delete_columns,
    #         storage_type=StorageType.LOCAL,
    #         **deltacat_storage_kwargs,
    #     )

    #     logger.info(f"pdebug: incremental table -> {incremental_delete_table=}, {round_completion_info=}")
    #     logger.info(f"pdebug:{annotations=}")
    #     logger.info(f"pdebug: {compacted_table=}, {type(compacted_table)=}, {type(inc_table)=}, {compacted_table['col_1']=}, {inc_table=}")
    #     delete_table = compacted_table.filter(
    #         pc.is_in(
    #             compacted_table['col_1'],
    #             value_set=inc_table['col_1'],
    #             skip_nulls=True
    #         )
    #     )
    #     delete_table = delete_table.append_column("spos",pa.array(np.repeat(delta_stream_position, len(delete_table))))
    #     logger.info(f"pdebug:{delete_table=}")
