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
    if not (on in table.column_names or sort_col_name in table.column_names):
        return table

    selector = table.group_by([on]).aggregate([(sort_col_name, "max")])

    table = table.filter(
        pc.is_in(
            table[sort_col_name],
            value_set=selector[f"{sort_col_name}_max"],
        )
    )
    return table


def prepare_delete(input: PrepareDeleteInput) -> Tuple[Any, List[str]]:
    delete_delta_spos_list = []
    upserts_in_interval = []
    deletes_in_interval = []
    all_upserts_affected_by_deletes_by_spos = []
    for idx, annotated_delta in enumerate(input.annotated_deltas):
        annotations = annotated_delta.annotations
        delta_stream_position = annotations[0].annotation_stream_position
        delta_type = annotations[0].annotation_delta_type
        delta_tables = input.deltacat_storage.download_delta(
            annotated_delta,
            max_parallelism=1,
            file_reader_kwargs_provider=input.read_kwargs_provider,
            columns=input.delete_columns,
            storage_type=StorageType.LOCAL,
            **input.deltacat_storage_kwargs,
        )
        if delta_type is DeltaType.UPSERT:
            for idx, table in enumerate(delta_tables):
                delta_tables[idx] = append_spos_col(table, delta_stream_position)
            upserts_in_interval.extend(delta_tables)
        elif delta_type is DeltaType.DELETE:
            for idx, table in enumerate(delta_tables):
                delta_tables[idx] = append_spos_col(table, delta_stream_position)
            delete_delta_spos_list.append(delta_stream_position)
            deletes_in_interval.extend(delta_tables)
    if not upserts_in_interval:
        return
    if not deletes_in_interval:
        return
    upserts_affected_by_deletes: pa.Table = pa.concat_tables(upserts_in_interval)
    deletes_affected_by_deletes: pa.Table = pa.concat_tables(deletes_in_interval)
    # logger.info(
    #     f"pdebug:before_filter_out_deletes {upserts_affected_by_deletes.to_pydict()=} \n\n {deletes_affected_by_deletes.to_pydict()=}"
    # )
    # filter out deletes
    upserts_affected_by_deletes = upserts_affected_by_deletes.filter(
        pc.is_in(
            upserts_affected_by_deletes[input.delete_columns[0]],
            value_set=deletes_affected_by_deletes[input.delete_columns[0]],
            skip_nulls=True,
        )
    )
    # logger.info(
    #     f"pdebug:after_filter_out_deletes {upserts_affected_by_deletes.to_pydict()=} \n\n {deletes_affected_by_deletes.to_pydict()=}"
    # )
    all_upserts_affected_by_deletes_by_spos.append(upserts_affected_by_deletes)
    all_upserts_affected_by_deletes_by_spos.append(deletes_affected_by_deletes)
    spos_table_of_all_deltas_affected_by_deletes = pa.concat_tables(
        all_upserts_affected_by_deletes_by_spos
    )
    for idx, annotated_delta in enumerate(input.annotated_deltas):
        annotations = annotated_delta.annotations
        delta_stream_position = annotations[0].annotation_stream_position
        delta_type = annotations[0].annotation_delta_type
        delta_tables = input.deltacat_storage.download_delta(
            annotated_delta,
            max_parallelism=1,
            file_reader_kwargs_provider=input.read_kwargs_provider,
            columns=input.delete_columns,
            storage_type=StorageType.LOCAL,
            **input.deltacat_storage_kwargs,
        )
        # logger.info(
        #     f"pdebug:prepare_delete:{idx=}, {delta_type=}, {delta_stream_position=} {spos_table_of_all_deltas_affected_by_deletes.to_pydict()=}, {delta_tables=}"
        # )
    obj_ref = ray.put(spos_table_of_all_deltas_affected_by_deletes)
    return obj_ref, delete_delta_spos_list


def prepare_delete3(input: PrepareDeleteInput) -> Any:
    pass
    # delete_delta_spos_list = []
    # upserts_in_interval = []
    # deletes_in_interval = []
    # all_upserts_affected_by_deletes_by_spos = []
    # for idx, annotated_delta in enumerate(input.annotated_deltas):
    #     annotations = annotated_delta.annotations
    #     delta_stream_position = annotations[0].annotation_stream_position
    #     delta_type = annotations[0].annotation_delta_type
    #     delta_tables = input.deltacat_storage.download_delta(
    #         annotated_delta,
    #         max_parallelism=1,
    #         file_reader_kwargs_provider=input.read_kwargs_provider,
    #         columns=input.delete_columns,
    #         storage_type=StorageType.LOCAL,
    #         **input.deltacat_storage_kwargs,
    #     )
    #     if delta_type is DeltaType.UPSERT:
    #         for idx, table in enumerate(delta_tables):
    #             delta_tables[idx] = append_spos_col(table, delta_stream_position)
    #         upserts_in_interval.extend(delta_tables)
    #     elif delta_type is DeltaType.DELETE:
    #         for idx, table in enumerate(delta_tables):
    #             delta_tables[idx] = append_spos_col(table, delta_stream_position)
    #         delete_delta_spos_list.append(delta_stream_position)
    #         deletes_in_interval.extend(delta_tables)
    # if not upserts_in_interval:
    #     return
    # if not deletes_in_interval:
    #     return
    # upserts_affected_by_deletes: pa.Table = pa.concat_tables(upserts_in_interval)
    # deletes_affected_by_deletes: pa.Table = pa.concat_tables(deletes_in_interval)
    # logger.info(
    #     f"pdebug:before_filter_out_deletes {upserts_affected_by_deletes.to_pydict()=} \n\n {deletes_affected_by_deletes.to_pydict()=}"
    # )
    # # filter out deletes
    # upserts_affected_by_deletes = upserts_affected_by_deletes.filter(
    #     pc.is_in(
    #         upserts_affected_by_deletes[input.delete_columns[0]],
    #         value_set=deletes_affected_by_deletes[input.delete_columns[0]],
    #         skip_nulls=True,
    #     )
    # )
    # logger.info(
    #     f"pdebug:after_filter_out_deletes {upserts_affected_by_deletes.to_pydict()=} \n\n {deletes_affected_by_deletes.to_pydict()=}"
    # )
    # all_upserts_affected_by_deletes_by_spos.append(upserts_affected_by_deletes)
    # all_upserts_affected_by_deletes_by_spos.append(deletes_affected_by_deletes)
    # spos_table_of_all_deltas_affected_by_deletes = pa.concat_tables(
    #     all_upserts_affected_by_deletes_by_spos
    # )
    # for idx, annotated_delta in enumerate(input.annotated_deltas):
    #     annotations = annotated_delta.annotations
    #     delta_stream_position = annotations[0].annotation_stream_position
    #     delta_type = annotations[0].annotation_delta_type
    #     delta_tables = input.deltacat_storage.download_delta(
    #         annotated_delta,
    #         max_parallelism=1,
    #         file_reader_kwargs_provider=input.read_kwargs_provider,
    #         columns=input.delete_columns,
    #         storage_type=StorageType.LOCAL,
    #         **input.deltacat_storage_kwargs,
    #     )
    #     logger.info(
    #         f"pdebug:prepare_delete:{idx=}, {delta_type=}, {delta_stream_position=} {spos_table_of_all_deltas_affected_by_deletes.to_pydict()=}, {delta_tables=}"
    #     )
    # obj_ref = ray.put(spos_table_of_all_deltas_affected_by_deletes)
    # return obj_ref, delete_delta_spos_list
