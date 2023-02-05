import logging
import pyarrow as pa
import ray
import time
from ray import cloudpickle
import pyarrow.compute as pc
import numpy as np
from deltacat.compute.compactor.utils.system_columns import get_minimal_hb_schema
from deltacat.utils.pyarrow import ReadKwargsProviderPyArrowSchemaOverride

from deltacat import logs
from collections import defaultdict
from itertools import repeat
from deltacat.storage import DeltaType
from deltacat.compute.compactor import SortKey, SortOrder, \
    RoundCompletionInfo, PrimaryKeyIndexVersionLocator, DeltaFileEnvelope, \
    DeltaFileLocator, PyArrowWriteResult
from deltacat.compute.compactor.utils import system_columns as sc, \
    primary_key_index as pki
from deltacat.utils.performance import timed_invocation

from typing import Any, Dict, List, Optional, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

@ray.remote
def write_pk_index(deduped_tables_ref: Any,
                    dest_file_indices_ref: Any,
                    dest_file_row_indices_ref: Any,
                    compaction_artifact_s3_bucket: str,
                    new_primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
                    max_records_per_index_file: int,
                    max_rows_per_mat_file: int,
                    dedupe_task_index: Optional[int] = None):

    logger.info(f"Starting pk index write task for dedupe result of {dedupe_task_index}...")

    deduped_tables = ray.get(cloudpickle.loads(deduped_tables_ref))
    dest_file_indices = ray.get(cloudpickle.loads(dest_file_indices_ref))
    dest_file_row_indices = ray.get(cloudpickle.loads(dest_file_row_indices_ref))

    logger.info(f"Got all the remote objects.")

    pki_results = []
    src_dfl_row_counts = defaultdict(int)
    for hb_index, table in deduped_tables:
        is_source_col = sc.is_source_column_np(table)
        stream_pos_col = sc.stream_position_column_np(table)
        file_idx_col = sc.file_index_column_np(table)
        dest_file_idx_col = []
        dest_file_row_idx_col = []
        for row_idx in range(len(table)):
            src_dfl = DeltaFileLocator.of(
                is_source_col[row_idx],
                stream_pos_col[row_idx],
                file_idx_col[row_idx],
            )
            dest_file_start_idx = dest_file_indices[src_dfl].item()
            dest_file_row_idx_offset = src_dfl_row_counts[src_dfl] + dest_file_row_indices[src_dfl].item()
            dest_file_idx_offset = int(
                dest_file_row_idx_offset / max_rows_per_mat_file
            )
            dest_file_idx = dest_file_start_idx + dest_file_idx_offset
            dest_file_idx_col.append(dest_file_idx)
            dest_file_row_idx = dest_file_row_idx_offset % max_rows_per_mat_file
            dest_file_row_idx_col.append(dest_file_row_idx)
            src_dfl_row_counts[src_dfl] += 1

        table = table.drop([
            sc._IS_SOURCE_COLUMN_NAME,
            sc._PARTITION_STREAM_POSITION_COLUMN_NAME,
            sc._ORDERED_FILE_IDX_COLUMN_NAME
        ])

        table = sc.append_file_idx_column(table, dest_file_idx_col)
        table = sc.append_record_idx_col(table, dest_file_row_idx_col)

        hb_pki_result = pki.write_primary_key_index_files(
            table,
            new_primary_key_index_version_locator,
            compaction_artifact_s3_bucket,
            hb_index,
            max_records_per_index_file,
        )
        pki_results.append(hb_pki_result)

    result = PyArrowWriteResult.union(pki_results)
    logger.info(f"Wrote new deduped primary key index: "
                f"{new_primary_key_index_version_locator}. Result: {result}")

    logger.info(f"[Dedupe task index {dedupe_task_index}] Finished writing primary key index.")

    return result
