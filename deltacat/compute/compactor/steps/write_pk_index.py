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
def write_pk_index(all_hb_idx_to_tables_refs: List[Any],
                    compaction_artifact_s3_bucket: str,
                    new_primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
                    max_records_per_index_file: int,
                    mat_index_to_file_id_offset: defaultdict,
                    dedupe_task_index: Optional[int] = None):

    logger.info(f"Starting pk index write task for {len(all_hb_idx_to_tables_refs)}...")
    all_hb_idx_to_tables = ray.get([cloudpickle.loads(t) for t in all_hb_idx_to_tables_refs])
    logger.info(f"Got all the remote objects.")

    all_hb_idx_to_tables_map = defaultdict(list)
    for hb_idx_to_tables in all_hb_idx_to_tables:
        all_hb_idx_to_tables_map[hb_idx_to_tables[0]].append(hb_idx_to_tables[1])

    # TODO(raghumdani): replace the file_ids with offsets.
    pki_results = []
    for hb_index, table_list in all_hb_idx_to_tables_map.items():
        concatenated_tables = pa.concat_tables(table_list)
        mat_bucket_indexes = sc.mat_bucket_index_column_np(concatenated_tables)
        logger.info(f"[Write PK task {dedupe_task_index}] building file id offsets for {len(mat_bucket_indexes)}...")
        file_id_offsets = []
        for mat_bucket in mat_bucket_indexes:
            file_id_offsets.append(mat_index_to_file_id_offset[mat_bucket.item()])

        # Building file id offsets takes about 6 seconds. 
        logger.info(f"[Write PK task {dedupe_task_index}] file ids offsets of length {len(file_id_offsets)} created.")

        concatenated_tables = concatenated_tables.drop([sc._MAT_BUCKET_IDX_COLUMN_NAME])
        new_file_ids = pc.add_checked(concatenated_tables[sc._ORDERED_FILE_IDX_COLUMN_NAME], 
                                        pa.array(file_id_offsets, sc._ORDERED_FILE_IDX_COLUMN_TYPE))

        # Just 500ms for add_checked. 
        logger.info(f"[Write PK task {dedupe_task_index}] New file ids created of len {len(new_file_ids)}")

        concatenated_tables = concatenated_tables.drop([sc._ORDERED_FILE_IDX_COLUMN_NAME])
        concatenated_tables = concatenated_tables.append_column(sc._ORDERED_FILE_IDX_COLUMN_FIELD, new_file_ids)
        # Dropping and appending columns are instantaneous. 
        logger.info(f"[Write PK task {dedupe_task_index}] appended the file idx columns.")

        # Takes about 10seconds. 
        hb_pki_result = pki.write_primary_key_index_files(
            concatenated_tables,
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
