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
