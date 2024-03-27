from typing import NamedTuple, List
from deltacat.compute.compactor.model.materialize_result import MaterializeResult

import numpy as np


class MergeResult(NamedTuple):
    materialize_results: List[MaterializeResult]
    input_record_count: np.int64
    deduped_record_count: np.int64
    deleted_record_count: np.int64
    peak_memory_usage_bytes: np.double
    telemetry_time_in_seconds: np.double
    task_completed_at: np.double
