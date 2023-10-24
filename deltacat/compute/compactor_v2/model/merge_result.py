from typing import NamedTuple, List
from deltacat.compute.compactor.model.materialize_result import MaterializeResult

import numpy as np


class MergeResult(NamedTuple):
    materialize_results: List[MaterializeResult]
    deduped_record_count: np.int64
    peak_memory_usage_bytes: np.double
    task_completed_at: np.double
