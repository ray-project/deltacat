from typing import NamedTuple, List
from deltacat.compute.compactor import (
    MaterializeResult,
)

import numpy as np


class DedupeResult(NamedTuple):
    materialize_results: List[MaterializeResult]
    deduped_record_count: np.int64
    peak_memory_usage_bytes: np.double
    telemetry_time_in_seconds: np.double
    task_completed_at: np.double
