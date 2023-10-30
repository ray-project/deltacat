from typing import NamedTuple

import numpy as np


class HashBucketResult(NamedTuple):
    hash_bucket_group_to_obj_id: np.ndarray
    hb_record_count: np.int64
    peak_memory_usage_bytes: np.double
    telemetry_time_in_seconds: np.double
    task_completed_at: np.double
