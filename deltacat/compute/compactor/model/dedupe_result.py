from typing import Dict, Tuple, NamedTuple

import numpy as np


class DedupeResult(NamedTuple):
    mat_bucket_idx_to_obj_id: Dict[int, Tuple]
    deduped_record_count: np.int64
