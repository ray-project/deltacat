from typing import Dict, Tuple, NamedTuple

import numpy as np
from deltacat.compute.compactor import PyArrowWriteResult


class DedupeResult(NamedTuple):
    mat_bucket_idx_to_obj_id: Dict[int, Tuple]
    write_pki_result: PyArrowWriteResult
    deduped_record_count: np.int64
    get_pki_time: float
    collective_op_time: float
    write_pki_time: float
