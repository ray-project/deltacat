from dataclasses import dataclass

import numpy as np


@dataclass
class HashBucketResult:
    hash_bucket_group_to_obj_id: np.ndarray
    hb_record_count: int
