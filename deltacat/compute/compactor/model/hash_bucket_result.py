from typing import NamedTuple

import numpy as np


class HashBucketResult(NamedTuple):
    hash_bucket_group_to_obj_id: np.ndarray
    hb_record_count: np.int64
