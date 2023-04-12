from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass
class DedupeResult:
    mat_bucket_idx_to_obj_id: Dict[int, Tuple]
    deduped_record_count: int
