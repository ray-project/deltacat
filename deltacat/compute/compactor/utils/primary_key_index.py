import logging
from typing import List, Optional, Tuple

import numpy as np
import pyarrow as pa
from ray.types import ObjectRef

from deltacat import logs
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.io.object_store import IObjectStore

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

# TODO: Deprecate this module in the favor of compactor_v2


def group_record_indices_by_hash_bucket(
    pki_table: pa.Table, num_buckets: int
) -> np.ndarray:

    hash_bucket_to_indices = np.empty([num_buckets], dtype="object")
    record_index = 0
    for digest in sc.pk_hash_column_np(pki_table):
        hash_bucket = pk_digest_to_hash_bucket_index(digest, num_buckets)
        if hash_bucket_to_indices[hash_bucket] is None:
            hash_bucket_to_indices[hash_bucket] = []
        hash_bucket_to_indices[hash_bucket].append(record_index)
        record_index += 1
    return hash_bucket_to_indices


def group_hash_bucket_indices(
    hash_bucket_object_groups: np.ndarray,
    num_buckets: int,
    num_groups: int,
    object_store: Optional[IObjectStore] = None,
) -> Tuple[np.ndarray, List[ObjectRef]]:
    """
    Groups all the ObjectRef that belongs to a particular hash bucket group and hash bucket index.
    """

    object_refs = []
    hash_bucket_group_to_obj_id = np.empty([num_groups], dtype="object")

    if hash_bucket_object_groups is None:
        return hash_bucket_group_to_obj_id, object_refs

    hb_group_to_object = np.empty([num_groups], dtype="object")
    for hb_index, obj in enumerate(hash_bucket_object_groups):
        if obj:
            hb_group = hb_index % num_groups
            if hb_group_to_object[hb_group] is None:
                hb_group_to_object[hb_group] = np.empty([num_buckets], dtype="object")
            hb_group_to_object[hb_group][hb_index] = obj

    for hb_group, obj in enumerate(hb_group_to_object):
        if obj is None:
            continue
        object_ref = object_store.put(obj)
        object_refs.append(object_ref)
        hash_bucket_group_to_obj_id[hb_group] = object_ref
        del object_ref
    return hash_bucket_group_to_obj_id, object_refs


def pk_digest_to_hash_bucket_index(digest, num_buckets: int) -> int:
    """
    Deterministically get the hash bucket a particular digest belongs to
    based on number of total hash buckets.
    """

    return int.from_bytes(digest, "big") % num_buckets
