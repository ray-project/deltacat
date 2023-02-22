import logging
from typing import List, Tuple

import numpy as np
import pyarrow as pa
import ray
from ray.types import ObjectRef

from deltacat import logs
from deltacat.compute.compactor import PrimaryKeyIndexVersionLocator
from deltacat.compute.compactor.utils import primary_key_index as pki

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def group_file_records_by_pk_hash_bucket(
    pki_table: pa.Table, num_buckets: int
) -> np.ndarray:
    # generate the new table for each new hash bucket
    hash_bucket_to_indices = pki.group_record_indices_by_hash_bucket(
        pki_table,
        num_buckets,
    )
    hash_bucket_to_table = np.empty([num_buckets], dtype="object")
    for hash_bucket, indices in enumerate(hash_bucket_to_indices):
        if indices:
            hash_bucket_to_table[hash_bucket] = pki_table.take(indices)
    return hash_bucket_to_table


@ray.remote(num_cpus=1, num_returns=2)
def rehash_bucket(
    hash_bucket_index: int,
    s3_bucket: str,
    old_pki_version_locator: PrimaryKeyIndexVersionLocator,
    num_buckets: int,
    num_groups: int,
) -> Tuple[np.ndarray, List[ObjectRef]]:

    logger.info(f"Starting rehash bucket task...")
    tables = pki.download_hash_bucket_entries(
        s3_bucket,
        hash_bucket_index,
        old_pki_version_locator,
    )
    prior_pk_index_table = pa.concat_tables(tables)
    hash_bucket_to_table = group_file_records_by_pk_hash_bucket(
        prior_pk_index_table,
        num_buckets,
    )
    hash_bucket_group_to_obj_id, object_refs = pki.group_hash_bucket_indices(
        hash_bucket_to_table,
        num_buckets,
        num_groups,
    )
    logger.info(f"Finished rehash bucket task...")
    return hash_bucket_group_to_obj_id, object_refs
