import logging
from typing import List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import uuid
import hashlib
from deltacat.compute.compactor_v2.constants import (
    TOTAL_BYTES_IN_SHA1_HASH,
    PK_DELIMITER,
)
from ray.types import ObjectRef
import time

from deltacat import logs
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.compute.compactor.utils.primary_key_index import (
    group_hash_bucket_indices as group_hash_bucket_indices_v1,
)
from deltacat.io.object_store import IObjectStore

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _append_sha1_hash_to_table(table: pa.Table, hash_column: pa.Array) -> pa.Table:
    hash_column_np = hash_column.to_numpy()

    result = []
    for digest in hash_column_np:
        result.append(hashlib.sha1(digest.encode("utf-8")).hexdigest())

    return sc.append_pk_hash_string_column(table, result)


def _is_sha1_desired(hash_column: pa.Array) -> bool:
    return hash_column.nbytes > TOTAL_BYTES_IN_SHA1_HASH * len(hash_column)


def group_by_pk_hash_bucket(
    table: pa.Table, num_buckets: int, primary_keys: List[str]
) -> np.ndarray:
    table = generate_pk_hash_column(table, primary_keys, requires_sha1=True)

    # group hash bucket record indices
    result = group_record_indices_by_hash_bucket(
        table,
        num_buckets,
    )

    return result


def generate_pk_hash_column(
    table: pa.Table,
    primary_keys: Optional[List[str]] = None,
    requires_sha1: bool = False,
) -> pa.Table:
    """
    Returns a new table after generating the primary key hash if desired.

    1. If there are no primary keys, each hash will be unique uuid/sha1 hex
    2. If there are more than 0 primary keys, returns a table with new columns appended.
    """

    start = time.monotonic()

    can_sha1 = False
    if primary_keys:
        pk_columns = []
        for pk_name in primary_keys:
            pk_columns.append(pc.cast(table[pk_name], pa.string()))

        pk_columns.append(PK_DELIMITER)
        hash_column = pc.binary_join_element_wise(*pk_columns)

        can_sha1 = requires_sha1 or _is_sha1_desired(hash_column)
    else:
        hash_column = pa.array(
            [uuid.uuid4().hex for _ in range(len(table))], pa.string()
        )

    logger.info(
        f"can_generate_sha1={can_sha1} for the table with hash column size"
        f"={hash_column.nbytes} bytes, num_rows={len(hash_column)}, "
        f"and requires_sha1={requires_sha1}"
    )

    if can_sha1:
        table = _append_sha1_hash_to_table(table, hash_column)
    else:
        table = table.append_column(sc._PK_HASH_STRING_COLUMN_FIELD, hash_column)

    end = time.monotonic()

    logger.info(
        f"Took {end - start}s to generate pk hash of len: {len(hash_column)}"
        f" and size: {hash_column.nbytes} bytes"
    )

    return table


def group_record_indices_by_hash_bucket(
    pki_table: pa.Table, num_buckets: int
) -> np.ndarray:
    """
    Groups the record indices by it's corresponding hash bucket. Hence, this method may
    create num_buckets tables as a result.
    """

    input_table_len = len(pki_table)

    hash_bucket_to_table = np.empty([num_buckets], dtype="object")
    hash_bucket_id_col_list = []
    bucketing_start_time = time.monotonic()
    for digest in sc.pk_hash_string_column_np(pki_table):
        hash_bucket = pk_digest_to_hash_bucket_index(digest, num_buckets)
        hash_bucket_id_col_list.append(hash_bucket)

    pki_table = sc.append_hash_bucket_idx_col(pki_table, hash_bucket_id_col_list)
    bucketing_end_time = time.monotonic()

    logger.info(
        f"Took {bucketing_end_time - bucketing_start_time}s to generate the "
        f"hb index for {len(pki_table)} rows"
    )

    sort_start_time = time.monotonic()

    hb_pk_table = pki_table.sort_by(sc._HASH_BUCKET_IDX_COLUMN_NAME)

    sort_end_time = time.monotonic()
    logger.info(
        f"Sorted the table with {len(hb_pk_table)} rows in {sort_end_time - sort_start_time}s"
    )

    hb_pk_grouped_by = hb_pk_table.group_by(sc._HASH_BUCKET_IDX_COLUMN_NAME).aggregate(
        [(sc._HASH_BUCKET_IDX_COLUMN_NAME, "count")]
    )

    group_by_end_time = time.monotonic()
    group_count_array = hb_pk_grouped_by[f"{sc._HASH_BUCKET_IDX_COLUMN_NAME}_count"]
    logger.info(
        f"Created {len(group_count_array)} groups by hash bucket index "
        f"in {group_by_end_time - sort_end_time}s"
    )

    hb_group_array = hb_pk_grouped_by[sc._HASH_BUCKET_IDX_COLUMN_NAME]

    result_len = 0
    for i, group_count in enumerate(group_count_array):
        hb_idx = hb_group_array[i].as_py()
        pyarrow_table = hb_pk_table.slice(offset=result_len, length=group_count.as_py())
        pyarrow_table = pyarrow_table.drop([sc._HASH_BUCKET_IDX_COLUMN_NAME])
        assert (
            hash_bucket_to_table[hb_idx] is None
        ), f"Hash bucket ID {hb_idx} already processed"
        hash_bucket_to_table[hb_idx] = pyarrow_table
        result_len += len(pyarrow_table)

    assert (
        input_table_len == result_len
    ), f"Grouping has resulted in record loss as {result_len} != {input_table_len}"

    bucketing_end_time = time.monotonic()

    logger.info(
        f"Final bucketing took: {bucketing_end_time - group_by_end_time}"
        f" and total records: {len(hb_pk_table)}"
    )

    return hash_bucket_to_table


def group_hash_bucket_indices(
    hash_bucket_object_groups: np.ndarray,
    num_buckets: int,
    num_groups: int,
    object_store: Optional[IObjectStore] = None,
) -> Tuple[np.ndarray, List[ObjectRef]]:
    """
    This method persists all tables for a given hash bucket into the object store
    and returns the object references for each hash group.
    """

    return group_hash_bucket_indices_v1(
        hash_bucket_object_groups, num_buckets, num_groups, object_store
    )


def pk_digest_to_hash_bucket_index(digest: str, num_buckets: int) -> int:
    """
    Generates the hash bucket index from the given digest.
    """
    return int(digest, 16) % num_buckets
