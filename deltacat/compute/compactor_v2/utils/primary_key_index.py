import logging
from typing import List, Optional, Iterable

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import uuid
import hashlib
from deltacat.compute.compactor_v2.constants import (
    TOTAL_BYTES_IN_SHA1_HASH,
    PK_DELIMITER,
    MAX_SIZE_OF_RECORD_BATCH_IN_GIB,
    SHA1_HASHING_FOR_MEMORY_OPTIMIZATION_DISABLED,
)
import time
from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelope
from deltacat import logs
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.io.object_store import IObjectStore
from deltacat.utils.performance import timed_invocation
from deltacat.utils.pyarrow import sliced_string_cast

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _append_sha1_hash_to_table(table: pa.Table, hash_column: pa.Array) -> pa.Table:
    hash_column_np = hash_column.to_numpy()

    result = []
    for hash_value in hash_column_np:
        if hash_value is None:
            result.append(None)
            logger.info("A primary key hash is null")
        else:
            result.append(hashlib.sha1(hash_value.encode("utf-8")).hexdigest())

    return sc.append_pk_hash_string_column(table, result)


def _is_sha1_desired(hash_columns: List[pa.Array]) -> bool:
    total_size = 0
    total_len = 0

    for hash_column in hash_columns:
        total_size += hash_column.nbytes
        total_len += len(hash_column)

    logger.info(
        f"Found total length of hash column={total_len} and total_size={total_size}"
    )

    if SHA1_HASHING_FOR_MEMORY_OPTIMIZATION_DISABLED:
        logger.info(
            f"SHA1_HASHING_FOR_MEMORY_OPTIMIZATION_DISABLED is True. "
            f"Returning False for is_sha1_desired"
        )
        return False

    return total_size > TOTAL_BYTES_IN_SHA1_HASH * total_len


def _append_table_by_hash_bucket(
    pki_table: pa.Table, hash_bucket_to_table: np.ndarray
) -> int:

    hb_pk_table, sort_latency = timed_invocation(
        lambda: pki_table.sort_by(sc._HASH_BUCKET_IDX_COLUMN_NAME)
    )
    logger.info(f"Sorting a pk table of length {len(pki_table)} took {sort_latency}s")

    hb_pk_grouped_by, groupby_latency = timed_invocation(
        lambda: hb_pk_table.group_by(sc._HASH_BUCKET_IDX_COLUMN_NAME).aggregate(
            [(sc._HASH_BUCKET_IDX_COLUMN_NAME, "count")]
        )
    )

    logger.info(
        f"Grouping a pki table of length {len(pki_table)} took {groupby_latency}s"
    )

    group_count_array = hb_pk_grouped_by[f"{sc._HASH_BUCKET_IDX_COLUMN_NAME}_count"]
    hb_group_array = hb_pk_grouped_by[sc._HASH_BUCKET_IDX_COLUMN_NAME]

    result_len = 0
    for i, group_count in enumerate(group_count_array):
        hb_idx = hb_group_array[i].as_py()
        pyarrow_table = hb_pk_table.slice(offset=result_len, length=group_count.as_py())
        pyarrow_table = pyarrow_table.drop(
            [sc._HASH_BUCKET_IDX_COLUMN_NAME, sc._PK_HASH_STRING_COLUMN_NAME]
        )
        if hash_bucket_to_table[hb_idx] is None:
            hash_bucket_to_table[hb_idx] = []
        hash_bucket_to_table[hb_idx].append(pyarrow_table)
        result_len += len(pyarrow_table)

    return result_len


def _optimized_group_record_batches_by_hash_bucket(
    pki_table: pa.Table, num_buckets: int
):

    input_table_len = len(pki_table)

    hash_bucket_to_tables = np.empty([num_buckets], dtype="object")
    hb_to_table = np.empty([num_buckets], dtype="object")

    # This split will ensure that the sort is not performed on a very huge table
    # resulting in ArrowInvalid: offset overflow while concatenating arrays
    # Known issue with Arrow: https://github.com/apache/arrow/issues/25822
    table_batches, to_batches_latency = timed_invocation(lambda: pki_table.to_batches())

    logger.info(f"to_batches took {to_batches_latency} for {len(pki_table)} rows")

    current_bytes = 0
    record_batches = []
    result_len = 0
    for record_batch in table_batches:
        if (
            record_batches
            and current_bytes + record_batch.nbytes >= MAX_SIZE_OF_RECORD_BATCH_IN_GIB
        ):
            logger.info(
                f"Total number of record batches without exceeding {MAX_SIZE_OF_RECORD_BATCH_IN_GIB} "
                f"is {len(record_batches)} and size {current_bytes}"
            )
            appended_len, append_latency = timed_invocation(
                _append_table_by_hash_bucket,
                pa.Table.from_batches(record_batches),
                hash_bucket_to_tables,
            )
            logger.info(
                f"Appended the hash bucketed batch of {appended_len} in {append_latency}s"
            )

            result_len += appended_len
            current_bytes = 0
            record_batches.clear()

        current_bytes += record_batch.nbytes
        record_batches.append(record_batch)

    if record_batches:
        appended_len, append_latency = timed_invocation(
            _append_table_by_hash_bucket,
            pa.Table.from_batches(record_batches),
            hash_bucket_to_tables,
        )
        result_len += appended_len
        current_bytes = 0
        record_batches.clear()

    concat_start = time.monotonic()
    for hb, tables in enumerate(hash_bucket_to_tables):
        if tables:
            assert hb_to_table[hb] is None, f"The HB index is repeated {hb}"
            hb_to_table[hb] = pa.concat_tables(tables)

    concat_end = time.monotonic()
    logger.info(
        f"Total time taken to concat all record batches with length "
        f"{input_table_len}: {concat_end - concat_start}s"
    )

    assert (
        input_table_len == result_len
    ), f"Grouping has resulted in record loss as {result_len} != {input_table_len}"

    return hb_to_table


def group_by_pk_hash_bucket(
    table: pa.Table, num_buckets: int, primary_keys: List[str]
) -> np.ndarray:
    new_tables = generate_pk_hash_column([table], primary_keys, requires_hash=True)
    assert (
        len(new_tables) == 1
    ), f"Expected only 1 table in the result but found {len(new_tables)}"

    table = new_tables[0]

    # group hash bucket record indices
    result = group_record_indices_by_hash_bucket(
        table,
        num_buckets,
    )

    return result


def generate_pk_hash_column(
    tables: List[pa.Table],
    primary_keys: Optional[List[str]] = None,
    requires_hash: bool = False,
) -> List[pa.Table]:
    """
    Returns a new table list after generating the primary key hash if desired.

    1. If there are no primary keys, each hash will be unique uuid/sha1 hex
    2. If there are more than 0 primary keys, returns a table with pk hash column appended.
    """

    def _generate_pk_hash(table: pa.Table) -> pa.Array:
        pk_columns = []
        for pk_name in primary_keys:
            pk_columns.append(sliced_string_cast(table[pk_name]))

        pk_columns.append(PK_DELIMITER)
        hash_column = pc.binary_join_element_wise(*pk_columns, null_handling="replace")
        return hash_column

    def _generate_uuid(table: pa.Table) -> pa.Array:
        hash_column = pa.array(
            [uuid.uuid4().hex for _ in range(len(table))], pa.string()
        )
        return hash_column

    start = time.monotonic()

    hash_column_list = []

    can_sha1 = False
    if primary_keys:
        hash_column_list = [_generate_pk_hash(table) for table in tables]

        can_sha1 = requires_hash or _is_sha1_desired(hash_column_list)
    else:
        hash_column_list = [_generate_uuid(table) for table in tables]

    logger.info(
        f"can_generate_sha1={can_sha1} for the table and requires_sha1={requires_hash}"
    )

    result = []

    total_len = 0
    total_size = 0
    for index, table in enumerate(tables):
        if can_sha1:
            table = _append_sha1_hash_to_table(table, hash_column_list[index])
        else:
            table = table.append_column(
                sc._PK_HASH_STRING_COLUMN_FIELD, hash_column_list[index]
            )

        total_len += len(table)
        total_size += hash_column_list[index].nbytes

        result.append(table)

    end = time.monotonic()

    logger.info(
        f"Took {end - start}s to generate pk hash of len: {total_len}"
        f" for size: {total_size} bytes"
    )

    return result


def group_record_indices_by_hash_bucket(
    pki_table: pa.Table, num_buckets: int
) -> np.ndarray:
    """
    Groups the record indices by it's corresponding hash bucket. Hence, this method may
    create num_buckets tables as a result.
    """

    input_table_len = len(pki_table)

    hash_bucket_id_col_list = np.empty([input_table_len], dtype="int32")
    bucketing_start_time = time.monotonic()

    for index, hash_value in enumerate(sc.pk_hash_string_column_np(pki_table)):
        hash_bucket = pk_digest_to_hash_bucket_index(hash_value, num_buckets)
        hash_bucket_id_col_list[index] = hash_bucket

    pki_table = sc.append_hash_bucket_idx_col(pki_table, hash_bucket_id_col_list)
    bucketing_end_time = time.monotonic()

    logger.info(
        f"Took {bucketing_end_time - bucketing_start_time}s to generate the "
        f"hb index for {len(pki_table)} rows"
    )

    result, group_latency = timed_invocation(
        _optimized_group_record_batches_by_hash_bucket,
        pki_table=pki_table,
        num_buckets=num_buckets,
    )

    logger.info(
        f"Final grouping of table with {input_table_len} records took: {group_latency}s"
    )

    return result


def group_hash_bucket_indices(
    hash_bucket_object_groups: np.ndarray,
    num_buckets: int,
    num_groups: int,
    object_store: Optional[IObjectStore] = None,
) -> np.ndarray:
    """
    This method persists all tables for a given hash bucket into the object store
    and returns the object references for each hash group.
    """

    hash_bucket_group_to_obj_id_size_tuple = np.empty([num_groups], dtype="object")

    if hash_bucket_object_groups is None:
        return hash_bucket_group_to_obj_id_size_tuple

    hb_group_to_object = np.empty([num_groups], dtype="object")
    hash_group_to_size = np.empty([num_groups], dtype="int64")
    hash_group_to_num_rows = np.empty([num_groups], dtype="int64")

    for hb_index, obj in enumerate(hash_bucket_object_groups):
        if obj:
            hb_group = hash_bucket_index_to_hash_group_index(hb_index, num_groups)
            if hb_group_to_object[hb_group] is None:
                hb_group_to_object[hb_group] = np.empty([num_buckets], dtype="object")
                hash_group_to_size[hb_group] = np.int64(0)
                hash_group_to_num_rows[hb_group] = np.int64(0)
            hb_group_to_object[hb_group][hb_index] = obj
            for dfe in obj:
                casted_dfe: DeltaFileEnvelope = dfe
                hash_group_to_size[hb_group] += casted_dfe.table_size_bytes
                hash_group_to_num_rows[hb_group] += casted_dfe.table_num_rows

    for hb_group, obj in enumerate(hb_group_to_object):
        if obj is None:
            continue
        object_ref = object_store.put(obj)
        hash_bucket_group_to_obj_id_size_tuple[hb_group] = (
            object_ref,
            hash_group_to_size[hb_group],
            hash_group_to_num_rows[hb_group],
        )
        del object_ref

    _, close_latency = timed_invocation(object_store.close)
    logger.info(f"Active connections to the object store closed in {close_latency}")

    return hash_bucket_group_to_obj_id_size_tuple


def hash_bucket_index_to_hash_group_index(hb_index: int, num_groups: int) -> int:
    return hb_index % num_groups


def hash_group_index_to_hash_bucket_indices(
    hb_group: int, num_buckets: int, num_groups: int
) -> Iterable[int]:

    if hb_group > num_buckets:
        return []

    return range(hb_group, num_buckets, num_groups)


def pk_digest_to_hash_bucket_index(digest: Optional[str], num_buckets: int) -> int:
    """
    Generates the hash bucket index from the given digest.
    """
    if digest is None:
        return 0
    return int(digest, 16) % num_buckets
