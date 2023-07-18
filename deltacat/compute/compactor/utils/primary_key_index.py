import json
import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple
import hashlib
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray
import s3fs
import time
from ray.types import ObjectRef

from deltacat import logs
from deltacat.aws import s3u
from deltacat.compute.compactor import (
    PrimaryKeyIndexLocator,
    PrimaryKeyIndexMeta,
    PrimaryKeyIndexVersionLocator,
    PrimaryKeyIndexVersionMeta,
    PyArrowWriteResult,
    RoundCompletionInfo,
)
from deltacat.compute.compactor.steps.rehash import rehash_bucket as rb
from deltacat.compute.compactor.steps.rehash import rewrite_index as ri
from deltacat.compute.compactor.utils import round_completion_file as rcf
from deltacat.compute.compactor.utils import system_columns as sc
from deltacat.constants import PRIMARY_KEY_INDEX_WRITE_BOTO3_CONFIG
from deltacat.storage import Manifest, PartitionLocator
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.types.tables import get_table_slicer, get_table_writer
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.ray_utils.concurrency import invoke_parallel
from deltacat.io.object_store import IObjectStore

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

_PK_BYTES_DELIMITER = "L6kl7u5f"
MAX_SIZE_OF_RECORD_BATCH_IN_GIB = 2 * 1024 * 1024 * 1024


def generate_pk_hash_column(table, primary_keys):
    start = time.monotonic()
    pk_columns = []
    for pk_name in primary_keys:
        pk_columns.append(pc.cast(table[pk_name], pa.string()))

    pk_columns.append(_PK_BYTES_DELIMITER)
    hash_column = pc.binary_join_element_wise(*pk_columns)
    hash_column_np = hash_column.to_numpy()

    # TODO: do this only if pk size is huge.
    # The above takes nearly 6s.
    result = []
    for digest in hash_column_np:
        result.append(hashlib.sha1(digest.encode("utf-8")).hexdigest())

    table = sc.append_pk_hash_column(table, result)
    end = time.monotonic()

    # It took approx 15.982524741999995s to generate pk hash of len: 5824678 and size: 593381554
    # It took approx 121.390761371s to generate pk hash of len: 86245179 and size: 6102873492
    logger.info(
        f"It took approx {end - start}s to generate pk hash of len: {len(hash_column)} and size: {hash_column.nbytes}"
    )

    return table


def rehash(
    options_provider: Callable[[int, Any], Dict[str, Any]],
    s3_bucket: str,
    source_partition_locator: PartitionLocator,
    old_rci: RoundCompletionInfo,
    new_hash_bucket_count: int,
    hash_bucket_index_group_count: int,
    records_per_primary_key_index_file: int,
    delete_old_primary_key_index: bool,
) -> RoundCompletionInfo:

    logger.info(
        f"Rehashing primary key index. Old round completion info: "
        f"{old_rci}. New hash bucket count: {new_hash_bucket_count}"
    )

    # collect old primary key index information
    old_pki_version_locator = old_rci.primary_key_index_version_locator
    old_pkiv_meta = old_pki_version_locator.primary_key_index_version_meta
    old_pki_meta = old_pkiv_meta.primary_key_index_meta
    old_compacted_partition_locator = old_pki_meta.compacted_partition_locator
    if old_pkiv_meta.hash_bucket_count == new_hash_bucket_count:
        raise ValueError(
            f"Primary key index rehash failed. Old hash bucket "
            f"count ({new_hash_bucket_count}) is "
            f"equal to new hash bucket count. Partition: "
            f"{old_compacted_partition_locator}."
        )

    # generate a new unique primary key index version locator to rehash into
    new_pki_meta = PrimaryKeyIndexMeta.of(
        old_compacted_partition_locator,
        old_pki_meta.primary_keys,
        old_pki_meta.sort_keys,
        old_pki_meta.primary_key_index_algorithm_version,
    )
    new_pki_locator = PrimaryKeyIndexLocator.of(new_pki_meta)
    new_pki_version_meta = PrimaryKeyIndexVersionMeta.of(
        new_pki_meta,
        new_hash_bucket_count,
    )
    rehashed_pki_version_locator = PrimaryKeyIndexVersionLocator.generate(
        new_pki_version_meta
    )

    # launch a rehash task for each bucket of the old primary key index version
    old_hash_bucket_count = old_pkiv_meta.hash_bucket_count
    hb_tasks_pending = invoke_parallel(
        items=range(old_hash_bucket_count),
        ray_task=rb.rehash_bucket,
        max_parallelism=None,
        options_provider=options_provider,
        s3_bucket=s3_bucket,
        old_pki_version_locator=old_pki_version_locator,
        num_buckets=new_hash_bucket_count,
        num_groups=hash_bucket_index_group_count,
    )
    logger.info(f"Getting {len(hb_tasks_pending)} rehash bucket results...")
    hb_results = ray.get([t[0] for t in hb_tasks_pending])
    logger.info(f"Got {len(hb_results)} rehash bucket results.")
    all_hash_group_idx_to_obj_id = defaultdict(list)
    for hash_group_idx_to_obj_id in hb_results:
        for hash_group_index, object_id in enumerate(hash_group_idx_to_obj_id):
            if object_id:
                all_hash_group_idx_to_obj_id[hash_group_index].append(object_id)
    hash_group_count = len(all_hash_group_idx_to_obj_id)
    logger.info(f"Rehash bucket groups created: {hash_group_count}")

    # write primary key index files for each rehashed output bucket
    pki_stats_promises = invoke_parallel(
        items=all_hash_group_idx_to_obj_id.values(),
        ray_task=ri.rewrite_index,
        max_parallelism=None,
        options_provider=options_provider,
        s3_bucket=s3_bucket,
        new_primary_key_index_version_locator=rehashed_pki_version_locator,
        max_records_per_index_file=records_per_primary_key_index_file,
    )
    logger.info(f"Getting {len(pki_stats_promises)} rewrite index results...")
    pki_stats = ray.get([t[0] for t in pki_stats_promises])
    logger.info(f"Got {len(pki_stats)} rewrite index results.")

    round_completion_info = RoundCompletionInfo.of(
        old_rci.high_watermark,
        old_rci.compacted_delta_locator,
        old_rci.compacted_pyarrow_write_result,
        PyArrowWriteResult.union(pki_stats),
        old_rci.sort_keys_bit_width,
        rehashed_pki_version_locator,
        old_rci.rebase_source_partition_locator,
    )
    rcf.write_round_completion_file(
        s3_bucket,
        source_partition_locator,
        new_pki_locator.primary_key_index_root_path,
        round_completion_info,
    )
    if delete_old_primary_key_index:
        delete_primary_key_index_version(
            s3_bucket,
            old_pki_version_locator,
        )
    logger.info(
        f"Rehashed primary key index. New round completion info: "
        f"{round_completion_info}."
    )
    return round_completion_info


def download_hash_bucket_entries(
    s3_bucket: str,
    hash_bucket_index: int,
    primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
) -> List[pa.Table]:

    pk_index_manifest_s3_url = (
        primary_key_index_version_locator.get_pkiv_hb_index_manifest_s3_url(
            s3_bucket,
            hash_bucket_index,
        )
    )
    result = s3u.download(pk_index_manifest_s3_url, False)
    logger.info(
        f"Downloading primary key index hash bucket manifest entries: "
        f"{pk_index_manifest_s3_url}. Primary key index version "
        f"locator: {primary_key_index_version_locator}"
    )
    pk_index_manifest = Manifest(json.loads(result["Body"].read().decode("utf-8")))
    tables = s3u.download_manifest_entries(
        pk_index_manifest, file_reader_kwargs_provider=file_reader_kwargs_provider
    )
    if not tables:
        logger.warning(
            f"Primary key index manifest is empty at: "
            f"{pk_index_manifest_s3_url}. Primary key index version "
            f"locator: {primary_key_index_version_locator}"
        )
    return tables


def delete_primary_key_index_version(
    s3_bucket: str, pki_version_locator: PrimaryKeyIndexVersionLocator
) -> None:

    logger.info(f"Deleting primary key index: {pki_version_locator}")
    s3u.delete_files_by_prefix(
        s3_bucket,
        pki_version_locator.primary_key_index_version_root_path,
    )
    logger.info(f"Primary key index deleted: {pki_version_locator}")


def _group_record_batches_by_hash_bucket(pki_table, hash_bucket_to_table):
    sort_start = time.monotonic()

    hb_pk_table = pki_table.sort_by(sc._HASH_BUCKET_IDX_COLUMN_NAME)

    sort_end = time.monotonic()
    # Sort took: 19.621158946999998
    logger.info(f"Sort took: {sort_end - sort_start}")

    hb_pk_grouped_by = hb_pk_table.group_by(sc._HASH_BUCKET_IDX_COLUMN_NAME).aggregate(
        [(sc._HASH_BUCKET_IDX_COLUMN_NAME, "count")]
    )

    group_by_end = time.monotonic()
    # groupby took 0.09588522000001376
    logger.info(f"groupby took {group_by_end - sort_end}")

    group_count_array = hb_pk_grouped_by[f"{sc._HASH_BUCKET_IDX_COLUMN_NAME}_count"]
    hb_group_array = hb_pk_grouped_by[sc._HASH_BUCKET_IDX_COLUMN_NAME]

    result_len = 0
    for i, group_count in enumerate(group_count_array):
        hb_idx = hb_group_array[i].as_py()
        pyarrow_table = hb_pk_table.slice(offset=result_len, length=group_count.as_py())
        pyarrow_table = pyarrow_table.drop([sc._HASH_BUCKET_IDX_COLUMN_NAME])
        if hash_bucket_to_table[hb_idx] is None:
            hash_bucket_to_table[hb_idx] = []
        hash_bucket_to_table[hb_idx].append(pyarrow_table)
        result_len += len(pyarrow_table)

    return result_len


def group_record_indices_by_hash_bucket(pki_table: pa.Table, num_buckets: int) -> Any:

    input_table_len = len(pki_table)
    hash_bucket_to_tables = np.empty([num_buckets], dtype="object")
    hb_to_table = np.empty([num_buckets], dtype="object")
    hash_bucket_id_col_list = []
    bucket_id_start = time.monotonic()
    for digest in sc.pk_hash_column_np(pki_table):
        hash_bucket = pk_digest_to_hash_bucket_index(digest, num_buckets)
        hash_bucket_id_col_list.append(hash_bucket)

    pki_table = sc.append_hash_bucket_idx_col(pki_table, hash_bucket_id_col_list)
    bucket_id_end = time.monotonic()

    # Took 4.721033879999993s to generate the hb index for 5817260 rows
    logger.info(
        f"Took {bucket_id_end - bucket_id_start}s to generate the hb index for {len(pki_table)} rows"
    )

    # This split will ensure that the sort is not performed on a very huge table
    # resulting in ArrowInvalid: offset overflow while concatenating arrays
    # Known issue with Arrow: https://github.com/apache/arrow/issues/25822
    start = time.monotonic()
    table_batches = pki_table.to_batches()
    end = time.monotonic()

    logger.info(f"toBatches took {end - start} for {len(pki_table)} rows")

    current_bytes = 0
    record_batches = []
    result_len = 0
    for rbatch in table_batches:
        current_bytes += rbatch.nbytes
        record_batches.append(rbatch)
        if current_bytes >= MAX_SIZE_OF_RECORD_BATCH_IN_GIB:
            # Total number of record batches without exceeding 2147483648 is 78 and size 2160901323
            logger.info(
                f"Total number of record batches without exceeding {MAX_SIZE_OF_RECORD_BATCH_IN_GIB} is {len(record_batches)} and size {current_bytes}"
            )
            result_len += _group_record_batches_by_hash_bucket(
                pa.Table.from_batches(record_batches), hash_bucket_to_tables
            )
            current_bytes = 0
            record_batches.clear()

    if record_batches:
        result_len += _group_record_batches_by_hash_bucket(
            pa.Table.from_batches(record_batches), hash_bucket_to_tables
        )
        current_bytes = 0
        record_batches.clear()

    concat_start = time.monotonic()
    for hb, tables in enumerate(hash_bucket_to_tables):
        if tables:
            assert hb_to_table[hb] is None, f"The HB index is repeated {hb}"
            hb_to_table[hb] = pa.concat_tables(tables)

    concat_end = time.monotonic()
    logger.info(
        f"Total time it took to concat all record batches: {concat_end - concat_start}"
    )

    assert (
        input_table_len == result_len
    ), f"Grouping has resulted in record loss as {result_len} != {input_table_len}"

    bucketing_end = time.monotonic()

    # Final bucketing took: 0.007040638999995963 and total records: 5817260
    # Final bucketing took: 140.91121000199996 and total records: 86245179
    logger.info(
        f"Final bucketing took: {bucketing_end - start} and total records: {input_table_len}"
    )

    return hb_to_table


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
    return int(digest, 16) % num_buckets


def write_primary_key_index_files(
    table: pa.Table,
    primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
    s3_bucket: str,
    hb_index: int,
    records_per_index_file: int,
) -> PyArrowWriteResult:
    """
    Writes primary key index files for the given hash bucket index out to the
    specified S3 bucket at the path identified by the given primary key index
    version locator. Output is written as 1 or more Parquet files with the
    given maximum number of records per file.

    TODO(raghumdani): Support writing primary key index to any data catalog
    """
    logger.info(
        f"Writing primary key index files for hash bucket {hb_index}. "
        f"Primary key index version locator: "
        f"{primary_key_index_version_locator}."
    )
    s3_file_system = s3fs.S3FileSystem(
        anon=False,
        s3_additional_kwargs={
            "ContentType": ContentType.PARQUET.value,
            "ContentEncoding": ContentEncoding.IDENTITY.value,
        },
        config_kwargs=PRIMARY_KEY_INDEX_WRITE_BOTO3_CONFIG,
    )
    pkiv_hb_index_s3_url_base = (
        primary_key_index_version_locator.get_pkiv_hb_index_s3_url_base(
            s3_bucket, hb_index
        )
    )
    manifest_entries = s3u.upload_sliced_table(
        table,
        pkiv_hb_index_s3_url_base,
        s3_file_system,
        records_per_index_file,
        get_table_writer(table),
        get_table_slicer(table),
    )
    manifest = Manifest.of(manifest_entries)
    pkiv_hb_index_s3_manifest_s3_url = (
        primary_key_index_version_locator.get_pkiv_hb_index_manifest_s3_url(
            s3_bucket, hb_index
        )
    )
    s3u.upload(pkiv_hb_index_s3_manifest_s3_url, str(json.dumps(manifest)))
    result = PyArrowWriteResult.of(
        len(manifest_entries),
        table.nbytes,
        manifest.meta.content_length,
        len(table),
    )
    logger.info(
        f"Wrote primary key index files for hash bucket {hb_index}. "
        f"Primary key index version locator: "
        f"{primary_key_index_version_locator}. Result: {result}"
    )
    return result
