import json
import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import ray
import s3fs
from ray import cloudpickle
from deltacat.constants import PRIMARY_KEY_INDEX_WRITE_BOTO3_CONFIG
from ray.types import ObjectRef

from deltacat.storage import Manifest, PartitionLocator
from deltacat.utils.ray_utils.concurrency import invoke_parallel
from deltacat.compute.compactor import PyArrowWriteResult, \
    RoundCompletionInfo, PrimaryKeyIndexMeta, PrimaryKeyIndexLocator, \
    PrimaryKeyIndexVersionMeta, PrimaryKeyIndexVersionLocator
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
from deltacat.storage import Manifest, PartitionLocator
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.types.tables import get_table_slicer, get_table_writer
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def rehash(
        options_provider: Callable[[int, Any], Dict[str, Any]],
        s3_bucket: str,
        source_partition_locator: PartitionLocator,
        old_rci: RoundCompletionInfo,
        new_hash_bucket_count: int,
        hash_bucket_index_group_count: int,
        records_per_primary_key_index_file: int,
        delete_old_primary_key_index: bool) -> RoundCompletionInfo:

    logger.info(f"Rehashing primary key index. Old round completion info: "
                f"{old_rci}. New hash bucket count: {new_hash_bucket_count}")

    # collect old primary key index information
    old_pki_version_locator = old_rci.primary_key_index_version_locator
    old_pkiv_meta = old_pki_version_locator.primary_key_index_version_meta
    old_pki_meta = old_pkiv_meta.primary_key_index_meta
    old_compacted_partition_locator = old_pki_meta.compacted_partition_locator
    if old_pkiv_meta.hash_bucket_count == new_hash_bucket_count:
        raise ValueError(f"Primary key index rehash failed. Old hash bucket "
                         f"count ({new_hash_bucket_count}) is "
                         f"equal to new hash bucket count. Partition: "
                         f"{old_compacted_partition_locator}.")

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
        new_pki_version_meta)

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
    logger.info(f"Rehashed primary key index. New round completion info: "
                f"{round_completion_info}.")
    return round_completion_info


def download_hash_bucket_entries(
        s3_bucket: str,
        hash_bucket_index: int,
        primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
        file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None) \
        -> List[pa.Table]:

    pk_index_manifest_s3_url = primary_key_index_version_locator\
        .get_pkiv_hb_index_manifest_s3_url(
            s3_bucket,
            hash_bucket_index,
        )
    result = s3u.download(pk_index_manifest_s3_url, False)
    logger.info(f"Downloading primary key index hash bucket manifest entries: "
                f"{pk_index_manifest_s3_url}. Primary key index version "
                f"locator: {primary_key_index_version_locator}")
    pk_index_manifest = Manifest(json.loads(result["Body"].read().decode("utf-8")))
    tables = s3u.download_manifest_entries(pk_index_manifest,
                                           file_reader_kwargs_provider=file_reader_kwargs_provider)
    if not tables:
        logger.warning(
            f"Primary key index manifest is empty at: "
            f"{pk_index_manifest_s3_url}. Primary key index version "
            f"locator: {primary_key_index_version_locator}")
    return tables


def delete_primary_key_index_version(
        s3_bucket: str,
        pki_version_locator: PrimaryKeyIndexVersionLocator) -> None:

    logger.info(f"Deleting primary key index: {pki_version_locator}")
    s3u.delete_files_by_prefix(
        s3_bucket,
        pki_version_locator.primary_key_index_version_root_path,
    )
    logger.info(f"Primary key index deleted: {pki_version_locator}")


def group_record_indices_by_hash_bucket(
        pki_table: pa.Table,
        num_buckets: int) -> np.ndarray:

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
        num_groups: int) -> Tuple[np.ndarray, List[ObjectRef]]:
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
                hb_group_to_object[hb_group] = np.empty(
                    [num_buckets], dtype="object")
            hb_group_to_object[hb_group][hb_index] = obj

    for hb_group, obj in enumerate(hb_group_to_object):
        if obj is None:
            continue
        obj_ref = ray.put(obj)
        pickled_obj_ref = cloudpickle.dumps(obj_ref)
        object_refs.append(pickled_obj_ref)
        hash_bucket_group_to_obj_id[hb_group] = pickled_obj_ref
        # NOTE: The cloudpickle.dumps API call creates an out of band object reference to the object_ref variable. 
        # After pickling, Ray cannot track the serialized copy of the object or determine when the ObjectRef has been deserialized 
        # (e.g., if the ObjectRef is deserialized by a non-Ray process). 
        # Thus the object_ref cannot be tracked by Ray's distributed reference counter, even if it goes out of scope. 
        # The object now has a permanent reference and the data can't be freed from Ray’s object store. 
        # Manually deleting the untrackable object references offsets these permanent references and 
        # helps to allow these objects to be garbage collected normally. 
        del obj_ref
        del pickled_obj_ref
    return hash_bucket_group_to_obj_id, object_refs


def pk_digest_to_hash_bucket_index(
        digest,
        num_buckets: int) -> int:
    """
    Deterministically get the hash bucket a particular digest belongs to
    based on number of total hash buckets.
    """

    return int.from_bytes(digest, "big") % num_buckets


def write_primary_key_index_files(
        table: pa.Table,
        primary_key_index_version_locator: PrimaryKeyIndexVersionLocator,
        s3_bucket: str,
        hb_index: int,
        records_per_index_file: int) -> PyArrowWriteResult:
    """
    Writes primary key index files for the given hash bucket index out to the
    specified S3 bucket at the path identified by the given primary key index
    version locator. Output is written as 1 or more Parquet files with the
    given maximum number of records per file.

    TODO(raghumdani): Support writing primary key index to any data catalog
    """
    logger.info(f"Writing primary key index files for hash bucket {hb_index}. "
                f"Primary key index version locator: "
                f"{primary_key_index_version_locator}.")
    s3_file_system = s3fs.S3FileSystem(
        anon=False,
        s3_additional_kwargs={
            "ContentType": ContentType.PARQUET.value,
            "ContentEncoding": ContentEncoding.IDENTITY.value,
        },
        config_kwargs=PRIMARY_KEY_INDEX_WRITE_BOTO3_CONFIG
    )
    pkiv_hb_index_s3_url_base = primary_key_index_version_locator\
        .get_pkiv_hb_index_s3_url_base(s3_bucket, hb_index)
    manifest_entries = s3u.upload_sliced_table(
        table,
        pkiv_hb_index_s3_url_base,
        s3_file_system,
        records_per_index_file,
        get_table_writer(table),
        get_table_slicer(table),
    )
    manifest = Manifest.of(manifest_entries)
    pkiv_hb_index_s3_manifest_s3_url = primary_key_index_version_locator\
        .get_pkiv_hb_index_manifest_s3_url(s3_bucket, hb_index)
    s3u.upload(
        pkiv_hb_index_s3_manifest_s3_url,
        str(json.dumps(manifest))
    )
    result = PyArrowWriteResult.of(
        len(manifest_entries),
        table.nbytes,
        manifest.meta.content_length,
        len(table),
    )
    logger.info(f"Wrote primary key index files for hash bucket {hb_index}. "
                f"Primary key index version locator: "
                f"{primary_key_index_version_locator}. Result: {result}")
    return result
