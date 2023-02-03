import ray
import pyarrow as pa
import numpy as np
import logging

from deltacat.compute.compactor.model.delta_file_envelope import DeltaFileEnvelopeGroups
from itertools import chain

from deltacat import logs
from deltacat.compute.compactor import DeltaAnnotated, DeltaFileEnvelope, \
    SortKey
from deltacat.compute.compactor.utils.primary_key_index import \
    group_hash_bucket_indices, group_record_indices_by_hash_bucket
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import StorageType
from deltacat.utils.common import sha1_digest
from deltacat.compute.compactor.utils import system_columns as sc

from typing import List, Optional, Generator, Tuple

from ray.types import ObjectRef

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

_PK_BYTES_DELIMITER = b'L6kl7u5f'

HashBucketGroupToObjectId = np.ndarray
HashBucketResult = Tuple[HashBucketGroupToObjectId, List[ObjectRef[DeltaFileEnvelopeGroups]]]


def _group_by_pk_hash_bucket(
        table: pa.Table,
        num_buckets: int,
        primary_keys: List[str]) -> np.ndarray:

    # generate the primary key digest column
    all_pk_column_fields = []
    for pk_name in primary_keys:
        # casting a primary key column to numpy also ensures no nulls exist
        column_fields = table[pk_name].to_numpy()
        all_pk_column_fields.append(column_fields)
    hash_column_generator = _hash_pk_bytes_generator(all_pk_column_fields)
    table = sc.append_pk_hash_column(table, hash_column_generator)

    # drop primary key columns to free up memory
    table = table.drop(primary_keys)

    # group hash bucket record indices
    hash_bucket_to_indices = group_record_indices_by_hash_bucket(
        table,
        num_buckets,
    )

    # generate the ordered record number column
    hash_bucket_to_table = np.empty([num_buckets], dtype="object")
    for hb, indices in enumerate(hash_bucket_to_indices):
        if indices:
            hash_bucket_to_table[hb] = sc.append_record_idx_col(
                table.take(indices),
                indices,
            )
    return hash_bucket_to_table


def _hash_pk_bytes_generator(all_column_fields) -> Generator[bytes, None, None]:
    for field_index in range(len(all_column_fields[0])):
        bytes_to_join = []
        for column_fields in all_column_fields:
            bytes_to_join.append(
                bytes(str(column_fields[field_index]), "utf-8")
            )
        yield sha1_digest(_PK_BYTES_DELIMITER.join(bytes_to_join))


def _group_file_records_by_pk_hash_bucket(
        annotated_delta: DeltaAnnotated,
        num_hash_buckets: int,
        primary_keys: List[str],
        sort_key_names: List[str],
        deltacat_storage=unimplemented_deltacat_storage) \
        -> Optional[DeltaFileEnvelopeGroups]:

    # read input parquet s3 objects into a list of delta file envelopes
    delta_file_envelopes = _read_delta_file_envelopes(
        annotated_delta,
        primary_keys,
        sort_key_names,
        deltacat_storage,
    )
    if delta_file_envelopes is None:
        return None

    # group the data by primary key hash value
    hb_to_delta_file_envelopes = np.empty([num_hash_buckets], dtype="object")
    for dfe in delta_file_envelopes:
        hash_bucket_to_table = _group_by_pk_hash_bucket(
            dfe.table,
            num_hash_buckets,
            primary_keys,
        )
        for hb, table in enumerate(hash_bucket_to_table):
            if table:
                if hb_to_delta_file_envelopes[hb] is None:
                    hb_to_delta_file_envelopes[hb] = []
                hb_to_delta_file_envelopes[hb].append(
                    DeltaFileEnvelope.of(
                        dfe.stream_position,
                        dfe.file_index,
                        dfe.delta_type,
                        table))
    return hb_to_delta_file_envelopes

def _read_delta_file_envelopes(
        annotated_delta: DeltaAnnotated,
        primary_keys: List[str],
        sort_key_names: List[str],
        deltacat_storage=unimplemented_deltacat_storage) \
        -> Optional[List[DeltaFileEnvelope]]:

    columns_to_read = list(chain(primary_keys, sort_key_names))
    tables = deltacat_storage.download_delta(
        annotated_delta,
        max_parallelism=1,
        columns=columns_to_read,
        storage_type=StorageType.LOCAL,
    )
    annotations = annotated_delta.annotations
    assert(len(tables) == len(annotations),
           f"Unexpected Error: Length of downloaded delta manifest tables "
           f"({len(tables)}) doesn't match the length of delta manifest "
           f"annotations ({len(annotations)}).")
    if not tables:
        return None

    delta_file_envelopes = []
    for i, table in enumerate(tables):
        delta_file = DeltaFileEnvelope.of(
            annotations[i].annotation_stream_position,
            annotations[i].annotation_file_index,
            annotations[i].annotation_delta_type,
            table,
        )
        delta_file_envelopes.append(delta_file)
    return delta_file_envelopes


@ray.remote(num_returns=2)
def hash_bucket(
        annotated_delta: DeltaAnnotated,
        primary_keys: List[str],
        sort_keys: List[SortKey],
        num_buckets: int,
        num_groups: int,
        deltacat_storage=unimplemented_deltacat_storage) -> HashBucketResult:

    logger.info(f"Starting hash bucket task...")
    sort_key_names = [key.key_name for key in sort_keys]
    delta_file_envelope_groups = _group_file_records_by_pk_hash_bucket(
        annotated_delta,
        num_buckets,
        primary_keys,
        sort_key_names,
        deltacat_storage,
    )
    hash_bucket_group_to_obj_id, object_refs = group_hash_bucket_indices(
        delta_file_envelope_groups,
        num_buckets,
        num_groups,
    )
    logger.info(f"Finished hash bucket task...")
    return hash_bucket_group_to_obj_id, object_refs
