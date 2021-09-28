import ray
import pyarrow as pa
import numpy as np
import logging

from itertools import chain
from pyarrow import csv as pacsv

from deltacat import logs
from deltacat.compute.compactor.model import delta_annotated as da, \
    delta_file_envelope, sort_key as sk
from deltacat.compute.compactor.utils.primary_key_index import \
    group_hash_bucket_indices, group_record_indices_by_hash_bucket
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.utils.common import sha1_digest
from deltacat.types.media import ContentType, CONTENT_TYPE_TO_USER_KWARGS_KEY
from deltacat.compute.compactor.utils import system_columns as sc

from ray.data.impl.arrow_block import SortKeyT
from typing import List, Optional, Generator, Dict, Any

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

_PK_BYTES_DELIMITER = b'L6kl7u5f'


def group_by_pk_hash_bucket(
        table: pa.Table,
        num_buckets: int,
        primary_keys: List[str]) -> np.ndarray:

    # generate the primary key digest column
    all_pk_column_fields = []
    for pk_name in primary_keys:
        # casting a primary key column to numpy also ensures no nulls exist
        column_fields = table[pk_name].to_numpy()
        all_pk_column_fields.append(column_fields)
    hash_column_generator = hash_pk_bytes_generator(all_pk_column_fields)
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
    for hash_bucket, indices in enumerate(hash_bucket_to_indices):
        if indices:
            hash_bucket_to_table[hash_bucket] = sc.append_record_idx_col(
                table.take(indices),
                indices,
            )
    return hash_bucket_to_table


def hash_pk_bytes_generator(all_column_fields) -> Generator[bytes, None, None]:
    for field_index in range(len(all_column_fields[0])):
        bytes_to_join = []
        for column_fields in all_column_fields:
            bytes_to_join.append(
                bytes(str(column_fields[field_index]), "utf-8")
            )
        yield sha1_digest(_PK_BYTES_DELIMITER.join(bytes_to_join))


def group_file_records_by_pk_hash_bucket(
        annotated_delta: Dict[str, Any],
        num_hash_buckets: int,
        column_names: List[str],
        primary_keys: List[str],
        sort_key_names: List[str],
        deltacat_storage=unimplemented_deltacat_storage) \
        -> Optional[np.ndarray]:

    # read input parquet s3 objects into a list of delta file envelopes
    delta_file_envelopes = read_delta_file_envelopes(
        annotated_delta,
        column_names,
        primary_keys,
        sort_key_names,
        deltacat_storage,
    )
    if delta_file_envelopes is None:
        return None

    # group the data by primary key hash value
    hb_to_delta_file_envelopes = np.empty([num_hash_buckets], dtype="object")
    for dfe in delta_file_envelopes:
        hash_bucket_to_table = group_by_pk_hash_bucket(
            delta_file_envelope.get_table(dfe),
            num_hash_buckets,
            primary_keys,
        )
        for hash_bucket, table in enumerate(hash_bucket_to_table):
            if table:
                if hb_to_delta_file_envelopes[hash_bucket] is None:
                    hb_to_delta_file_envelopes[hash_bucket] = []
                hb_to_delta_file_envelopes[hash_bucket].append(
                    delta_file_envelope.of(
                        delta_file_envelope.get_stream_position(dfe),
                        delta_file_envelope.get_file_index(dfe),
                        delta_file_envelope.get_delta_type(dfe),
                        table))
    return hb_to_delta_file_envelopes


def read_delta_file_envelopes(
        annotated_delta: Dict[str, Any],
        column_names: List[str],
        primary_keys: List[str],
        sort_key_names: List[str],
        deltacat_storage=unimplemented_deltacat_storage) \
        -> Optional[List[Dict[str, Any]]]:

    columns_to_read = list(chain(primary_keys, sort_key_names))
    # TODO (pdames): Always read delimited text primary key columns as strings?
    tables = deltacat_storage.download_delta(
        annotated_delta,
        file_reader_kwargs={
            CONTENT_TYPE_TO_USER_KWARGS_KEY[
                ContentType.UNESCAPED_TSV.value]: {
                "read_options": pacsv.ReadOptions(
                    column_names=column_names,
                ),
                "convert_options": pacsv.ConvertOptions(
                    include_columns=columns_to_read,
                    null_values=[""],
                    strings_can_be_null=True,
                )
            },
            CONTENT_TYPE_TO_USER_KWARGS_KEY[ContentType.CSV.value]: {
                "read_options":
                    pacsv.ReadOptions(column_names=column_names),
                "convert_options":
                    pacsv.ConvertOptions(include_columns=columns_to_read)
            },
            CONTENT_TYPE_TO_USER_KWARGS_KEY[ContentType.PARQUET.value]: {
                "columns": columns_to_read
            },
            CONTENT_TYPE_TO_USER_KWARGS_KEY[ContentType.FEATHER.value]: {
                "columns": columns_to_read
            },
        },
    )
    annotations = da.get_annotations(annotated_delta)
    assert(len(tables) == len(annotations),
           f"Unexpected Error: Length of downloaded delta manifest tables "
           f"({len(tables)}) doesn't match the length of delta manifest "
           f"annotations ({len(annotations)}).")
    if not tables:
        return None

    delta_file_envelopes = []
    for i, table in enumerate(tables):
        delta_file = delta_file_envelope.of(
            da.get_annotation_stream_position(annotations[i]),
            da.get_annotation_file_index(annotations[i]),
            da.get_annotation_delta_type(annotations[i]),
            table,
        )
        delta_file_envelopes.append(delta_file)
    return delta_file_envelopes


@ray.remote(num_returns=2)
def hash_bucket(
        annotated_delta: Dict[str, Any],
        column_names: List[str],
        primary_keys: List[str],
        sort_keys: SortKeyT,
        num_buckets: int,
        num_groups: int,
        deltacat_storage=unimplemented_deltacat_storage):

    logger.info(f"Starting hash bucket task...")
    sort_key_names = [sk.get_key_name(key) for key in sort_keys]
    delta_file_envelope_groups = group_file_records_by_pk_hash_bucket(
        annotated_delta,
        num_buckets,
        column_names,
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
