import ray
import logging
from collections import defaultdict
from typing import Dict, List, Any, Tuple, Optional
import pyarrow as pa
import pyarrow.compute as pc

from deltacat import logs
import deltacat.compute.converter.utils.iceberg_columns as sc
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.compute.converter.utils.io import (
    download_data_table_and_append_iceberg_columns,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@ray.remote
def hash_bucket(
    sub_bucket_input_files: List[Tuple[int, Any]],
    identifier_columns: List[str],
    num_hash_buckets: int,
    s3_client_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[int, List[str]], Dict[int, int]]:
    """
    Process a sub-bucket of files and distribute records into hash buckets.

    Args:
        sub_bucket_input_files: List of (sequence_number, data_file) tuples for this sub-bucket
        identifier_columns: List of identifier column names for hashing
        num_hash_buckets: Number of hash buckets to distribute records into
        s3_client_kwargs: S3 client configuration

    Returns:
        Tuple of (bucket_number -> list of object_ref_ids, bucket_number -> record_count)
    """
    if s3_client_kwargs is None:
        s3_client_kwargs = {}

    logger.info(f"Processing sub-bucket with {len(sub_bucket_input_files)} files")

    # Download and combine all files in this sub-bucket
    data_file_tables = []
    downloaded_data_file_record_count = 0

    for sequence_number, data_file in sub_bucket_input_files:
        logger.info(f"Downloading data file: {data_file.file_path}")

        data_file_table = download_data_table_and_append_iceberg_columns(
            file=data_file,
            columns_to_download=identifier_columns,
            additional_columns_to_append=[
                sc._FILE_PATH_COLUMN_NAME,
                sc._ORDERED_RECORD_IDX_COLUMN_NAME,
            ],
            s3_client_kwargs=s3_client_kwargs,
        )

        logger.info(f"Downloaded table length: {len(data_file_table)}")
        downloaded_data_file_record_count += len(data_file_table)
        data_file_tables.append(data_file_table)

    # Concatenate all tables from this sub-bucket
    if len(data_file_tables) == 1:
        combined_table = data_file_tables[0]
    else:
        combined_table = pa.concat_tables(data_file_tables)

    logger.info(f"Combined table record count: {downloaded_data_file_record_count}")

    # Generate hash bucket index column
    data_file_table_with_bucket_index = generate_pk_hash_bucket_index_column(
        data_file_table=combined_table,
        identifier_columns=identifier_columns,
        num_hash_buckets=num_hash_buckets,
    )

    # Group by hash bucket and store in object store
    (
        bucket_number_to_object_refs,
        bucket_number_to_record_counts,
    ) = group_by_hash_bucket_and_put_object(
        data_file_table_with_bucket_index, num_hash_buckets
    )

    logger.info(f"Created {len(bucket_number_to_object_refs)} hash buckets")
    return bucket_number_to_object_refs, bucket_number_to_record_counts


def generate_pk_hash_bucket_index_column(
    data_file_table: pa.Table, identifier_columns: List[str], num_hash_buckets: int
) -> pa.Table:
    """
    Generate hash bucket index column based on existing identifier hash column.

    Args:
        data_file_table: PyArrow table with pre-computed hash column
        identifier_columns: List of identifier column names (used for logging)
        num_hash_buckets: Number of hash buckets

    Returns:
        Table with added hash_bucket_index column
    """
    logger.info(
        f"Generating hash bucket index for {len(identifier_columns)} identifier columns"
    )

    # The table already contains the hash column computed from identifier columns
    # We just need to convert the existing hash to bucket indices
    hash_column = data_file_table.column(sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME)

    # Calculate bucket index for each hash value
    hash_bucket_indices = []
    for i in range(len(hash_column)):
        hash_value = hash_column[i].as_py()  # Convert to Python value
        if hash_value is not None:
            # Convert hash string to integer and mod with number of buckets
            # Hash is already in hex string format from concatenate_hashed_identifier_columns
            hash_int = int(hash_value, 16)
            bucket_index = hash_int % num_hash_buckets
        else:
            bucket_index = 0  # Default bucket for null values
        hash_bucket_indices.append(bucket_index)

    # Add hash bucket index column to table
    hash_bucket_array = pa.array(hash_bucket_indices, type=pa.int32())
    table_with_hash_bucket = data_file_table.add_column(
        len(data_file_table.column_names),
        sc._HASH_BUCKET_INDEX_COLUMN_NAME,
        hash_bucket_array,
    )

    logger.info(
        f"Added hash_bucket_index column with {len(hash_bucket_indices)} values"
    )
    return table_with_hash_bucket


def group_by_hash_bucket_and_put_object(
    data_file_table: pa.Table, num_hash_buckets: int
) -> Tuple[Dict[int, List[str]], Dict[int, int]]:
    """
    Group records by hash bucket and store each bucket in object store.

    Args:
        data_file_table: Table with hash_bucket_index column
        num_hash_buckets: Number of hash buckets

    Returns:
        Tuple of (bucket_number -> list of object reference IDs, bucket_number -> record_count)
    """
    bucket_number_to_object_refs = defaultdict(list)
    bucket_number_to_record_counts = defaultdict(int)
    object_store = RayPlasmaObjectStore()

    for bucket_number in range(num_hash_buckets):
        # Filter table for this bucket
        filter_mask = pc.equal(
            data_file_table.column(sc._HASH_BUCKET_INDEX_COLUMN_NAME),
            pa.scalar(bucket_number, type=pa.int32()),
        )
        bucket_table = data_file_table.filter(filter_mask)

        # Track record count for this bucket
        record_count = len(bucket_table)
        bucket_number_to_record_counts[bucket_number] = record_count

        # Only store non-empty buckets
        if record_count > 0:
            logger.info(f"Hash bucket {bucket_number}: {record_count} records")

            # Store in object store and get reference ID
            object_ref_id = object_store.put(bucket_table)
            bucket_number_to_object_refs[bucket_number].append(object_ref_id)
        else:
            logger.info(f"Hash bucket {bucket_number}: empty, skipping")

    logger.info(f"Stored {len(bucket_number_to_object_refs)} non-empty hash buckets")
    return dict(bucket_number_to_object_refs), dict(bucket_number_to_record_counts)
