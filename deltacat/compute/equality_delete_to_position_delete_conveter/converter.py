from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from typing import Union, Optional
import invoke_parallel
import ray


def convert_equality_deletes_to_position_deletes(
        source_table_identifier: Union[str, Identifier],
        destination_table_identifier: Union[str, Identifier],
        read_branch,
        write_branch,
        snapshot_id,
        iceberg_catalog_name,
        iceberg_catalog_properties,
        **kwargs):
    """
    Convert equality delete to position delete.
    Compute and memory heavy work from downloading equality delete table and compute position deletes
    will be executed on Ray remote tasks.
    """
    # Run on head node
    catalog = load_catalog(iceberg_catalog_name=iceberg_catalog_name,
                           iceberg_catalog_properties=iceberg_catalog_properties,
                           )
    iceberg_table = fetch_table(catalog, source_table_identifier)
    num_of_buckets = get_num_of_bucket(iceberg_table)

    # Ray remote task: convert
    # Assuming that memory comsume by each bucket doesn't exceed one node's memory limit.
    # TODO: Add split mechanism to split large buckets
    convert_tasks_pending = invoke_parallel(
        ray_task=convert,
        max_parallelism=task_max_parallelism,
        options_provider=convert_options_provider,
        kwargs_provider=convert_input_provider,
    )
    materialized_s3_links = ray.get(convert_tasks_pending)

    # Commit ideally a REPLACE type snapshot, not supported yet in Pyiceberg, leave as placeholder for now.
    commit_s3_files_to_iceberg_snapshot(materialized_s3_links)


@ray.remote
def convert(table, snapshot_id, bucket_index):
    # Load Icberg table, get S3 links
    data_files, equality_delete_files = load_table_files(table, snapshot_id, bucket_index)

    # Read from S3
    compacted_table, equality_delete_table = download_bucketed_table(data_files=data_files,
                                                                     equality_delete_files=equality_delete_files,
                                                                     columns=primary_key_columns)

    # Compute
    compacted_table = append_record_idx(compacted_table)
    position_delete_table = filter_rows_to_be_deleted(compacted_table, equality_delete_table)
    position_delete_table = append_data_file_path(position_delete_table)

    # Write to S3, get S3 link back
    positional_delete_files = materialize_positional_delete_table_to_s3(position_delete_table)

    # For query performance purpose, setting lower_bound == upper bound will help filter out positional delete files,
    # Code link: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java#L52
    set_lower_upper_bound_positional_delete_metadata(positional_delete_files)

    return positional_delete_files


def fetch_table(iceberg_catalog, table_identifer):
    iceberg_table = iceberg_catalog.load_table(table_identifer)
    return iceberg_table


def get_num_of_bucket(table) -> int:
    from pyiceberg.transforms import BucketTransform
    for partition_field in table.metadata.partition_specs[0].fields:
        if isinstance(partition_field.transform, BucketTransform):
            return partition_field.transform.num_buckets


def load_catalog(iceberg_catalog_name, iceberg_catalog_properties):
    catalog = load_catalog(
        name=iceberg_catalog_name,
        **iceberg_catalog_properties,
    )
    return catalog


def materialize_positional_delete_table_to_s3(positional_delete_table):
    from deltacat.aws.s3u import upload_sliced_table
    upload_sliced_table(table=positional_delete_table)


def download_bucketed_table(data_files, equality_delete_files):
    from deltacat.utils.pyarrow import s3_file_to_table
    compacted_table = s3_file_to_table(data_files)
    equality_delete_table = s3_file_to_table(equality_delete_files)
    return compacted_table, equality_delete_table


def append_record_idx(compacted_table):
    return compacted_table


def filter_rows_to_be_deleted(compacted_table, equality_delete_table):
    if compacted_table:
        equality_delete_table = pc.is_in(
            compacted_table[sc._PK_HASH_STRING_COLUMN_NAME],
            incremental_table[sc._PK_HASH_STRING_COLUMN_NAME],
        )

        positional_delete_table = compacted_table.filter(equality_delete_table)
        logger.info(f"POSITIONAL_DELETE_TABLE_LENGTH:{len(positional_delete_table)}")

    # incremental_table = _drop_delta_type_rows(incremental_table, DeltaType.DELETE)
    # result_table_list.append(incremental_table)
    final_table = None
    if positional_delete_table:
        final_table = final_table.drop([sc._PK_HASH_STRING_COLUMN_NAME])
        final_table = final_table.drop([sc._ORDERED_RECORD_IDX_COLUMN_NAME])
        final_table = final_table.drop([sc._ORDERED_FILE_IDX_COLUMN_NAME])

    return final_table


def append_data_file_path():
    """
    Util function to construct positional delete files, positional delete files contains two columns: data file path, position
    """
    pass


def set_lower_upper_bound_positional_delete_metadata():
    """
    Util function to set lower bound and upper bound of positional delete file metadata
    """
    pass


def load_table_files(table, snapshot_id, bucket_index):
    """
    load files based on bucket index, utilze hidden partitioning to filter out irrelevant files.
    """
    # TODO: Performance testing using Daft scan task for plan partitioned files and estimated memory
    row_filter = f"bucket={bucket_index}"
    iceberg_tasks = table.scan(row_filter=row_filter, snapshot_id=snapshot_id).plan_files()
