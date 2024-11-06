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
        iceberg_spec_format_version,
        memory_resource_planner,
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

    from deltacat.equality_delete_to_position_delete_converter.overrides import fetch_all_bucket_files
    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(table, num_of_buckets)

    convert_options_provider = functools.partial(
        task_resource_options_provider,
        pg_config=params.pg_config,
        resource_amount_provider=memory_resource_planner,
        equality_delete_file_meta=equality_delete_dict[bucket_index],
        primary_keys=params.primary_keys,
        deltacat_storage=params.deltacat_storage,
        deltacat_storage_kwargs=params.deltacat_storage_kwargs,
        ray_custom_resources=params.ray_custom_resources,
        memory_logs_enabled=params.memory_logs_enabled,
        estimate_resources_params=params.estimate_resources_params,
    )

    def convert_input_provider(index, item) -> dict[str, MergeInput]:
        return {
            "input": ConvertInput.of(
                bucket_index=bucket_index,
                equality_delete_file=equality_delete_dict[index],
                position_delete_file=position_delete_dict[index],
                data_file_dict=data_file_dict[index],
                primary_keys=params.primary_keys,
                sort_keys=params.sort_keys,
                compact_small_files=compact_small_files,
                # copied from compactor v2
                enable_profiler=params.enable_profiler,
                metrics_config=params.metrics_config,
                s3_table_writer_kwargs=params.s3_table_writer_kwargs,
                read_kwargs_provider=params.read_kwargs_provider,
                deltacat_storage=params.deltacat_storage,
                deltacat_storage_kwargs=params.deltacat_storage_kwargs,
            )
        }
    # Ray remote task: convert
    # Assuming that memory consume by each bucket doesn't exceed one node's memory limit.
    # TODO: Add split mechanism to split large buckets
    convert_tasks_pending = invoke_parallel(
        items=all_files_for_bucket.items(),
        ray_task=convert,
        max_parallelism=task_max_parallelism,
        options_provider=convert_options_provider,
        kwargs_provider=convert_input_provider,
    )

    materialized_s3_links = ray.get(convert_tasks_pending)

    from deltacat.equality_delete_to_position_delete_converter.overrides import replace

    with table.transaction() as tx:
        snapshot_properties = {}
        commit_uuid = uuid.uuid4()
        with tx.update_snapshot(snapshot_properties=snapshot_properties).replace(
                commit_uuid=commit_uuid, using_starting_sequence=True
        ) as replace_snapshot:
            replace_snapshot.append_data_file(materialized_s3_links)
            replace_snapshot.delete_data_file(equality_delete_files)
            replace_snapshot._commit()


@ray.remote
def convert(bucket_index,
            equality_delete_file,
            position_delete_file,
            data_file_dict,
            primary_keys,
            sort_keys,
            compact_small_files):

    # Download ONLY equality delete table primary keys and store in Ray Plasma store
    equality_delete_table = download_bucketed_table(equality_delete_files=equality_delete_files,
                                                                     columns=primary_key_columns)

    equality_delete_table_ref = ray.put(equality_delete_table)

    def pos_compute_options_provider(memory_resource_planner):
        return (memory_resource_planner(data_file_primary_key_memory + previous_pos_delete_file_primary_key_memory +
                file_path_column_memory + pos_column_memory))


    file_level_compute_tasks_pending_ref = invoke_parallel(
        items=data_file_dict.items(),
        ray_task=compute_pos_delete_file_level,

        max_parallelism=task_max_parallelism,
        options_provider=pos_compute_options_provider,
        kwargs_provider=pos_compute_input_provider,
    )

    materialzed_pos_delete_file_link = ray.get(file_level_compute_tasks_pending_ref)

    # For query performance purpose, setting lower_bound == upper bound will help filter out positional delete files,
    # Code link: https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java#L52
    set_lower_upper_bound_positional_delete_metadata(positional_delete_files)

    return materialzed_pos_delete_file_link

@ray.remote
def compute_pos_delete_file_level(equality_delete_table_ref, data_file, previous_pos_delete_files):
    # fetch equality delete table from ray plasma store
    equality_delete_table = ray.get(equality_delete_table_ref)

    data_file_table = download_bucketed_table(files=data_file,
                                            columns=primary_key_columns)

    if previous_pos_delete_files:
        previous_pos_delete_table = download_bucketed_table(files=previous_pos_delete_file,
                                                        columns=primary_key_column)

    data_file_table = append_record_idx(data_file_table)
    new_position_delete_table = filter_rows_to_be_deleted(data_file_table, equality_delete_table)
    pos_delete_table = pa.concat_tables([previous_pos_delete_table, new_position_delete_table])
    pos_delete_table= append_data_file_path(pos_delete_table)

    # Write to S3, get S3 link back
    new_positional_delete_file = materialize_positional_delete_table_to_s3(pos_delete_table)
    return new_positional_delete_file

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
