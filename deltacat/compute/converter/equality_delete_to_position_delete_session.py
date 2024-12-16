# from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from typing import Union, Optional, List
from deltacat.utils.ray_utils.concurrency import (
    invoke_parallel,
    task_resource_options_provider,
)
import ray
import uuid
import functools
from deltacat.compute.converter.utils.convert_task_options import convert_resource_options_provider
import logging
from itertools import groupby
from deltacat import logs
from collections import defaultdict
from deltacat.compute.converter.steps.convert import convert
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.pyiceberg.overrides import fetch_all_bucket_files, parquet_files_dict_to_iceberg_data_files
from deltacat.compute.converter.utils.converter_session_utils import check_data_files_sequence_number
from deltacat.compute.converter.pyiceberg.replace_snapshot import commit_replace_snapshot, commit_append_and_delete_snapshot
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

ray.init(local_mode=True)
def convert_equality_deletes_to_position_deletes(
        compact_small_files,
        position_delete_for_single_file,
        task_max_parallelism,
        iceberg_table,
        merge_keys: Optional[List[str]]=None):
    """
    Convert equality delete to position delete.
    Compute and memory heavy work from downloading equality delete table and compute position deletes
    will be executed on Ray remote tasks.
    """


    # Run on head node
    # catalog = load_catalog(iceberg_catalog_name=iceberg_catalog_name,
    #                        iceberg_catalog_properties=iceberg_catalog_properties,
    #                        )
    # iceberg_table = fetch_table(catalog, source_table_identifier)

    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(iceberg_table)

    # files_for_each_bucket: {partition_value: [(equality_delete_files_list, data_files_list, pos_delete_files_list)]
    files_for_each_bucket = defaultdict(tuple)
    for k,v in data_file_dict.items():
        print(f"data_file: k, v:{k, v}")
    for k, v in equality_delete_dict.items():
        print(f"equality_delete_file: k, v:{k, v}")
    for partition_value, equality_delete_file_list in equality_delete_dict.items():
        result_equality_delete_file, result_data_file = check_data_files_sequence_number(
            data_files_list=data_file_dict[partition_value],
            equality_delete_files_list=equality_delete_dict[partition_value])
        print(f"result_data_file:{result_data_file}")
        print(f"result_equality_delete_file:{result_equality_delete_file}")
        files_for_each_bucket[partition_value] = (result_data_file, result_equality_delete_file, [])

    # Using table identifier fields as merge keys if merge keys not provided
    if not merge_keys:
        identifier_fields = iceberg_table.schema().identifier_field_names()
    convert_options_provider = functools.partial(
        task_resource_options_provider,
        resource_amount_provider=convert_resource_options_provider,
    )
    max_file_to_download_based_on_memory = 10

    def convert_input_provider(index, item):
        return {
            "convert_input": ConvertInput.of(
                files_for_each_bucket=item,
                convert_task_index=index,
                identifier_fields=identifier_fields,
                compact_small_files=compact_small_files,
                position_delete_for_multiple_data_files=position_delete_for_single_file,
                max_parallel_data_file_download=max_file_to_download_based_on_memory,
            )
        }
    # Ray remote task: convert
    # Assuming that memory consume by each bucket doesn't exceed one node's memory limit.
    # TODO: Add split mechanism to split large buckets
    convert_tasks_pending = invoke_parallel(
        items=files_for_each_bucket.items(),
        ray_task=convert,
        max_parallelism=task_max_parallelism,
        options_provider=convert_options_provider,
        kwargs_provider=convert_input_provider,
    )
    to_be_deleted_files_list = []
    to_be_added_files_dict_list = []
    convert_results = ray.get(convert_tasks_pending)
    for convert_result in convert_results:
        to_be_deleted_files_list.extend(convert_result[0].values())
        to_be_added_files_dict_list.append(convert_result[1])

    new_position_delete_files = parquet_files_dict_to_iceberg_data_files(io=iceberg_table.io,
                                                                    table_metadata=iceberg_table.metadata,
                                                                    files_dict_list=to_be_added_files_dict_list)
    commit_overwrite_snapshot(iceberg_table=iceberg_table,
                            to_be_deleted_files_list=to_be_deleted_files_list[0],
                            new_position_delete_files=new_position_delete_files)
    # commit_replace_snapshot(iceberg_table=iceberg_table,
    #                         to_be_deleted_files_list=to_be_deleted_files_list[0],
    #                         new_position_delete_files=new_position_delete_files)


def fetch_table(iceberg_catalog, table_identifer):
    iceberg_table = iceberg_catalog.load_table(table_identifer)
    return iceberg_table

def load_catalog(iceberg_catalog_name, iceberg_catalog_properties):
    catalog = load_catalog(
        name=iceberg_catalog_name,
        **iceberg_catalog_properties,
    )
    return catalog

def get_s3_path(bucket_name: str, database_name: Optional[str] = None, table_name: Optional[str] = None) -> str:
    result_path = f"s3://{bucket_name}"
    if database_name is not None:
        result_path += f"/{database_name}.db"

    if table_name is not None:
        result_path += f"/{table_name}"
    return result_path

def get_bucket_name():
    return "metadata-py4j-zyiqin1"
def get_credential():
    import boto3
    boto3_session = boto3.Session()
    credentials = boto3_session.get_credentials()
    return credentials
def get_glue_catalog():
#     # from pyiceberg.catalog.glue import GLUE_CATALOG_ENDPOINT, GlueCatalog
    from pyiceberg.catalog import load_catalog
    credential = get_credential()
    access_key_id = credential.access_key
    secret_access_key = credential.secret_key
    session_token = credential.token
    # print(f"session_token: {session_token}")
    s3_path = get_s3_path(get_bucket_name())
    glue_catalog = load_catalog("glue", **{"warehouse": s3_path,
                    "type": "glue",
                    "aws_access_key_id": access_key_id,
                    "aws_secret_access_key": secret_access_key,
                    "aws_session_token": session_token,
                    "region_name": "us-east-1",
                    "s3.access-key-id": access_key_id,
                    "s3.secret-access-key": secret_access_key,
                    "s3.session-token": session_token,
                    "s3.region": "us-east-1"})

    return glue_catalog
