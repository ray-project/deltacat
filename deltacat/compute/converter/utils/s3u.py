from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_delay,
    wait_random_exponential,
)
from typing import Union
from deltacat.aws.s3u import CapturedBlockWritePaths, UuidBlockWritePathProvider
from deltacat.types.tables import (
    get_table_writer,
    get_table_length,
    TABLE_CLASS_TO_SLICER_FUNC,
)
from typing import Optional, Dict, Any, List
from deltacat.exceptions import RetryableError
from deltacat.storage import (
    DistributedDataset,
    LocalTable,
)
from deltacat.types.media import (
    ContentEncoding,
    ContentType,
)
from deltacat.aws.s3u import UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY
import s3fs


def get_credential():
    import boto3

    boto3_session = boto3.Session()
    credentials = boto3_session.get_credentials()
    return credentials


def get_s3_file_system(content_type):
    token_holder = get_credential()
    content_encoding = ContentEncoding.IDENTITY

    s3_file_system = s3fs.S3FileSystem(
        key=token_holder.access_key,
        secret=token_holder.secret_key,
        token=token_holder.token,
        s3_additional_kwargs={
            "ServerSideEncryption": "aws:kms",
            # TODO: Get tagging from table properties
            "ContentType": content_type.value,
            "ContentEncoding": content_encoding.value,
        },
    )
    return s3_file_system


def upload_table_with_retry(
    table: Union[LocalTable, DistributedDataset],
    s3_url_prefix: str,
    s3_table_writer_kwargs: Optional[Dict[str, Any]],
    content_type: ContentType = ContentType.PARQUET,
    max_records_per_file: Optional[int] = 4000000,
    s3_file_system=None,
    **s3_client_kwargs,
) -> List[str]:
    """
    Writes the given table to 1 or more S3 files and return Redshift
    manifest entries describing the uploaded files.
    """
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_delay(UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )

    if s3_table_writer_kwargs is None:
        s3_table_writer_kwargs = {}

    if not s3_file_system:
        s3_file_system = get_s3_file_system(content_type=content_type)
    capture_object = CapturedBlockWritePaths()
    block_write_path_provider = UuidBlockWritePathProvider(
        capture_object=capture_object
    )
    s3_table_writer_func = get_table_writer(table)
    table_record_count = get_table_length(table)
    if max_records_per_file is None or not table_record_count:
        retrying(
            fn=upload_table,
            table_slices=table,
            s3_base_url=f"{s3_url_prefix}",
            s3_file_system=s3_file_system,
            s3_table_writer_func=s3_table_writer_func,
            s3_table_writer_kwargs=s3_table_writer_kwargs,
            block_write_path_provider=block_write_path_provider,
            content_type=content_type,
            **s3_client_kwargs,
        )
    else:
        table_slicer_func = TABLE_CLASS_TO_SLICER_FUNC.get(type(table))
        table_slices = table_slicer_func(table, max_records_per_file)
        for table_slice in table_slices:
            retrying(
                fn=upload_table,
                table_slices=table_slice,
                s3_base_url=f"{s3_url_prefix}",
                s3_file_system=s3_file_system,
                s3_table_writer_func=s3_table_writer_func,
                s3_table_writer_kwargs=s3_table_writer_kwargs,
                block_write_path_provider=block_write_path_provider,
                content_type=content_type,
                **s3_client_kwargs,
            )
    del block_write_path_provider
    write_paths = capture_object.write_paths()
    return write_paths


def upload_table(
    table_slices,
    s3_base_url,
    s3_file_system,
    s3_table_writer_func,
    block_write_path_provider,
    content_type,
    s3_table_writer_kwargs,
):
    s3_table_writer_func(
        table_slices,
        s3_base_url,
        s3_file_system,
        block_write_path_provider,
        content_type.value,
        **s3_table_writer_kwargs,
    )
    # TODO: Add a proper fix for block_refs and write_paths not persisting in Ray actors
