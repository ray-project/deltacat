import logging
import multiprocessing
from functools import partial
from typing import Any, Callable, Dict, Generator, List, Optional, Union
from uuid import uuid4
from botocore.config import Config
from deltacat.aws.constants import (
    BOTO_MAX_RETRIES,
    UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY,
    BOTO_THROTTLING_ERROR_CODES,
    RETRYABLE_TRANSIENT_ERRORS,
    BOTO_TIMEOUT_ERROR_CODES,
    UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY,
    DOWNLOAD_MANIFEST_ENTRY_RETRY_STOP_AFTER_DELAY,
)

import pyarrow as pa
import ray
import s3fs
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource import FilenameProvider
from ray.types import ObjectRef
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_delay,
    wait_random_exponential,
)
from deltacat.utils.ray_utils.concurrency import invoke_parallel
import deltacat.aws.clients as aws_utils
from deltacat import logs
from deltacat.storage import (
    DistributedDataset,
    LocalDataset,
    LocalTable,
    Manifest,
    ManifestEntry,
    ManifestEntryList,
)
from deltacat.types.media import (
    ContentEncoding,
    ContentType,
    TableType,
    DistributedDatasetType,
)
from deltacat.types.tables import (
    TABLE_CLASS_TO_SIZE_FUNC,
    TABLE_TYPE_TO_READER_FUNC,
    TABLE_TYPE_TO_DATASET_CREATE_FUNC_REFS,
    DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC,
    get_table_length,
)
from deltacat.exceptions import (
    RetryableError,
    RetryableUploadTableError,
    RetryableDownloadTableError,
    RetryableDownloadFileError,
    RetryableUploadFileError,
    NonRetryableDownloadFileError,
    NonRetryableUploadFileError,
    NonRetryableUploadTableError,
    NonRetryableDownloadTableError,
)
from deltacat.types.partial_download import PartialFileDownloadParams
from deltacat.utils.common import ReadKwargsProvider
from deltacat.exceptions import categorize_errors

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CapturedBlockWritePaths:
    def __init__(self):
        self._write_paths: List[str] = []
        self._block_refs: List[ObjectRef[Block]] = []

    def extend(self, write_paths: List[str], block_refs: List[ObjectRef[Block]]):
        try:
            iter(write_paths)
        except TypeError:
            pass
        else:
            self._write_paths.extend(write_paths)
        try:
            iter(block_refs)
        except TypeError:
            pass
        else:
            self._block_refs.extend(block_refs)

    def write_paths(self) -> List[str]:
        return self._write_paths

    def block_refs(self) -> List[ObjectRef[Block]]:
        return self._block_refs


class UuidBlockWritePathProvider(FilenameProvider):
    """Block write path provider implementation that writes each
    dataset block out to a file of the form: {base_path}/{uuid}
    """

    def __init__(
        self, capture_object: CapturedBlockWritePaths, base_path: Optional[str] = None
    ):
        self.base_path = base_path
        self.write_paths: List[str] = []
        self.block_refs: List[ObjectRef[Block]] = []
        self.capture_object = capture_object

    def __del__(self):
        if self.write_paths or self.block_refs:
            self.capture_object.extend(
                self.write_paths,
                self.block_refs,
            )

    def get_filename_for_block(
        self, block: Any, task_index: int, block_index: int
    ) -> str:
        if self.base_path is None:
            raise ValueError(
                "Base path must be provided to UuidBlockWritePathProvider",
            )
        return self._get_write_path_for_block(
            base_path=self.base_path,
            block=block,
            block_index=block_index,
        )

    def _get_write_path_for_block(
        self,
        base_path: str,
        *,
        filesystem: Optional[pa.filesystem.FileSystem] = None,
        dataset_uuid: Optional[str] = None,
        block: Optional[ObjectRef[Block]] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        write_path = f"{base_path}/{str(uuid4())}"
        self.write_paths.append(write_path)
        if block:
            self.block_refs.append(block)
        return write_path

    def __call__(
        self,
        base_path: str,
        *,
        filesystem: Optional[pa.filesystem.FileSystem] = None,
        dataset_uuid: Optional[str] = None,
        block: Optional[ObjectRef[Block]] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        return self._get_write_path_for_block(
            base_path,
            filesystem=filesystem,
            dataset_uuid=dataset_uuid,
            block=block,
            block_index=block_index,
            file_format=file_format,
        )


class S3Url:
    def __init__(self, url: str):

        from urllib.parse import urlparse

        self._parsed = urlparse(url, allow_fragments=False)  # support '#' in path
        if not self._parsed.scheme:  # support paths w/o 's3://' scheme
            url = f"s3://{url}"
            self._parsed = urlparse(url, allow_fragments=False)
        if self._parsed.query:  # support '?' in path
            self.key = f"{self._parsed.path.lstrip('/')}?{self._parsed.query}"
        else:
            self.key = self._parsed.path.lstrip("/")
        self.bucket = self._parsed.netloc
        self.url = self._parsed.geturl()


def parse_s3_url(url: str) -> S3Url:
    return S3Url(url)


def s3_resource_cache(region: Optional[str], **kwargs) -> ServiceResource:

    return aws_utils.resource_cache(
        "s3",
        region,
        **kwargs,
    )


def s3_client_cache(region: Optional[str], **kwargs) -> BaseClient:

    return aws_utils.client_cache("s3", region, **kwargs)


def get_object_at_url(url: str, **s3_client_kwargs) -> Dict[str, Any]:

    s3 = s3_client_cache(None, **s3_client_kwargs)

    parsed_s3_url = parse_s3_url(url)
    return s3.get_object(Bucket=parsed_s3_url.bucket, Key=parsed_s3_url.key)


def delete_files_by_prefix(bucket: str, prefix: str, **s3_client_kwargs) -> None:

    s3 = s3_resource_cache(None, **s3_client_kwargs)
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()


def filter_paths_by_prefix(bucket, prefix):
    return objects_to_paths(
        bucket,
        filter_objects_by_prefix(bucket, prefix),
    )


def objects_to_paths(bucket, objects):
    for obj in objects:
        yield get_path_from_object(bucket, obj)


def get_path_from_object(bucket, obj):
    return "s3://{}/{}".format(bucket, obj["Key"])


def filter_objects_by_prefix(
    bucket: str, prefix: str, **s3_client_kwargs
) -> Generator[Dict[str, Any], None, None]:

    s3 = s3_client_cache(None, **s3_client_kwargs)
    params = {"Bucket": bucket, "Prefix": prefix}
    more_objects_to_list = True
    while more_objects_to_list:
        response = s3.list_objects_v2(**params)
        if "Contents" in response:
            for obj in response["Contents"]:
                yield obj
        params["ContinuationToken"] = response.get("NextContinuationToken")
        more_objects_to_list = params["ContinuationToken"] is not None


@categorize_errors
def read_file(
    s3_url: str,
    content_type: ContentType,
    content_encoding: ContentEncoding = ContentEncoding.IDENTITY,
    table_type: TableType = TableType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    partial_file_download_params: Optional[PartialFileDownloadParams] = None,
    **s3_client_kwargs,
) -> LocalTable:

    reader = TABLE_TYPE_TO_READER_FUNC[table_type.value]
    try:
        table = reader(
            s3_url,
            content_type.value,
            content_encoding.value,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
            partial_file_download_params,
            **s3_client_kwargs,
        )
        return table
    except ClientError as e:
        if (
            e.response["Error"]["Code"]
            in BOTO_TIMEOUT_ERROR_CODES | BOTO_THROTTLING_ERROR_CODES
        ):
            # Timeout error not caught by botocore
            raise RetryableDownloadTableError(
                f"Retry table download from: {s3_url} after receiving {type(e).__name__}",
            ) from e
        raise NonRetryableDownloadTableError(
            f"Failed table download from: {s3_url} after receiving {type(e).__name__}"
        ) from e
    except RETRYABLE_TRANSIENT_ERRORS as e:
        raise RetryableDownloadTableError(
            f"Retry download for: {s3_url} after receiving {type(e).__name__}"
        ) from e
    except BaseException as e:
        logger.warning(
            f"Read has failed for {s3_url} and content_type={content_type} "
            f"and encoding={content_encoding}. Error: {e}",
            exc_info=True,
        )
        raise NonRetryableDownloadTableError(
            f"Read has failed for {s3_url} and content_type={content_type} "
            f"and encoding={content_encoding}",
        ) from e


def upload_sliced_table(
    table: Union[LocalTable, DistributedDataset],
    s3_url_prefix: str,
    s3_file_system: s3fs.S3FileSystem,
    max_records_per_entry: Optional[int],
    s3_table_writer_func: Callable,
    table_slicer_func: Callable,
    s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
    content_type: ContentType = ContentType.PARQUET,
    **s3_client_kwargs,
) -> ManifestEntryList:

    # @retry decorator can't be pickled by Ray, so wrap upload in Retrying
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_delay(UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )

    manifest_entries = ManifestEntryList()
    table_record_count = get_table_length(table)

    if max_records_per_entry is None or not table_record_count:
        # write the whole table to a single s3 file
        manifest_entries = retrying(
            upload_table,
            table,
            f"{s3_url_prefix}",
            s3_file_system,
            s3_table_writer_func,
            s3_table_writer_kwargs,
            content_type,
            **s3_client_kwargs,
        )
    else:
        # iteratively write table slices
        table_slices = table_slicer_func(table, max_records_per_entry)
        for table_slice in table_slices:
            slice_entries = retrying(
                upload_table,
                table_slice,
                f"{s3_url_prefix}",
                s3_file_system,
                s3_table_writer_func,
                s3_table_writer_kwargs,
                content_type,
                **s3_client_kwargs,
            )
            manifest_entries.extend(slice_entries)
    return manifest_entries


def upload_table(
    table: Union[LocalTable, DistributedDataset],
    s3_base_url: str,
    s3_file_system: s3fs.S3FileSystem,
    s3_table_writer_func: Callable,
    s3_table_writer_kwargs: Optional[Dict[str, Any]],
    content_type: ContentType = ContentType.PARQUET,
    **s3_client_kwargs,
) -> ManifestEntryList:
    """
    Writes the given table to 1 or more S3 files and return Redshift
    manifest entries describing the uploaded files.
    """
    if s3_table_writer_kwargs is None:
        s3_table_writer_kwargs = {}

    capture_object = CapturedBlockWritePaths()
    block_write_path_provider = UuidBlockWritePathProvider(capture_object)
    s3_table_writer_func(
        table,
        s3_base_url,
        s3_file_system,
        block_write_path_provider,
        content_type.value,
        **s3_table_writer_kwargs,
    )
    # TODO: Add a proper fix for block_refs and write_paths not persisting in Ray actors
    del block_write_path_provider
    block_refs = capture_object.block_refs()
    write_paths = capture_object.write_paths()
    metadata = _get_metadata(table, write_paths, block_refs)
    manifest_entries = ManifestEntryList()
    for block_idx, s3_url in enumerate(write_paths):
        try:
            manifest_entry = ManifestEntry.from_s3_obj_url(
                s3_url,
                metadata[block_idx].num_rows,
                metadata[block_idx].size_bytes,
                **s3_client_kwargs,
            )
            manifest_entries.append(manifest_entry)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                # s3fs may swallow S3 errors - we were probably throttled
                raise RetryableUploadTableError(
                    f"Retry table upload from: {s3_url} after receiving {type(e).__name__}",
                ) from e
            if (
                e.response["Error"]["Code"]
                in BOTO_TIMEOUT_ERROR_CODES | BOTO_THROTTLING_ERROR_CODES
            ):
                raise RetryableUploadTableError(
                    f"Retry table upload from: {s3_url} after receiving {type(e).__name__}",
                ) from e
            raise NonRetryableUploadTableError(
                f"Failed table upload to: {s3_url} after receiving {type(e).__name__}",
            ) from e
        except RETRYABLE_TRANSIENT_ERRORS as e:
            raise RetryableUploadTableError(
                f"Retry upload for: {s3_url} after receiving {type(e).__name__}",
            ) from e
        except BaseException as e:
            logger.warning(
                f"Upload has failed for {s3_url} and content_type={content_type}. Error: {e}",
                exc_info=True,
            )
            raise NonRetryableUploadTableError(
                f"Upload has failed for {s3_url} and content_type={content_type} because of {type(e).__name__}",
            ) from e
    return manifest_entries


def download_manifest_entry(
    manifest_entry: ManifestEntry,
    token_holder: Optional[Dict[str, Any]] = None,
    table_type: TableType = TableType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    content_type: Optional[ContentType] = None,
    content_encoding: Optional[ContentEncoding] = None,
) -> LocalTable:

    s3_client_kwargs = _get_s3_client_kwargs_from_token(token_holder=token_holder)
    if not content_type:
        content_type = manifest_entry.meta.content_type
        assert (
            content_type
        ), f"Unknown content type for manifest entry: {manifest_entry}"
        content_type = ContentType(content_type)
    if not content_encoding:
        content_encoding = manifest_entry.meta.content_encoding
        assert (
            content_encoding
        ), f"Unknown content encoding for manifest entry: {manifest_entry}"
        content_encoding = ContentEncoding(content_encoding)
    s3_url = manifest_entry.uri
    if s3_url is None:
        s3_url = manifest_entry.url

    partial_file_download_params = None
    if manifest_entry.meta and manifest_entry.meta.content_type_parameters:
        for type_params in manifest_entry.meta.content_type_parameters:
            if isinstance(type_params, PartialFileDownloadParams):
                partial_file_download_params = type_params
                break

    # @retry decorator can't be pickled by Ray, so wrap download in Retrying
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_delay(DOWNLOAD_MANIFEST_ENTRY_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )
    table = retrying(
        read_file,
        s3_url,
        content_type,
        content_encoding,
        table_type,
        column_names,
        include_columns,
        file_reader_kwargs_provider,
        partial_file_download_params,
        **s3_client_kwargs,
    )
    return table


@ray.remote
def download_manifest_entry_ray(*args, **kwargs) -> ObjectRef[LocalTable]:
    return download_manifest_entry(*args, **kwargs)


def download_manifest_entries(
    manifest: Manifest,
    token_holder: Optional[Dict[str, Any]] = None,
    table_type: TableType = TableType.PYARROW,
    max_parallelism: Optional[int] = 1,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
) -> LocalDataset:

    if max_parallelism and max_parallelism <= 1:
        return _download_manifest_entries(
            manifest,
            token_holder,
            table_type,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
        )
    else:
        return _download_manifest_entries_parallel(
            manifest,
            token_holder,
            table_type,
            max_parallelism,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
        )


def download_manifest_entries_distributed(
    manifest: Manifest,
    token_holder: Optional[Dict[str, Any]] = None,
    table_type: TableType = TableType.PYARROW,
    max_parallelism: Optional[int] = 1000,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: Optional[
        DistributedDatasetType
    ] = DistributedDatasetType.RAY_DATASET,
) -> DistributedDataset:

    params = {
        "manifest": manifest,
        "token_holder": token_holder,
        "table_type": table_type,
        "max_parallelism": max_parallelism,
        "column_names": column_names,
        "include_columns": include_columns,
        "file_reader_kwargs_provider": file_reader_kwargs_provider,
        "ray_options_provider": ray_options_provider,
        "distributed_dataset_type": distributed_dataset_type,
    }

    if distributed_dataset_type == DistributedDatasetType.RAY_DATASET:
        return _download_manifest_entries_ray_data_distributed(**params)
    elif distributed_dataset_type is not None:
        return _download_manifest_entries_all_dataset_distributed(**params)
    else:
        raise ValueError(
            f"Distributed dataset type {distributed_dataset_type} not supported."
        )


def upload(s3_url: str, body, **s3_client_kwargs) -> Dict[str, Any]:

    parsed_s3_url = parse_s3_url(s3_url)
    s3 = s3_client_cache(None, **s3_client_kwargs)
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=15),
        stop=stop_after_delay(UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )
    return retrying(
        _put_object,
        s3,
        body,
        parsed_s3_url.bucket,
        parsed_s3_url.key,
    )


def _put_object(
    s3_client, body: Any, bucket: str, key: str, **s3_put_object_kwargs
) -> Dict[str, Any]:
    try:
        return s3_client.put_object(
            Body=body, Bucket=bucket, Key=key, **s3_put_object_kwargs
        )
    except ClientError as e:
        if e.response["Error"]["Code"] in BOTO_THROTTLING_ERROR_CODES:
            error_code = e.response["Error"]["Code"]
            raise RetryableUploadFileError(
                f"Retry upload for: {bucket}/{key} after receiving {error_code}",
            ) from e
        raise NonRetryableUploadFileError(
            f"Failed table upload to: {bucket}/{key}"
        ) from e
    except RETRYABLE_TRANSIENT_ERRORS as e:
        raise RetryableUploadFileError(
            f"Retry upload for: {bucket}/{key} after receiving {type(e).__name__}"
        ) from e
    except BaseException as e:
        logger.error(
            f"Upload has failed for {bucket}/{key}. Error: {type(e).__name__}",
            exc_info=True,
        )
        raise NonRetryableUploadFileError(
            f"Failed table upload to: {bucket}/{key}"
        ) from e


def download(
    s3_url: str, fail_if_not_found: bool = True, **s3_client_kwargs
) -> Optional[Dict[str, Any]]:

    parsed_s3_url = parse_s3_url(s3_url)
    s3 = s3_client_cache(None, **s3_client_kwargs)
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=15),
        stop=stop_after_delay(UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )
    return retrying(
        _get_object,
        s3,
        parsed_s3_url.bucket,
        parsed_s3_url.key,
        fail_if_not_found=fail_if_not_found,
    )


def _get_object(s3_client, bucket: str, key: str, fail_if_not_found: bool = True):
    try:
        return s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            if fail_if_not_found:
                raise NonRetryableDownloadFileError(
                    f"Failed get object from: {bucket}/{key}"
                ) from e
            logger.info(f"file not found: {bucket}/{key}")
    except RETRYABLE_TRANSIENT_ERRORS as e:
        raise RetryableDownloadFileError(
            f"Retry get object: {bucket}/{key} after receiving {type(e).__name__}"
        ) from e

    return None


def _download_manifest_entries_parallel(
    manifest: Manifest,
    token_holder: Optional[Dict[str, Any]] = None,
    table_type: TableType = TableType.PYARROW,
    max_parallelism: Optional[int] = None,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
) -> LocalDataset:

    tables = []
    pool = multiprocessing.Pool(max_parallelism)
    downloader = partial(
        download_manifest_entry,
        token_holder=token_holder,
        table_type=table_type,
        column_names=column_names,
        include_columns=include_columns,
        file_reader_kwargs_provider=file_reader_kwargs_provider,
    )
    for table in pool.map(downloader, [e for e in manifest.entries]):
        tables.append(table)
    return tables


def _download_manifest_entries(
    manifest: Manifest,
    token_holder: Optional[Dict[str, Any]] = None,
    table_type: TableType = TableType.PYARROW,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
) -> LocalDataset:

    return [
        download_manifest_entry(
            manifest_entry=e,
            token_holder=token_holder,
            table_type=table_type,
            column_names=column_names,
            include_columns=include_columns,
            file_reader_kwargs_provider=file_reader_kwargs_provider,
        )
        for e in manifest.entries
    ]


@ray.remote
def _block_metadata(block: Block) -> BlockMetadata:
    return BlockAccessor.for_block(block).get_metadata(
        input_files=None,
        exec_stats=None,
    )


def _get_s3_client_kwargs_from_token(token_holder) -> Dict[Any, Any]:
    conf = Config(retries={"max_attempts": BOTO_MAX_RETRIES, "mode": "adaptive"})
    return (
        {
            "aws_access_key_id": token_holder["accessKeyId"],
            "aws_secret_access_key": token_holder["secretAccessKey"],
            "aws_session_token": token_holder["sessionToken"],
            "config": conf,
        }
        if token_holder
        else {"config": conf}
    )


def _get_metadata(
    table: Union[LocalTable, DistributedDataset],
    write_paths: List[str],
    block_refs: List[ObjectRef[Block]],
) -> List[BlockMetadata]:
    metadata: List[BlockMetadata] = []
    if not block_refs:
        # this must be a local table - ensure it was written to only 1 file
        assert len(write_paths) == 1, (
            f"Expected table of type '{type(table)}' to be written to 1 "
            f"file, but found {len(write_paths)} files."
        )
        table_size = None
        table_size_func = TABLE_CLASS_TO_SIZE_FUNC.get(type(table))
        if table_size_func:
            table_size = table_size_func(table)
        else:
            logger.warning(f"Unable to estimate '{type(table)}' table size.")
        metadata.append(
            BlockMetadata(
                num_rows=get_table_length(table),
                size_bytes=table_size,
                schema=None,
                input_files=None,
                exec_stats=None,
            )
        )
    else:
        # TODO(pdames): Expose BlockList metadata getter from Ray Dataset?
        # ray 1.10
        # metadata = dataset._blocks.get_metadata()
        # ray 2.0.0dev
        metadata = table._plan.execute().get_metadata()
        if (
            not metadata
            or metadata[0].size_bytes is None
            or metadata[0].num_rows is None
        ):
            metadata_futures = [
                _block_metadata.remote(block_ref) for block_ref in block_refs
            ]
            metadata = ray.get(metadata_futures)
    return metadata


def _download_manifest_entries_ray_data_distributed(
    manifest: Manifest,
    token_holder: Optional[Dict[str, Any]] = None,
    table_type: TableType = TableType.PYARROW,
    max_parallelism: Optional[int] = 1000,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
) -> DistributedDataset:

    table_pending_ids = []
    manifest_entries = manifest.entries
    if manifest_entries:
        table_pending_ids = invoke_parallel(
            manifest_entries,
            download_manifest_entry_ray,
            token_holder,
            table_type,
            column_names,
            include_columns,
            file_reader_kwargs_provider,
            max_parallelism=max_parallelism,
            options_provider=ray_options_provider,
        )
    return TABLE_TYPE_TO_DATASET_CREATE_FUNC_REFS[table_type](table_pending_ids)


def _download_manifest_entries_all_dataset_distributed(
    manifest: Manifest,
    token_holder: Optional[Dict[str, Any]] = None,
    table_type: TableType = TableType.PYARROW,
    max_parallelism: Optional[int] = 1000,
    column_names: Optional[List[str]] = None,
    include_columns: Optional[List[str]] = None,
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ray_options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    distributed_dataset_type: Optional[
        DistributedDatasetType
    ] = DistributedDatasetType.RAY_DATASET,
) -> DistributedDataset:

    entry_content_type = None
    entry_content_encoding = None
    uris = []
    for entry in manifest.entries or []:
        if (
            entry_content_type is not None
            and entry_content_type != entry.meta.content_type
        ):
            raise ValueError(
                f"Mixed content types of ({entry_content_type},"
                f" {entry.meta.content_type}) is not supported."
            )

        if (
            entry_content_encoding is not None
            and entry_content_encoding != entry.meta.content_encoding
        ):
            raise ValueError(
                f"Mixed content encoding of {entry_content_encoding},"
                f" {entry.meta.content_encoding} is not supported."
            )

        entry_content_type = entry.meta.content_type
        entry_content_encoding = entry.meta.content_encoding
        uris.append(entry.uri)

    s3_client_kwargs = _get_s3_client_kwargs_from_token(token_holder=token_holder)

    if distributed_dataset_type in DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC:
        return DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC[distributed_dataset_type.value](
            uris=uris,
            content_type=entry_content_type,
            content_encoding=entry_content_encoding,
            column_names=column_names,
            include_columns=include_columns,
            read_func_kwargs_provider=file_reader_kwargs_provider,
            ray_options_provider=ray_options_provider,
            s3_client_kwargs=s3_client_kwargs,
        )
    else:
        raise ValueError(
            f"Unsupported distributed dataset type={distributed_dataset_type}"
        )
