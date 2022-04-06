import ray
import deltacat.aws.clients as aws_utils
import logging
import multiprocessing
import s3fs

from functools import partial

from uuid import uuid4

from ray.types import ObjectRef
from ray.data.datasource import BlockWritePathProvider
from ray.data.block import Block, BlockAccessor, BlockMetadata

from deltacat import logs
from deltacat.storage import LocalTable, LocalDataset, DistributedDataset, \
    Manifest, ManifestEntry, ManifestEntryList
from deltacat.aws.constants import TIMEOUT_ERROR_CODES
from deltacat.exceptions import RetryableError, NonRetryableError
from deltacat.types.media import ContentType, ContentEncoding
from deltacat.types.tables import TABLE_TYPE_TO_READER_FUNC, \
    TABLE_CLASS_TO_SIZE_FUNC, get_table_length
from deltacat.types.media import TableType

from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from tenacity import Retrying
from tenacity import wait_random_exponential
from tenacity import stop_after_delay
from tenacity import retry_if_exception_type

from typing import Any, Callable, Dict, List, Optional, Generator, Union, Tuple

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class UuidBlockWritePathProvider(BlockWritePathProvider):
    """Block write path provider implementation that writes each
    dataset block out to a file of the form: {base_path}/{uuid}
    """
    def __init__(self):
        self.write_paths: List[str] = []
        self.block_refs: List[ObjectRef[Block]] = []

    def _get_write_path_for_block(
            self,
            base_path: str,
            filesystem: Optional["pyarrow.fs.FileSystem"] = None,
            dataset_uuid: Optional[str] = None,
            block: Optional[ObjectRef[Block]] = None,
            block_index: Optional[int] = None,
            file_format: Optional[str] = None) -> str:
        write_path = f"{base_path}/{str(uuid4())}"
        self.write_paths.append(write_path)
        if block:
            self.block_refs.append(block)
        return write_path


class ParsedURL:
    def __init__(
            self,
            url: str):

        from urllib.parse import urlparse

        self._parsed = urlparse(
            url,
            allow_fragments=False  # support '#' in path
        )
        if not self._parsed.scheme:  # support paths w/o 's3://' scheme
            url = f"s3://{url}"
            self._parsed = urlparse(url, allow_fragments=False)
        if self._parsed.query:  # support '?' in path
            self.key = \
                f"{self._parsed.path.lstrip('/')}?{self._parsed.query}"
        else:
            self.key = self._parsed.path.lstrip('/')
        self.bucket = self._parsed.netloc
        self.url = self._parsed.geturl()


def parse_s3_url(url: str) -> ParsedURL:
    return ParsedURL(url)


def s3_resource_cache(
        region: Optional[str],
        **kwargs) -> ServiceResource:

    return aws_utils.resource_cache(
        "s3",
        region,
        **kwargs,
    )


def s3_client_cache(
        region: Optional[str],
        **kwargs) -> BaseClient:

    return aws_utils.client_cache(
        "s3",
        region,
        **kwargs
    )


def get_object_at_url(
        url: str,
        **s3_client_kwargs) -> Dict[str, Any]:

    s3 = s3_client_cache(
        None,
        **s3_client_kwargs)

    parsed_s3_url = parse_s3_url(url)
    return s3.get_object(
        Bucket=parsed_s3_url.bucket,
        Key=parsed_s3_url.key
    )


def delete_files_by_prefix(
        bucket: str,
        prefix: str,
        **s3_client_kwargs) -> None:

    s3 = s3_resource_cache(None, **s3_client_kwargs)
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()


def filter_objects_by_prefix(
        bucket: str,
        prefix: str,
        **s3_client_kwargs) -> Generator[Dict[str, Any], None, None]:

    s3 = s3_client_cache(
        None,
        **s3_client_kwargs
    )
    params = {"Bucket": bucket, "Prefix": prefix}
    more_objects_to_list = True
    while more_objects_to_list:
        response = s3.list_objects_v2(**params)
        if "Contents" in response:
            for obj in response["Contents"]:
                yield obj
        params["ContinuationToken"] = response.get("NextContinuationToken")
        more_objects_to_list = params["ContinuationToken"] is not None


def read_file(
        s3_url: str,
        content_type: ContentType,
        content_encoding: ContentEncoding = ContentEncoding.IDENTITY,
        table_type: TableType = TableType.PYARROW,
        column_names: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        file_reader_kwargs: Optional[Dict[str, Any]] = None,
        **s3_client_kwargs) -> LocalTable:

    reader = TABLE_TYPE_TO_READER_FUNC[table_type.value]
    try:
        table = reader(
            s3_url,
            content_type.value,
            content_encoding.value,
            column_names,
            include_columns,
            file_reader_kwargs,
            **s3_client_kwargs
        )
        return table
    except ClientError as e:
        if e.response["Error"]["Code"] in TIMEOUT_ERROR_CODES:
            # Timeout error not caught by botocore
            raise RetryableError(f"Retry table download from: {s3_url}") \
                from e
        raise NonRetryableError(f"Failed table download from: {s3_url}") \
            from e


def upload_sliced_table(
        table: Union[LocalTable, DistributedDataset],
        s3_url_prefix: str,
        s3_file_system: s3fs.S3FileSystem,
        max_records_per_entry: Optional[int],
        s3_table_writer_func: Callable,
        table_slicer_func: Callable,
        s3_table_writer_kwargs: Optional[Dict[str, Any]] = None,
        content_type: ContentType = ContentType.PARQUET,
        **s3_client_kwargs) -> ManifestEntryList:

    # @retry decorator can't be pickled by Ray, so wrap upload in Retrying
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_delay(30 * 60),
        retry=retry_if_exception_type(RetryableError)
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
            **s3_client_kwargs
        )
    else:
        # iteratively write table slices
        table_slices = table_slicer_func(
            table,
            max_records_per_entry
        )
        for table_slice in table_slices:
            slice_entries = retrying(
                upload_table,
                table_slice,
                f"{s3_url_prefix}",
                s3_file_system,
                s3_table_writer_func,
                s3_table_writer_kwargs,
                content_type,
                **s3_client_kwargs
            )
            manifest_entries.extend(slice_entries)

    return manifest_entries


@ray.remote
def _block_metadata(block: Block) -> BlockMetadata:
    return BlockAccessor.for_block(block).get_metadata(input_files=None)


def _get_metadata(
        table: Union[LocalTable, DistributedDataset],
        block_write_path_provider: UuidBlockWritePathProvider) \
        -> List[BlockMetadata]:
    metadata: List[BlockMetadata] = []
    if not block_write_path_provider.block_refs:
        # this must be a local table - ensure it was written to only 1 file
        assert len(block_write_path_provider.write_paths) == 1, \
            f"Expected table of type '{type(table)}' to be written to 1 " \
            f"file, but found {block_write_path_provider.write_paths} files."
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
                exec_stats=None)
        )
    else:
        # TODO(pdames): Expose BlockList metadata getter from Ray Dataset?
        metadata = table._blocks.get_metadata()
        if not metadata or metadata[0].size_bytes is None or \
                metadata[0].num_rows is None:
            metadata_futures = [_block_metadata.remote(block_ref)
                                for block_ref
                                in block_write_path_provider.block_refs]
            metadata = ray.get(metadata_futures)
    return metadata


def upload_table(
    table: Union[LocalTable, DistributedDataset],
    s3_base_url: str,
    s3_file_system: s3fs.S3FileSystem,
    s3_table_writer_func: Callable,
    s3_table_writer_kwargs: Optional[Dict[str, Any]],
    content_type: ContentType = ContentType.PARQUET,
    **s3_client_kwargs) -> ManifestEntryList:
    """
    Writes the given table to 1 or more S3 files and return Redshift
    manifest entries describing the uploaded files.
    """
    if s3_table_writer_kwargs is None:
        s3_table_writer_kwargs = {}

    block_write_path_provider = UuidBlockWritePathProvider()
    s3_table_writer_func(
        table,
        s3_base_url,
        s3_file_system,
        block_write_path_provider,
        content_type.value,
        **s3_table_writer_kwargs
    )
    metadata = _get_metadata(table, block_write_path_provider)
    manifest_entries = ManifestEntryList()
    for block_idx, s3_url in enumerate(block_write_path_provider.write_paths):
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
                raise RetryableError(f"Retry table upload to: {s3_url}") \
                    from e
            raise NonRetryableError(f"Failed table upload to: {s3_url}") \
                from e
    return manifest_entries


def download_manifest_entry(
        manifest_entry: ManifestEntry,
        token_holder: Optional[Dict[str, Any]] = None,
        table_type: TableType = TableType.PYARROW,
        column_names: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        file_reader_kwargs: Optional[Dict[str, Any]] = None) -> LocalTable:

    s3_client_kwargs = {
        "aws_access_key_id": token_holder["accessKeyId"],
        "aws_secret_access_key": token_holder["secretAccessKey"],
        "aws_session_token": token_holder["sessionToken"]
    } if token_holder else {}
    content_type = manifest_entry.meta.content_type
    content_encoding = manifest_entry.meta.content_encoding
    s3_url = manifest_entry.uri
    if s3_url is None:
        s3_url = manifest_entry.url
    table = read_file(
        s3_url,
        ContentType(content_type),
        ContentEncoding(content_encoding),
        table_type,
        column_names,
        include_columns,
        file_reader_kwargs,
        **s3_client_kwargs
    )
    return table


def _download_manifest_entries(
        manifest: Manifest,
        token_holder: Optional[Dict[str, Any]] = None,
        table_type: TableType = TableType.PYARROW,
        column_names: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        file_reader_kwargs: Optional[Dict[str, Any]] = None) \
        -> LocalDataset:

    return [
        download_manifest_entry(e, token_holder, table_type, column_names,
                                include_columns, file_reader_kwargs)
        for e in manifest.entries
    ]


def _download_manifest_entries_parallel(
        manifest: Manifest,
        token_holder: Optional[Dict[str, Any]] = None,
        table_type: TableType = TableType.PYARROW,
        max_parallelism: Optional[int] = None,
        column_names: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        file_reader_kwargs: Optional[Dict[str, Any]] = None) -> LocalDataset:

    tables = []
    pool = multiprocessing.Pool(max_parallelism)
    downloader = partial(
        download_manifest_entry,
        token_holder=token_holder,
        table_type=table_type,
        column_names=column_names,
        include_columns=include_columns,
        file_reader_kwargs=file_reader_kwargs,
    )
    for table in pool.map(downloader, [e for e in manifest.entries]):
        tables.append(table)
    return tables


def download_manifest_entries(
        manifest: Manifest,
        token_holder: Optional[Dict[str, Any]] = None,
        table_type: TableType = TableType.PYARROW,
        max_parallelism: Optional[int] = 1,
        column_names: Optional[List[str]] = None,
        include_columns: Optional[List[str]] = None,
        file_reader_kwargs: Optional[Dict[str, Any]] = None) -> LocalDataset:

    if max_parallelism and max_parallelism <= 1:
        return _download_manifest_entries(
            manifest,
            token_holder,
            table_type,
            column_names,
            include_columns,
            file_reader_kwargs,
        )
    else:
        return _download_manifest_entries_parallel(
            manifest,
            token_holder,
            table_type,
            max_parallelism,
            column_names,
            include_columns,
            file_reader_kwargs,
        )


def upload(
        s3_url: str,
        body,
        **s3_client_kwargs) -> Dict[str, Any]:

    # TODO (pdames): add tenacity retrying
    parsed_s3_url = parse_s3_url(s3_url)
    s3 = s3_client_cache(None, **s3_client_kwargs)
    return s3.put_object(
        Body=body,
        Bucket=parsed_s3_url.bucket,
        Key=parsed_s3_url.key,
    )


def download(
        s3_url: str,
        fail_if_not_found: bool = True,
        **s3_client_kwargs) -> Optional[Dict[str, Any]]:

    # TODO (pdames): add tenacity retrying
    parsed_s3_url = parse_s3_url(s3_url)
    s3 = s3_client_cache(None, **s3_client_kwargs)
    try:
        return s3.get_object(
            Bucket=parsed_s3_url.bucket,
            Key=parsed_s3_url.key,
        )
    except ClientError as e:
        if fail_if_not_found:
            raise
        else:
            if e.response['Error']['Code'] != "404":
                if e.response['Error']['Code'] != 'NoSuchKey':
                    raise
            logger.info(
                f"file not found: {s3_url}")
    except s3.exceptions.NoSuchKey:
        if fail_if_not_found:
            raise
        else:
            logger.info(
                f"file not found: {s3_url}")
    return None

