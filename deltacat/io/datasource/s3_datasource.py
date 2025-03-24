import logging
import json

from collections import defaultdict
from enum import Enum
from typing import Union, List, Callable, Optional, Dict, Any, Tuple

import pyarrow as pa
import pyarrow.fs
from pyarrow import parquet as pq
from pyarrow.fs import S3FileSystem

from ray.data import (
    Datasource,
    ReadTask,
)
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data.block import BlockMetadata
from ray.data.datasource import (
    DefaultFileMetadataProvider,
    PathPartitionParser,
    FastFileMetadataProvider,
    ParquetMetadataProvider,
)

from deltacat.aws.s3u import (
    S3Url,
    parse_s3_url,
    filter_objects_by_prefix,
    objects_to_paths,
)
from deltacat.types.media import (
    ContentType,
    DELIMITED_TEXT_CONTENT_TYPES,
)
from deltacat.storage import (
    Manifest,
    ManifestEntryList,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.filesystem import resolve_paths_and_filesystem
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CachedFileMetadataProvider(
    FastFileMetadataProvider,
    ParquetMetadataProvider,
):
    def __init__(self, meta_cache: Dict[str, BlockMetadata]):
        self._meta_cache = meta_cache

    def get_meta_cache(self) -> Dict[str, BlockMetadata]:
        return self._meta_cache

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, pa.Schema]],
        **kwargs,
    ) -> BlockMetadata:
        agg_block_metadata = BlockMetadata(
            num_rows=0,
            size_bytes=0,
            schema=schema,
            input_files=[],
            exec_stats=None,
        )
        for path in paths:
            block_metadata = self._meta_cache.get(path)
            if block_metadata is None:
                raise ValueError(f"Block metadata not found for path: {path}")
            if block_metadata.num_rows is None:
                agg_block_metadata.num_rows = None
            elif agg_block_metadata.num_rows is not None:
                agg_block_metadata.num_rows += block_metadata.num_rows
            if block_metadata.size_bytes is None:
                agg_block_metadata.size_bytes = None
            elif agg_block_metadata.size_bytes is not None:
                agg_block_metadata.size_bytes += block_metadata.size_bytes
            agg_block_metadata.input_files.append(path)
        return agg_block_metadata


class S3PathType(str, Enum):
    MANIFEST = "manifest"
    PREFIX = "prefix"
    FILES_AND_FOLDERS = "files_and_folders"


class HivePartitionParser(PathPartitionParser):
    def __init__(
        self,
        base_dir: Optional[str] = None,
        filter_fn: Optional[Callable[[Dict[str, str]], bool]] = None,
    ):
        super(HivePartitionParser, self).__init__(
            base_dir=base_dir,
            filter_fn=filter_fn,
        )


class DelimitedTextReaderConfig:
    def __init__(
        self,
        csv: bool = False,
        header: bool = False,
        delimiter: Optional[str] = None,
        bzip2: bool = False,
        gzip: bool = False,
        zstd: bool = False,
        add_quotes: Optional[bool] = None,
        null_as: str = "",
        escape: bool = False,
        fixed_width: bool = False,
    ):
        self.header = header
        self.delimiter = delimiter if delimiter else "," if csv else "|"
        self.bzip2 = bzip2
        self.gzip = gzip
        self.zstd = zstd
        self.add_quotes = add_quotes if add_quotes else True if csv else False
        self.null_as = null_as
        self.escape = escape
        self.fixed_width = fixed_width

    def _get_arrow_compression_codec_name(self) -> str:
        arrow_compression_codec_name = None
        codecs_enabled = {
            "bz2": self.bzip2,
            "gzip": self.gzip,
            "zstd": self.zstd,
        }
        for encoding, flag in codecs_enabled.items():
            if arrow_compression_codec_name and flag:
                raise ValueError(
                    f"Multiple delimited text compression types specified "
                    f"({codecs_enabled}). Please ensure that only one "
                    f"compression type is set and try again."
                )
            if flag:
                arrow_compression_codec_name = encoding
        return arrow_compression_codec_name

    def to_arrow_reader_kwargs(
        self, include_columns: Optional[List[str]], schema: Optional[pa.Schema]
    ) -> Dict[str, Any]:
        from pyarrow import csv

        if self.fixed_width:
            raise NotImplementedError(
                "Delimited text files configured with FIXEDWIDTH are not "
                "currently supported."
            )
        open_stream_args = {}
        arrow_compression_codec_name = self._get_arrow_compression_codec_name()
        if arrow_compression_codec_name:
            open_stream_args["compression"] = arrow_compression_codec_name
        column_names = None
        if schema:
            column_names = schema.names
        autogen_column_names = False if self.header or column_names else True
        read_options = csv.ReadOptions(
            use_threads=False,
            column_names=column_names,
            autogenerate_column_names=autogen_column_names,
        )
        parse_options = csv.ParseOptions(
            delimiter=self.delimiter,
            quote_char='"' if self.add_quotes else False,
            escape_char="\\" if self.escape else False,
            double_quote=False if self.escape else True,
        )
        convert_options = csv.ConvertOptions(
            column_types=schema,
            null_values=[self.null_as] if self.null_as is not None else [],
            true_values=["t"],
            false_values=["f"],
            strings_can_be_null=True if self.null_as is not None else False,
            quoted_strings_can_be_null=True if self.null_as else False,
            include_columns=include_columns,
        )
        return {
            "open_stream_args": open_stream_args,
            "read_options": read_options,
            "parse_options": parse_options,
            "convert_options": convert_options,
        }


def normalize_s3_paths_for_filesystem(
    paths: Union[str, List[str]],
    filesystem: pyarrow.fs.FileSystem,
) -> Tuple[List[str], List[S3Url]]:
    urls = []
    if isinstance(paths, str):
        paths = [paths]
    if isinstance(filesystem, S3FileSystem):
        urls = [parse_s3_url(url) for url in paths]
        # pyarrow.fs.FileSystem paths should not start with "s3://"
        # pyarrow.fs.FileSystem paths should not end with "/"
        paths = [f"{u.bucket}/{u.key}".rstrip("/") for u in urls]
    return paths, urls


def _expand_s3_paths_by_content_type(
    base_paths: Union[str, List[str]],
    base_urls: List[S3Url],
    content_type_provider: Callable[[str], ContentType],
    path_type: S3PathType,
    user_fs: Optional[S3FileSystem],
    resolved_fs: S3FileSystem,
    **s3_client_kwargs,
) -> Tuple[Dict[ContentType, List[str]], CachedFileMetadataProvider]:
    if path_type == S3PathType.MANIFEST:
        content_type_to_paths, meta_provider = _expand_s3_manifest_paths(
            base_paths,
            resolved_fs,
            content_type_provider,
        )
    elif path_type == S3PathType.PREFIX:
        content_type_to_paths, meta_provider = _expand_s3_prefix_paths(
            base_urls,
            content_type_provider,
            **s3_client_kwargs,
        )
    elif path_type == S3PathType.FILES_AND_FOLDERS:
        # TODO(pdames): Only allow files and call get_object(file_path)?
        base_paths, file_infos = DefaultFileMetadataProvider().expand_paths(
            base_paths, resolved_fs
        )
        file_sizes = [file_info.size for file_info in file_infos]
        meta_provider = CachedFileMetadataProvider(
            {
                path: BlockMetadata(
                    num_rows=None,
                    size_bytes=file_sizes[i],
                    schema=None,
                    input_files=[],
                    exec_stats=None,
                )
                for i, path in enumerate(base_paths)
            }
        )
        content_type_to_paths = _infer_content_types_from_paths(
            base_paths,
            content_type_provider,
        )
    else:
        raise NotImplementedError(f"Unsupported S3 path type: {path_type}")
    # TODO(pdames): normalize S3 file paths before adding them to either
    #  content_type_to_paths or meta_provider
    # normalize S3 file paths for each content type based on the filesystem
    for content_type, paths in content_type_to_paths.items():
        paths, urls = normalize_s3_paths_for_filesystem(
            paths,
            user_fs,
        )
        content_type_to_paths[content_type] = paths
    # normalize block metadata provider S3 file paths based on the filesystem
    meta_provider = CachedFileMetadataProvider(
        {
            normalize_s3_paths_for_filesystem(path, user_fs)[0][0]: metadata
            for path, metadata in meta_provider.get_meta_cache().items()
        }
    )
    return content_type_to_paths, meta_provider


def _expand_s3_prefix_paths(
    urls: List[S3Url],
    content_type_provider: Callable[[str], ContentType],
    **s3_client_kwargs,
) -> Tuple[Dict[ContentType, List[str]], CachedFileMetadataProvider]:
    assert len(urls) == 1, f"Expected 1 S3 prefix, found {len(urls)}."
    objects = list(
        filter_objects_by_prefix(urls[0].bucket, urls[0].key, **s3_client_kwargs)
    )
    paths = list(
        objects_to_paths(
            urls[0].bucket,
            objects,
        )
    )
    meta_cache: Dict[str, BlockMetadata] = {
        path: BlockMetadata(
            num_rows=None,
            size_bytes=objects[i]["ContentLength"],
            schema=None,
            input_files=[],
            exec_stats=None,
        )
        for i, path in enumerate(paths)
    }
    content_type_to_paths = _infer_content_types_from_paths(
        paths,
        content_type_provider,
    )
    return content_type_to_paths, CachedFileMetadataProvider(meta_cache)


def _infer_content_types_from_paths(
    paths: List[str],
    content_type_provider: Callable[[str], ContentType],
) -> Dict[ContentType, List[str]]:
    content_type_to_paths = defaultdict(list)
    for path in paths:
        if not path.endswith("/"):
            content_type_to_paths[content_type_provider(path)].append(path)
    return content_type_to_paths


def _expand_s3_manifest_paths(
    paths: List[str],
    filesystem: Optional[S3FileSystem],
    content_type_provider: Callable[[str], ContentType],
) -> Tuple[Dict[ContentType, List[str]], CachedFileMetadataProvider]:
    assert len(paths) == 1, f"Expected 1 manifest path, found {len(paths)}."
    path = paths[0]
    with filesystem.open_input_file(path) as f:
        manifest = Manifest(json.loads(f.read()))
    content_type_to_paths = {}
    meta_provider = CachedFileMetadataProvider({})
    if not manifest.entries:
        logger.warning(f"No entries to read in DeltaCAT Manifest: {path}")
    else:
        content_type_to_paths, meta_provider = _read_manifest_entry_paths(
            manifest.entries,
            manifest.meta.content_type if manifest.meta else None,
            content_type_provider,
        )
    # TODO(pdames): infer the schema from a manifest if available?
    # if not schema and ContentType.PARQUET not in content_type_to_paths:
    #     schema = _infer_schema_from_manifest(manifest)
    return content_type_to_paths, meta_provider


def _read_manifest_entry_paths(
    entries: ManifestEntryList,
    manifest_content_type: Optional[str],
    content_type_provider: Callable[[str], ContentType],
) -> Tuple[Dict[ContentType, List[str]], CachedFileMetadataProvider]:
    # support manifests with heterogenous content types
    content_type_to_paths = defaultdict(list)
    meta_cache: Dict[str, BlockMetadata] = {}
    for e in entries:
        url = e.url if e.url else e.uri
        # get manifest entry content type or fall back to manifest content type
        content_type = e.meta.content_type or manifest_content_type
        if content_type:
            content_type_to_paths[ContentType(content_type)] = url
        else:
            # fall back to content type inference by file extension
            content_type_to_paths[content_type_provider(url)].append(url)
        meta_cache[url] = BlockMetadata(
            num_rows=e.meta.record_count,
            size_bytes=e.meta.content_length,
            schema=None,
            input_files=[],
            exec_stats=None,
        )
    return content_type_to_paths, CachedFileMetadataProvider(meta_cache)


class S3Datasource(Datasource):
    def prepare_read(
        self,
        parallelism: int,
        paths: Union[str, List[str]],
        content_type_provider: Callable[[str], ContentType],
        path_type: S3PathType = S3PathType.MANIFEST,
        filesystem: Optional[S3FileSystem] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[pa.Schema] = None,
        csv_reader_config: DelimitedTextReaderConfig = DelimitedTextReaderConfig(),
        partitioning: HivePartitionParser = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        read_kwargs_provider: Optional[ReadKwargsProvider] = None,
        **s3_client_kwargs,
    ) -> List[ReadTask]:
        # if required, normalize s3 paths to work with the filesystem provided
        if not filesystem:
            filesystem = S3FileSystem()
        paths, urls = normalize_s3_paths_for_filesystem(paths, filesystem)
        paths, resolved_fs = resolve_paths_and_filesystem(
            paths,
            filesystem,
        )
        # find all files in manifests, prefixes, and folders
        content_type_to_paths, meta_provider = _expand_s3_paths_by_content_type(
            paths,
            urls,
            content_type_provider,
            path_type,
            filesystem,
            resolved_fs,
            **s3_client_kwargs,
        )
        num_content_types = len(content_type_to_paths)
        if num_content_types > 1 and not schema:
            # infer schema from a single parquet file
            # TODO (pdames): read verbose manifest schema if available, and infer
            #  schema from a sample parquet dataset if not
            path = content_type_to_paths[ContentType.PARQUET][0]
            with resolved_fs.open_input_file(path, **open_stream_args) as f:
                schema = pq.read_schema(f)
        content_type_to_reader = {
            ContentType.PARQUET: ParquetDatasource(),
            ContentType.CSV: CSVDatasource(),
        }
        all_read_tasks = []
        for content_type, paths in content_type_to_paths.items():
            reader = content_type_to_reader.get(content_type)
            assert reader, f"No datasource found for: {content_type}"
            prepare_read_kwargs = {
                "parallelism": parallelism,
                "paths": paths,
                "filesystem": resolved_fs,
                "schema": schema,
                "meta_provider": meta_provider,
                "partitioning": partitioning,
            }
            if content_type == ContentType.PARQUET:
                if columns:
                    prepare_read_kwargs["columns"] = columns
            elif content_type in DELIMITED_TEXT_CONTENT_TYPES:
                prepare_read_kwargs.update(
                    csv_reader_config.to_arrow_reader_kwargs(columns, schema)
                )
            else:
                raise NotImplementedError(f"Unsupported content type: {content_type}")
            # merge any provided reader kwargs for this content type with those
            # inferred from CSV Reader Config
            if read_kwargs_provider:
                prepare_read_kwargs = read_kwargs_provider(
                    content_type,
                    prepare_read_kwargs,
                )
            # explicitly specified `open_stream_args` override those inferred
            # from CSV Reader Config
            if open_stream_args:
                prepare_read_kwargs["open_stream_args"] = open_stream_args
            read_tasks = reader.prepare_read(**prepare_read_kwargs)
            all_read_tasks.extend(read_tasks)
        return all_read_tasks
