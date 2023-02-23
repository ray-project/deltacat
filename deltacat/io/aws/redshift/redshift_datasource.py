import json
import logging
from collections import OrderedDict, defaultdict
from enum import Enum
from errno import ENOENT
from os import strerror
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pyarrow as pa
import ray
import s3fs
from pyarrow import parquet as pq
from pyarrow.fs import FileSystem, FileType, S3FileSystem
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import (
    BlockWritePathProvider,
    CSVDatasource,
    DefaultBlockWritePathProvider,
    DefaultFileMetadataProvider,
    ParquetBaseDatasource,
    ParquetMetadataProvider,
    PathPartitionParser,
)
from ray.data.datasource.datasource import ArrowRow, Datasource, ReadTask, WriteResult
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.file_meta_provider import FastFileMetadataProvider
from ray.types import ObjectRef

from deltacat import ContentEncoding, ContentType, logs
from deltacat.aws.redshift.model.manifest import (
    Manifest,
    ManifestEntry,
    ManifestEntryList,
    ManifestMeta,
)
from deltacat.aws.s3u import (
    S3Url,
    filter_objects_by_prefix,
    objects_to_paths,
    parse_s3_url,
)
from deltacat.types.media import DELIMITED_TEXT_CONTENT_TYPES
from deltacat.utils.common import ReadKwargsProvider

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CapturingBlockWritePathProvider(BlockWritePathProvider):
    """Delegating block write path provider that saves an ordered dictionary of
    input keyword arguments for every block write path returned."""

    def __init__(self, block_write_path_provider: BlockWritePathProvider):
        self.block_write_path_provider = block_write_path_provider
        self.write_path_kwargs: Dict[str, Dict[str, Any]] = OrderedDict()

    def _get_write_path_for_block(self, base_path: str, *args, **kwargs) -> str:
        write_path = self.block_write_path_provider(
            base_path,
            *args,
            **kwargs,
        )
        kwargs["base_path"] = base_path
        self.write_path_kwargs[write_path] = kwargs
        return write_path


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


class RedshiftUnloadTextArgs:
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
                    f"Multiple Redshift UNLOAD compression types specified "
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
                "Redshift text files unloaded with FIXEDWIDTH are not "
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


class S3PathType(str, Enum):
    MANIFEST = "manifest"
    PREFIX = "prefix"
    FILES_AND_FOLDERS = "files_and_folders"


class RedshiftWriteResult:
    def __init__(self):
        self.metadata = None
        self.path = None
        self.dataset_uuid = None
        self.block_write_path_provider = None
        self.content_type = None
        self.content_encoding = None
        self.filesystem = None


def _normalize_s3_paths_for_filesystem(
    paths: Union[str, List[str]],
    filesystem: Union[S3FileSystem, s3fs.S3FileSystem],
) -> Tuple[List[str], List[S3Url]]:
    if isinstance(paths, str):
        paths = [paths]
    urls = [parse_s3_url(url) for url in paths]
    if isinstance(filesystem, FileSystem):
        # pyarrow.fs.FileSystem paths should not start with "s3://"
        # pyarrow.fs.FileSystem paths should not end with "/"
        paths = [f"{u.bucket}/{u.key}".rstrip("/") for u in urls]
    else:
        # s3fs.S3FileSystem can start with "s3://" (presumably others can too)
        paths = [u.url.rstrip("/") for u in urls]
    return paths, urls


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


def _expand_manifest_paths(
    paths: List[str],
    filesystem: Optional[Union[S3FileSystem, s3fs.S3FileSystem]],
    content_type_provider: Callable[[str], ContentType],
) -> Tuple[Dict[ContentType, List[str]], CachedFileMetadataProvider]:
    assert len(paths) == 1, f"Expected 1 manifest path, found {len(paths)}."
    path = paths[0]
    with filesystem.open_input_file(path) as f:
        manifest = Manifest(json.loads(f.read()))
    content_type_to_paths = {}
    meta_provider = CachedFileMetadataProvider({})
    if not manifest.entries:
        logger.warning(f"No entries to read in Redshift Manifest: {path}")
    else:
        content_type_to_paths, meta_provider = _read_manifest_entry_paths(
            manifest.entries,
            manifest.meta.content_type if manifest.meta else None,
            content_type_provider,
        )
    # TODO(pdames): infer the schema from a verbose manifest if available?
    # if not schema and ContentType.PARQUET not in content_type_to_paths:
    #     schema = _infer_schema_from_manifest(manifest)
    return content_type_to_paths, meta_provider


def _infer_content_types_from_paths(
    paths: List[str],
    content_type_provider: Callable[[str], ContentType],
) -> Dict[ContentType, List[str]]:
    content_type_to_paths = defaultdict(list)
    for path in paths:
        if not path.endswith("/"):
            content_type_to_paths[content_type_provider(path)].append(path)
    return content_type_to_paths


def _expand_prefix_paths(
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


def _expand_paths_by_content_type(
    base_paths: Union[str, List[str]],
    base_urls: List[S3Url],
    content_type_provider: Callable[[str], ContentType],
    path_type: S3PathType,
    user_fs: Optional[Union[S3FileSystem, s3fs.S3FileSystem]],
    resolved_fs: S3FileSystem,
    **s3_client_kwargs,
) -> Tuple[Dict[ContentType, List[str]], CachedFileMetadataProvider]:
    if path_type == S3PathType.MANIFEST:
        content_type_to_paths, meta_provider = _expand_manifest_paths(
            base_paths,
            resolved_fs,
            content_type_provider,
        )
    elif path_type == S3PathType.PREFIX:
        content_type_to_paths, meta_provider = _expand_prefix_paths(
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
        paths, urls = _normalize_s3_paths_for_filesystem(
            paths,
            user_fs,
        )
        content_type_to_paths[content_type] = paths
    # normalize block metadata provider S3 file paths based on the filesystem
    meta_provider = CachedFileMetadataProvider(
        {
            _normalize_s3_paths_for_filesystem(path, user_fs)[0][0]: metadata
            for path, metadata in meta_provider.get_meta_cache().items()
        }
    )
    return content_type_to_paths, meta_provider


class RedshiftDatasource(Datasource[Union[ArrowRow, Any]]):
    def prepare_read(
        self,
        parallelism: int,
        paths: Union[str, List[str]],
        content_type_provider: Callable[[str], ContentType],
        path_type: S3PathType = S3PathType.MANIFEST,
        filesystem: Optional[Union[S3FileSystem, s3fs.S3FileSystem]] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[pa.Schema] = None,
        unload_args: RedshiftUnloadTextArgs = RedshiftUnloadTextArgs(),
        partitioning: HivePartitionParser = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        read_kwargs_provider: Optional[ReadKwargsProvider] = None,
        **s3_client_kwargs,
    ) -> List[ReadTask]:
        # default to pyarrow.fs.S3FileSystem if no filesystem given
        if filesystem is None:
            filesystem = S3FileSystem()
        # normalize s3 paths to work with the filesystem provided
        paths, urls = _normalize_s3_paths_for_filesystem(paths, filesystem)
        paths, resolved_fs = _resolve_paths_and_filesystem(
            paths,
            filesystem,
        )
        # find all files in manifests, prefixes, and folders
        content_type_to_paths, meta_provider = _expand_paths_by_content_type(
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
            ContentType.PARQUET: ParquetBaseDatasource(),
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
                    unload_args.to_arrow_reader_kwargs(columns, schema)
                )
            else:
                raise NotImplementedError(f"Unsupported content type: {content_type}")
            # merge any provided reader kwargs for this content type with those
            # inferred from Redshift UNLOAD args
            if read_kwargs_provider:
                prepare_read_kwargs = read_kwargs_provider(
                    content_type,
                    prepare_read_kwargs,
                )
            # explicitly specified `open_stream_args` override those inferred
            # from Redshift UNLOAD args
            if open_stream_args:
                prepare_read_kwargs["open_stream_args"] = open_stream_args
            read_tasks = reader.prepare_read(**prepare_read_kwargs)
            all_read_tasks.extend(read_tasks)
        return all_read_tasks

    def do_write(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        path: str,
        dataset_uuid: str,
        filesystem: Optional[FileSystem] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        write_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        _block_udf: Optional[Callable[[Block], Block]] = None,
        **write_args,
    ) -> List[ObjectRef[WriteResult]]:
        if filesystem is None:
            filesystem = S3FileSystem()
        paths, _ = _normalize_s3_paths_for_filesystem(path, filesystem)
        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        assert len(paths) == 1, f"Expected 1 write path, found {len(paths)}."
        path = paths[0]
        block_path_provider = CapturingBlockWritePathProvider(block_path_provider)
        writer = ParquetBaseDatasource()
        write_results = writer.do_write(
            blocks,
            metadata,
            path,
            dataset_uuid,
            filesystem,
            try_create_dir,
            open_stream_args,
            block_path_provider,
            write_args_fn,
            _block_udf,
            **write_args,
        )
        # append a summary of this write operation in the last write result
        rwr = RedshiftWriteResult()
        rwr.metadata = metadata
        rwr.path = path
        rwr.dataset_uuid = dataset_uuid
        rwr.block_write_path_provider = block_path_provider
        rwr.content_type = ContentType.PARQUET.value
        rwr.content_encoding = ContentEncoding.IDENTITY.value
        rwr.filesystem = filesystem
        rwr_obj_ref = ray.put(rwr)
        write_results.append(rwr_obj_ref)
        return write_results

    def on_write_complete(self, write_results: List[WriteResult], **kwargs) -> None:
        # TODO (pdames): time latency of this operation - overall redshift write times
        #  are 2-3x pure read_parquet_fast() times
        # restore the write operation summary from the last write result
        result: RedshiftWriteResult = write_results[len(write_results) - 1]
        write_path_args = result.block_write_path_provider.write_path_kwargs
        blocks_written = len(write_path_args)
        expected_blocks_written = len(result.metadata)
        # TODO(pdames): Corner cases where mismatch is expected? Emply blocks?
        #  Blocks filtered/split/merged to more/less write paths?
        assert blocks_written == expected_blocks_written, (
            f"Dataset write result validation failed. Found "
            f"{blocks_written}/{expected_blocks_written} Dataset blocks "
            f"written. Refusing to commit Redshift Manifest."
        )
        manifest_entries = ManifestEntryList()
        for block_idx, path in enumerate(write_path_args.keys()):
            file_info = result.filesystem.get_file_info(path)
            if file_info.type == FileType.File:
                content_length = file_info.size
            else:
                raise FileNotFoundError(ENOENT, strerror(ENOENT), path)
            num_rows = result.metadata[block_idx].num_rows
            source_content_length = result.metadata[block_idx].size_bytes
            manifest_entry_meta = ManifestMeta.of(
                int(num_rows) if num_rows is not None else None,
                int(content_length) if content_length is not None else None,
                result.content_type,
                result.content_encoding,
                int(source_content_length) if source_content_length else None,
            )
            parsed_url = parse_s3_url(path)
            manifest_entry = ManifestEntry.of(
                parsed_url.url,
                manifest_entry_meta,
            )
            manifest_entries.append(manifest_entry)
        manifest = Manifest.of(manifest_entries)
        manifest_path = f"{result.path}/manifest"
        logger.debug(f"Write succeeded for Dataset ID: {result.dataset_uuid}")
        with result.filesystem.open_output_stream(
            manifest_path,
            # Also See:
            # docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonRequestHeaders.html
            # Arrow s3fs.cc: tinyurl.com/2axa6m9m
            metadata={"Content-Type": ContentType.JSON.value},
        ) as f:
            f.write(json.dumps(manifest).encode("utf-8"))
        logger.debug(f"Manifest committed to: {manifest_path}")
