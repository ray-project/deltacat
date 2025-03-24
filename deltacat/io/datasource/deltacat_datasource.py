import logging
import json

from collections import defaultdict
from enum import Enum
from typing import Union, List, Callable, Optional, Dict, Any, Tuple

import msgpack
import pyarrow as pa
import pyarrow.fs
from pyarrow import parquet as pq
from pyarrow.fs import S3FileSystem

from ray.data import (
    Datasource,
    ReadTask,
)
from ray.data._internal.datasource.binary_datasource import BinaryDatasource
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data.block import BlockMetadata
from ray.data.datasource import (
    DefaultFileMetadataProvider,
    PathPartitionParser,
    FastFileMetadataProvider,
    ParquetMetadataProvider,
)

from constants import METAFILE_FORMAT, METAFILE_FORMAT_JSON
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
    Delta,
    Manifest,
    ManifestEntryList,
)
from deltacat.utils.common import ReadKwargsProvider
from deltacat.utils.filesystem import resolve_paths_and_filesystem
from deltacat.utils.url import DeltacatUrl
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class DeltacatReadType(str, Enum):
    METADATA = "metadata"
    METADATA_RECURSIVE = "metadata_recursive"
    DATA = "data"


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


class PathType(str, Enum):
    MANIFEST = "manifest"
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


def _infer_content_types_from_paths(
    paths: List[str],
    content_type_provider: Callable[[str], ContentType],
) -> Dict[ContentType, List[str]]:
    content_type_to_paths = defaultdict(list)
    for path in paths:
        if not path.endswith("/"):
            content_type_to_paths[content_type_provider(path)].append(path)
    return content_type_to_paths


def _expand_manifest_paths_by_content_type(
    manifest: Manifest,
) -> Tuple[Dict[ContentType, List[str]], CachedFileMetadataProvider]:
    content_type_to_paths = {}
    meta_provider = CachedFileMetadataProvider({})
    if not manifest.entries:
        logger.warning(f"No entries to read in DeltaCAT Manifest: {manifest_path}")
    else:
        content_type_to_paths, meta_provider = _read_manifest_entry_paths(
            manifest.entries,
            manifest.meta.content_type if manifest.meta else None,
        )
    # TODO(pdames): infer the schema from a manifest if available?
    # if not schema and ContentType.PARQUET not in content_type_to_paths:
    #     schema = _infer_schema_from_manifest(manifest)
    return content_type_to_paths, meta_provider


def _read_manifest_entry_paths(
    entries: ManifestEntryList,
    manifest_content_type: Optional[str],
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
            # TODO(pdames): fall back to content type inference by file extension
            raise ValueError(
                f"Manifest entry missing content type: {e}. "
                f"Please specify a content type for each manifest entry."
            )
        meta_cache[url] = BlockMetadata(
            num_rows=e.meta.record_count,
            size_bytes=e.meta.content_length,
            schema=None,
            input_files=[],
            exec_stats=None,
        )
    return content_type_to_paths, CachedFileMetadataProvider(meta_cache)


class DeltacatDatasource(Datasource):
    """Datasource for reading registered DeltaCAT catalog objects."""

    def __init__(
        self,
        url: DeltacatUrl,
        deltacat_read_type: DeltacatReadType = DeltacatReadType.DATA,
        timestamp_as_of: Optional[int] = None,
        merge_on_read: Optional[bool] = False,
        read_kwargs_provider: Optional[ReadKwargsProvider] = None,
    ):
        self._url = url
        self._deltacat_read_type = deltacat_read_type
        self._timestamp_as_of = timestamp_as_of
        self._merge_on_read = merge_on_read
        self._filesystem = url.catalog.filesystem
        self._read_kwargs_provider = read_kwargs_provider

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown.

        Note that the in-memory data size may be larger than the on-disk data size.
        """
        raise NotImplementedError

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        kwargs = self._read_kwargs_provider(self._url.datasource_type)
        if self._deltacat_read_type == DeltacatReadType.METADATA:
            # do a shallow read of the top-level DeltaCAT metadata
            metafile = self._url.reader(**kwargs)
            read_tasks = self._metafile_to_read_task(metafile)
        elif self._deltacat_read_type == DeltacatReadType.METADATA_RECURSIVE:
            list_results = self._list_all_metafiles(**kwargs)
            read_tasks = self._metafile_list_results_to_read_tasks(list_results)
        elif self._deltacat_read_type == DeltacatReadType.DATA:
            # do a deep read across all in-scope Delta manifest file paths
            # recursive is implicitly true for deep data reads
            list_results = self._list_all_metafiles(**kwargs)
            deltas: List[Delta] = list_results[len(list_results) - 1]
            read_tasks = []
            for delta in deltas:
                read_tasks.append(
                    self._get_delta_manifest_read_tasks(
                        delta.manifest,
                        parallelism,
                    ),
                )
        else:
            raise NotImplementedError(
                f"Unsupported DeltaCAT read type: {self._deltacat_read_type}"
            )

        return read_tasks

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        data = f.readall()

        builder = ArrowBlockBuilder()
        item = {self._COLUMN_NAME: data}
        builder.add(item)
        yield builder.build()

    def _get_read_task(self) -> Iterable[Block]:
        return BinaryDatasource().get_read_tasks()

    def _list_all_metafiles_read_tasks(self, **kwargs):
        read_tasks = []
        read_tasks.append(
            ReadTask(
                read_fn=lambda tasks=chunk_tasks: get_read_task(tasks),
                metadata=metadata,
            )
        )

    def _list_all_metafiles(self, **kwargs):
        list_results = []
        lister = self._url.listers.pop(0)[0]
        # the top-level lister doesn't have any missing keyword args
        metafiles = lister(**kwargs)
        list_results.append(metafiles)
        for lister, kwarg_name, kwarg_val_resolver_fn in self._url.listers:
            # each subsequent lister needs to inject missing keyword args from the parent metafile
            for metafile in metafiles:
                kwargs.update(
                    {kwarg_name: kwarg_val_resolver_fn(metafile)}
                    if kwarg_name and kwarg_val_resolver_fn
                    else {}
                )
                metafiles = lister(**kwargs)
                list_results.append(metafiles)

    def _get_delta_manifest_read_tasks(
        self,
        delta_manifest: Manifest,
        parallelism: int,
    ) -> List[ReadTask]:
        # find all files in the Delta manifest
        content_type_to_paths, meta_provider = _expand_manifest_paths_by_content_type(
            delta_manifest,
            self._filesystem,
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
                "filesystem": self._filesystem,
                "schema": schema,
                "meta_provider": meta_provider,
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

    def prepare_read(
        self,
        parallelism: int,
        paths: Union[str, List[str]],
        content_type_provider: Callable[[str], ContentType],
        path_type: PathType = PathType.MANIFEST,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[pa.Schema] = None,
        csv_reader_config: DelimitedTextReaderConfig = DelimitedTextReaderConfig(),
        partitioning: HivePartitionParser = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        read_kwargs_provider: Optional[ReadKwargsProvider] = None,
        **s3_client_kwargs,
    ) -> List[ReadTask]:
        pass
