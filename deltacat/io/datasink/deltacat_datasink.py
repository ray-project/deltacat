import json
import logging

from collections import OrderedDict
from errno import ENOENT
from os import strerror
from typing import Callable, Dict, Any, Optional, Iterable, List

import pyarrow

import ray

from ray.data._internal.datasource.parquet_datasink import (
    ParquetDatasink as _ParquetDatasink,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import (
    Block,
    BlockAccessor,
)
from ray.data.datasource.filename_provider import (
    FilenameProvider,
    _DefaultFilenameProvider,
)
from ray.types import ObjectRef

from deltacat.aws.s3u import parse_s3_url
from deltacat.types.media import (
    ContentType,
    ContentEncoding,
)
from deltacat.storage import (
    ManifestEntryList,
    ManifestMeta,
    ManifestEntry,
    Manifest,
)
from deltacat.utils.filesystem import resolve_paths_and_filesystem
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CapturingBlockWritePathProvider(FilenameProvider):
    """Delegating block write path provider that saves an ordered dictionary of
    input keyword arguments for every block write path returned."""

    def __init__(
        self,
        block_write_path_provider: FilenameProvider,
        base_path: Optional[str] = None,
    ):
        self.base_path = base_path
        self.block_write_path_provider = block_write_path_provider
        self.write_path_kwargs: Dict[str, Dict[str, Any]] = OrderedDict()

    def get_filename_for_block(
        self,
        block: Any,
        task_index: int,
        block_index: int,
    ) -> str:
        if self.base_path is None:
            raise ValueError(
                "Base path must be provided to CapturingBlockWritePathProvider",
            )
        return self._get_write_path_for_block(
            base_path=self.base_path,
            block=block,
            block_index=block_index,
        )

    def _get_write_path_for_block(
        self,
        base_path: str,
        *args,
        **kwargs,
    ) -> str:
        filename = self.block_write_path_provider.get_filename_for_block(
            *args,
            **kwargs,
        )
        write_path = f"{base_path}/{filename}"
        kwargs["base_path"] = base_path
        self.write_path_kwargs[write_path] = kwargs
        return write_path


class DeltacatWriteResult:
    def __init__(self):
        self.metadata = None
        self.path = None
        self.dataset_uuid = None
        self.block_write_path_provider = None
        self.content_type = None
        self.content_encoding = None
        self.filesystem = None


class DeltacatDatasink(_ParquetDatasink):
    def __init__(
        self,
        path: str,
        *,
        arrow_parquet_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        arrow_parquet_args: Optional[Dict[str, Any]] = None,
        num_rows_per_file: Optional[int] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = _DefaultFilenameProvider(),
        dataset_uuid: Optional[str] = None,
    ):
        super().__init__(
            path,
            arrow_parquet_args_fn=arrow_parquet_args_fn,
            arrow_parquet_args=arrow_parquet_args,
            num_rows_per_file=num_rows_per_file,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=CapturingBlockWritePathProvider(filename_provider),
            dataset_uuid=dataset_uuid,
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List[ObjectRef[DeltacatWriteResult]]:
        paths, filesystem = resolve_paths_and_filesystem(
            self.path,
            self.filesystem,
        )
        assert len(paths) == 1, f"Expected 1 write path, found {len(paths)}."
        path = paths[0]
        write_results = super().write(blocks)
        # append a summary of this write operation in the last write result
        metadata = [BlockAccessor.for_block(_).get_metadata() for _ in blocks]
        rwr = DeltacatWriteResult()
        rwr.metadata = metadata
        rwr.path = path
        rwr.dataset_uuid = self.dataset_uuid
        rwr.block_write_path_provider = self.filename_provider
        rwr.content_type = ContentType.PARQUET.value
        rwr.content_encoding = ContentEncoding.IDENTITY.value
        rwr.filesystem = filesystem
        rwr_obj_ref = ray.put(rwr)
        write_results.append(rwr_obj_ref)
        return write_results

    def on_write_complete(self, write_results: List[Any], **kwargs) -> None:
        # TODO (pdames): time latency of this operation - overall s3 write times
        #  are 2-3x pure read_parquet_fast() times
        # restore the write operation summary from the last write result
        result: DeltacatWriteResult = write_results[len(write_results) - 1]
        write_path_args = result.block_write_path_provider.write_path_kwargs
        blocks_written = len(write_path_args)
        expected_blocks_written = len(result.metadata)
        # TODO(pdames): Corner cases where mismatch is expected? Emply blocks?
        #  Blocks filtered/split/merged to more/less write paths?
        assert blocks_written == expected_blocks_written, (
            f"Dataset write result validation failed. Found "
            f"{blocks_written}/{expected_blocks_written} Dataset blocks "
            f"written. Refusing to commit DeltaCAT Manifest."
        )
        manifest_entries = ManifestEntryList()
        for block_idx, path in enumerate(write_path_args.keys()):
            file_info = result.filesystem.get_file_info(path)
            if file_info.type == pyarrow.fs.FileType.File:
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
        with result.filesystem.open_output_stream(manifest_path) as f:
            f.write(json.dumps(manifest).encode("utf-8"))
        logger.debug(f"Manifest committed to: {manifest_path}")
