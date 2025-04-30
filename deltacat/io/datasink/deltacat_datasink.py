import logging

from collections import OrderedDict
from typing import Dict, Any, Optional, List, Iterable

from ray.data import Datasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource import WriteResult

from ray.data.datasource.filename_provider import (
    FilenameProvider,
)

from deltacat import logs

from deltacat.constants import METAFILE_FORMAT_MSGPACK
from deltacat.storage import Metafile
from deltacat.io.datasource.deltacat_datasource import (
    METAFILE_DATA_COLUMN_NAME,
    METAFILE_TYPE_COLUMN_NAME,
)
from deltacat.utils.url import DeltaCatUrl, DeltaCatUrlWriter

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


class DeltaCatWriteResult:
    def __init__(self):
        self.metadata = None
        self.path = None
        self.dataset_uuid = None
        self.block_write_path_provider = None
        self.content_type = None
        self.content_encoding = None
        self.filesystem = None


class DeltaCatDatasink(Datasink[List[Metafile]]):
    def __init__(
        self,
        url: DeltaCatUrl,
        *,
        metadata_only: bool = False,
        copy_on_write: Optional[bool] = False,
    ):
        self._url = url
        self._metadata_only = metadata_only
        self._copy_on_write = copy_on_write

    def on_write_start(self) -> None:
        pass

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List[Metafile]:
        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if (
                METAFILE_DATA_COLUMN_NAME in pa_table.column_names
                and METAFILE_TYPE_COLUMN_NAME in pa_table.column_names
            ):
                for pa_scalar in pa_table[METAFILE_DATA_COLUMN_NAME]:
                    metafile_msgpack_bytes = pa_scalar.as_py()
                    metafile = Metafile.deserialize(
                        serialized=metafile_msgpack_bytes,
                        meta_format=METAFILE_FORMAT_MSGPACK,
                    )
                    # TODO(pdames): Add `metafile` to writer as a kwarg instead
                    #  of constructing a new URL with the metafile as input.
                    writer_url = DeltaCatUrlWriter(self._url, metafile=metafile)
                    # TODO(pdames): Run writes in order from catalog -> delta
                    #  by truncating the URL down to just dc://{catalog-name}
                    #  and rebuilding all path elements from there.
                    writer_url.write(metafile)
            else:
                raise NotImplementedError(
                    f"Expected {METAFILE_DATA_COLUMN_NAME} and "
                    f"{METAFILE_TYPE_COLUMN_NAME} columns in the input block, "
                    f"but found {pa_table.column_names}."
                )

    def on_write_complete(
        self,
        write_result: WriteResult[List[Metafile]],
    ):
        pass


"""
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
"""
