import pyarrow as pa
import logging

from pyarrow import parquet as pq
from pyarrow.fs import FileSystem
from ray.data import ReadTask

from ray.data.block import BlockAccessor, Block
from ray.data.context import DatasetContext
from ray.data.datasource import FileBasedDatasource
from ray.data.datasource.file_based_datasource import _resolve_kwargs, \
    _resolve_paths_and_filesystem, _wrap_s3_serialization_workaround, \
    _S3FileSystemWrapper, BaseFileMetadataProvider, DefaultFileMetadataProvider
from ray.data.impl.output_buffer import BlockOutputBuffer

from deltacat import logs

from typing import Dict, Any, Callable, Union, List, Optional, Iterable

from ray.data.impl.util import _check_pyarrow_version

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class ParquetDatasource(FileBasedDatasource):
    """Parquet datasource, for reading and writing Parquet files. The primary
    difference from ray.data.datasource.ParquetDatasource is that this does not
    use PyArrow's `ParquetDataset` abstraction for Dataset reads."""

    def prepare_read(
            self,
            parallelism: int,
            paths: Union[str, List[str]],
            filesystem: Optional[FileSystem] = None,
            schema: Optional[Union[type, pa.Schema]] = None,
            open_stream_args: Optional[Dict[str, Any]] = None,
            meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
            _block_udf: Optional[Callable[[Block], Block]] = None,
            **reader_args) -> List[ReadTask]:
        """Overrides FileBasedDatasource `prepare_read`.

        Why? `open_input_file` is required for random access reads of Parquet
        files instead of the `open_input_stream` method used by default.
        """
        _check_pyarrow_version()
        import numpy as np

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        paths, file_sizes = meta_provider.expand_paths(paths, filesystem)

        read_stream = self._read_stream

        filesystem = _wrap_s3_serialization_workaround(filesystem)

        def read_files(
            read_paths: List[str],
            fs: Union[FileSystem, _S3FileSystemWrapper],
        ) -> Iterable[Block]:
            logger.debug(f"Reading {len(read_paths)} files.")
            if isinstance(fs, _S3FileSystemWrapper):
                fs = fs.unwrap()
            ctx = DatasetContext.get_current()
            output_buffer = BlockOutputBuffer(
                block_udf=_block_udf,
                target_max_block_size=ctx.target_max_block_size
            )
            for read_path in read_paths:
                with fs.open_input_file(read_path) as f:
                    for data in read_stream(f, read_path, **reader_args):
                        output_buffer.add_block(data)
                        if output_buffer.has_next():
                            yield output_buffer.next()
            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        read_tasks = []
        for read_paths, file_sizes in zip(
            np.array_split(paths, parallelism),
            np.array_split(file_sizes, parallelism)
        ):
            if len(read_paths) <= 0:
                continue

            meta = meta_provider(
                read_paths,
                schema,
                rows_per_file=self._rows_per_file(),
                file_sizes=file_sizes,
            )
            read_task = ReadTask(
                lambda read_paths=read_paths: read_files(read_paths, filesystem), meta
            )
            read_tasks.append(read_task)

        return read_tasks

    def _read_file(self, f: pa.NativeFile, path: str, **reader_args):
        use_threads = reader_args.pop("use_threads", False)
        return pq.read_table(f, use_threads=use_threads, **reader_args)

    def _write_block(self,
                     f: pa.NativeFile,
                     block: BlockAccessor,
                     writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
                     **writer_args):
        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)
        pq.write_table(block.to_arrow(), f, **writer_args)

    def _file_format(self) -> str:
        return "parquet"
