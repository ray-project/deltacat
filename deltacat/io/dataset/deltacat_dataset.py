# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, cast

import pyarrow as pa
from ray.data import Dataset

from deltacat.utils.url import DeltaCatUrl
from deltacat.io.datasink.deltacat_datasink import DeltaCatDatasink


class DeltaCatDataset(Dataset):
    @staticmethod
    def from_dataset(dataset: Dataset) -> DeltaCatDataset:
        # cast to DeltacatDataset in-place since it only adds new methods
        dataset.__class__ = DeltaCatDataset
        return cast(DeltaCatDataset, dataset)

    def write_deltacat(
        self,
        url: DeltaCatUrl,
        *,
        # if the source dataset only contains DeltaCAT metadata, then only copy the metadata to the destination... if it contains external source file paths, then register them in a new Delta.
        metadata_only: bool = False,
        # merge all deltas as part of the write operation
        copy_on_write: Optional[bool] = False,
        filesystem: Optional[pa.fs.S3FileSystem] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        arrow_parquet_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        min_rows_per_file: Optional[int] = None,
        ray_remote_args: Dict[str, Any] = None,
        concurrency: Optional[int] = None,
        **arrow_parquet_args,
    ) -> None:
        """Writes the dataset to files and commits DeltaCAT metadata indexing
        the files written.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.{extension}, where ``uuid`` is a
        unique id for the dataset.

        The DeltaCAT manifest will be written to ``f"{path}/manifest``

        Examples:
            >>> ds.write_deltacat("s3://catalog/root/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            url: The path to the root directory where materialized files and
                DeltaCAT manifest will be written.
            filesystem: The filesystem implementation to write to. This should
                be either a PyArrow S3FileSystem.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.S3FileSystem.open_output_stream
            filename_provider: FilenameProvider implementation
                to write each dataset block to a custom output path.
            arrow_parquet_args_fn: Callable that returns a dictionary of write
                arguments to use when writing each block to a file. Overrides
                any duplicate keys from arrow_parquet_args. This should be used
                instead of arrow_parquet_args if any of your write arguments
                cannot be pickled, or if you'd like to lazily resolve the write
                arguments for each dataset block.
            arrow_parquet_args: Options to pass to
                pyarrow.parquet.write_table(), which is used to write out each
                block to a file.
        """
        datasink = DeltaCatDatasink(
            url,
            metadata_only=metadata_only,
            copy_on_write=copy_on_write,
            arrow_parquet_args_fn=arrow_parquet_args_fn,
            arrow_parquet_args=arrow_parquet_args,
            min_rows_per_file=min_rows_per_file,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=arrow_open_stream_args,
            dataset_uuid=self._uuid,
        )
        self.write_datasink(
            datasink,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )
