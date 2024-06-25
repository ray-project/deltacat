# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, TypeVar, Union, cast

import pyarrow as pa
import s3fs
from ray.data import Dataset

T = TypeVar("T")


class DeltacatDataset(Dataset[T]):
    @staticmethod
    def from_dataset(dataset: Dataset[T]) -> DeltacatDataset[T]:
        # cast to DeltacatDataset in-place since it only adds new methods
        dataset.__class__ = DeltacatDataset
        return cast(DeltacatDataset[T], dataset)

    def write_redshift(
        self,
        path: str,
        *,
        filesystem: Optional[Union[pa.fs.FileSystem, s3fs.S3FileSystem]] = None,
        try_create_dir: bool = True,
        arrow_open_stream_args: Optional[Dict[str, Any]] = None,
        arrow_parquet_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **arrow_parquet_args,
    ) -> None:
        """Writes the dataset to Parquet files and commits a Redshift manifest
        back to S3 indexing the files written. The output can be loaded into
        Redshift by providing it to the Redshift COPY command, or via AWS Data
        Wrangler's `wr.redshift.copy_from_files()` API.

        This is only supported for datasets convertible to Arrow records.
        To control the number of files, use ``.repartition()``.

        Unless a custom block path provider is given, the format of the output
        files will be {uuid}_{block_idx}.parquet, where ``uuid`` is an unique
        id for the dataset.

        The Redshift manifest will be written to ``f"{path}/manifest``

        Examples:
            >>> ds.write_redshift("s3://bucket/path")

        Time complexity: O(dataset size / parallelism)

        Args:
            path: The path to the destination root directory where Parquet
                files and the Redshift manifest will be written to.
            filesystem: The filesystem implementation to write to. This should
                be either PyArrow's S3FileSystem or s3fs.
            try_create_dir: Try to create all directories in destination path
                if True. Does nothing if all directories already exist.
            arrow_open_stream_args: kwargs passed to
                pyarrow.fs.FileSystem.open_output_stream
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
        raise NotImplementedError(
            "Writing to Redshift is not yet supported. "
            "Please use DeltacatDataset.write_parquet() instead."
        )
