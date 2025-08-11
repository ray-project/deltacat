import logging
from typing import Callable, Dict, List, Optional, Union

from fsspec import AbstractFileSystem

import pyarrow as pa
from pyarrow import csv as pacsv
import pyarrow.fs as pafs

from ray.data import Dataset
from ray.data.datasource import FilenameProvider

from deltacat import logs
from deltacat.types.media import ContentEncoding, ContentType

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def write_parquet(
    dataset: Dataset,
    base_path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[Callable, FilenameProvider],
    **kwargs,
) -> None:

    dataset.write_parquet(
        base_path,
        filesystem=filesystem,
        try_create_dir=False,
        filename_provider=block_path_provider,
        **kwargs,
    )


def write_csv(
    dataset: Dataset,
    base_path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[Callable, FilenameProvider],
    **kwargs,
) -> None:
    """
    Write a Ray Dataset to a CSV file (or other delimited text format).
    """
    # Extract CSV-specific options from kwargs
    delimiter = kwargs.pop("delimiter", ",")
    quoting_style = kwargs.pop("quoting_style", None)
    include_header = kwargs.pop("include_header", False)

    # Create a function that will generate WriteOptions inside the worker process
    def arrow_csv_args_fn():
        write_options = pacsv.WriteOptions(
            delimiter=delimiter,
            include_header=include_header,
            quoting_style=quoting_style,
        )
        return {"write_options": write_options}

    # Check if the block_path_provider will generate .gz files to avoid double compression
    pa_open_stream_args = {}
    if not (
        hasattr(block_path_provider, "content_encoding")
        and block_path_provider.content_encoding == ContentEncoding.GZIP
    ):
        # Block path provider will not generate .gz files, so we need to apply explicit compression
        pa_open_stream_args["compression"] = ContentEncoding.GZIP.value

    dataset.write_csv(
        base_path,
        arrow_open_stream_args=pa_open_stream_args,
        filesystem=filesystem,
        try_create_dir=False,
        filename_provider=block_path_provider,
        arrow_csv_args_fn=arrow_csv_args_fn,
        **kwargs,
    )


def write_json(
    dataset: Dataset,
    base_path: str,
    *,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[Callable, FilenameProvider],
    **kwargs,
) -> None:
    """
    Write a Ray Dataset to a JSON file using Ray's native JSON writer.
    """
    # Check if the block_path_provider will generate .gz files to avoid double compression
    pa_open_stream_args = {}
    if not (
        hasattr(block_path_provider, "content_encoding")
        and block_path_provider.content_encoding == ContentEncoding.GZIP
    ):
        # Block path provider will not generate .gz files, so we need to apply explicit compression
        pa_open_stream_args["compression"] = ContentEncoding.GZIP.value

    dataset.write_json(
        base_path,
        arrow_open_stream_args=pa_open_stream_args,
        filesystem=filesystem,
        try_create_dir=False,
        filename_provider=block_path_provider,
        **kwargs,
    )


CONTENT_TYPE_TO_DATASET_WRITE_FUNC: Dict[str, Callable] = {
    ContentType.UNESCAPED_TSV.value: write_csv,
    ContentType.TSV.value: write_csv,
    ContentType.CSV.value: write_csv,
    ContentType.PSV.value: write_csv,
    ContentType.PARQUET.value: write_parquet,
    ContentType.JSON.value: write_json,
}


def content_type_to_writer_kwargs(content_type: str) -> Dict[str, any]:
    """
    Returns writer kwargs for the given content type when writing with Ray Dataset.
    """
    if content_type == ContentType.UNESCAPED_TSV.value:
        return {
            "delimiter": "\t",
            "include_header": False,
            "quoting_style": "none",
        }
    if content_type == ContentType.TSV.value:
        return {
            "delimiter": "\t",
            "include_header": False,
        }
    if content_type == ContentType.CSV.value:
        return {
            "delimiter": ",",
            "include_header": False,
        }
    if content_type == ContentType.PSV.value:
        return {
            "delimiter": "|",
            "include_header": False,
        }
    if content_type in {ContentType.PARQUET.value, ContentType.JSON.value}:
        return {}
    raise ValueError(f"Unsupported content type: {content_type}")


def slice_dataset(dataset: Dataset, max_len: Optional[int]) -> List[Dataset]:
    """
    Returns equally-sized dataset slices of up to `max_len` records each.
    """
    # TODO (pdames): Since the purpose of this function is to ideally produce
    #  a dataset that won't write less than `max_len` records per file when the
    #  total dataset size is greater than `max_len`, we should also ensure that
    #  each dataset returned only contains 1 block.
    if max_len is None:
        return [dataset]
    dataset_len = dataset.count()
    num_splits = int(dataset_len / max_len)
    if dataset_len % max_len == 0:
        num_splits -= 1
    if num_splits <= 0:
        return [dataset]
    split_indices = [max_len + max_len * i for i in range(num_splits)]
    return dataset.split_at_indices(split_indices)


def dataset_size(dataset: Dataset) -> int:
    return dataset.size_bytes()


def dataset_to_file(
    table: Dataset,
    base_path: str,
    filesystem: Optional[Union[AbstractFileSystem, pafs.FileSystem]],
    block_path_provider: Union[Callable, FilenameProvider],
    content_type: str = ContentType.PARQUET.value,
    schema: Optional[pa.Schema] = None,
    **kwargs,
) -> None:
    """
    Writes the given Distributed Dataset to one or more files.
    """
    writer = CONTENT_TYPE_TO_DATASET_WRITE_FUNC.get(content_type)
    if not writer:
        raise NotImplementedError(
            f"Distributed Dataset writer for content type '{content_type}' not"
            f" implemented. Known content types: "
            f"{CONTENT_TYPE_TO_DATASET_WRITE_FUNC.keys}"
        )
    writer_kwargs = content_type_to_writer_kwargs(content_type)
    writer_kwargs.update(kwargs)
    writer(
        table,
        base_path,
        filesystem=filesystem,
        block_path_provider=block_path_provider,
        **writer_kwargs,
    )
