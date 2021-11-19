import logging

from pyarrow import csv as pacsv
from fsspec import AbstractFileSystem

from ray.data import Dataset
from ray.data.datasource import BlockWritePathProvider

from deltacat import logs
from deltacat.types.media import ContentType, ContentEncoding

from typing import Callable, Dict, List, Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def write_parquet(
        dataset: Dataset,
        base_path: str,
        *,
        filesystem: AbstractFileSystem,
        block_path_provider: BlockWritePathProvider,
        **kwargs) -> None:

    dataset.write_parquet(
        base_path,
        filesystem=filesystem,
        try_create_dir=False,
        block_path_provider=block_path_provider,
        **kwargs,
    )


def write_csv(
        dataset: Dataset,
        base_path: str,
        *,
        filesystem: AbstractFileSystem,
        block_path_provider: BlockWritePathProvider,
        **kwargs) -> None:

    # column names are kept in table metadata, so omit header
    arrow_csv_args_fn = lambda: {
        "write_options": pacsv.WriteOptions(include_header=False)
    }
    pa_open_stream_args = {"compression": ContentEncoding.GZIP.value}
    dataset.write_csv(
        base_path,
        arrow_open_stream_args=pa_open_stream_args,
        filesystem=filesystem,
        try_create_dir=False,
        block_path_provider=block_path_provider,
        # column names are kept in table metadata, so omit header
        arrow_csv_args_fn=arrow_csv_args_fn,
        **kwargs,
    )


CONTENT_TYPE_TO_DATASET_WRITE_FUNC: Dict[str, Callable] = {
    ContentType.CSV.value: write_csv,
    ContentType.PARQUET.value: write_parquet,
}


def slice_dataset(
        dataset: Dataset,
        max_len: Optional[int]) -> List[Dataset]:
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
        file_system: AbstractFileSystem,
        block_path_provider: BlockWritePathProvider,
        content_type: str = ContentType.PARQUET.value,
        **kwargs) -> None:
    """
    Writes the given Distributed Dataset to a file.
    """
    writer = CONTENT_TYPE_TO_DATASET_WRITE_FUNC.get(content_type)
    if not writer:
        raise NotImplementedError(
            f"Distributed Dataset writer for content type '{content_type}' not"
            f" implemented. Known content types: "
            f"{CONTENT_TYPE_TO_DATASET_WRITE_FUNC.keys}")
    writer(
        table,
        base_path,
        filesystem=file_system,
        block_path_provider=block_path_provider,
        **kwargs
    )
