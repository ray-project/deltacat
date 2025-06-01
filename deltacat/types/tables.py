import logging
from enum import Enum
from typing import Callable, Dict, Type, Union, Optional, Any, List
from uuid import uuid4

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as papq
import ray
from ray.data.block import Block, BlockMetadata, BlockAccessor
from ray.data.dataset import Dataset as RayDataset, MaterializedDataset
from ray.data.datasource import FilenameProvider
from ray.data.read_api import (
    from_arrow,
    from_arrow_refs,
    from_numpy,
    from_pandas,
    from_pandas_refs,
)
from ray.types import ObjectRef
from tenacity import (
    Retrying,
    wait_random_exponential,
    stop_after_delay,
    retry_if_exception_type,
)

import deltacat.storage as dcs
from deltacat import logs
from deltacat.constants import (
    UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY,
    RETRYABLE_TRANSIENT_ERRORS,
)
from deltacat.storage.model.types import (
    LocalTable,
    DistributedDataset,
)
from deltacat.types.media import (
    TableType,
    DistributedDatasetType,
    ContentType,
    EXPLICIT_COMPRESSION_CONTENT_TYPES,
    ContentEncoding,
)
from deltacat.utils import numpy as np_utils
from deltacat.utils import pandas as pd_utils
from deltacat.utils import polars as pl_utils
from deltacat.utils import pyarrow as pa_utils
from deltacat.utils import daft as daft_utils
from deltacat.utils.ray_utils import dataset as ds_utils
from deltacat.storage.model.manifest import (
    ManifestEntryList,
    ManifestEntry,
    EntryParams,
    EntryType,
)
from deltacat.exceptions import (
    RetryableError,
    RetryableUploadTableError,
    NonRetryableUploadTableError,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


TABLE_TYPE_TO_S3_READER_FUNC: Dict[int, Callable] = {
    TableType.PYARROW_PARQUET.value: pa_utils.s3_file_to_parquet,
    TableType.PYARROW.value: pa_utils.s3_file_to_table,
    TableType.PANDAS.value: pd_utils.s3_file_to_dataframe,
    TableType.NUMPY.value: np_utils.s3_file_to_ndarray,
}

TABLE_CLASS_TO_WRITER_FUNC: Dict[
    Type[Union[dcs.LocalTable, dcs.DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.table_to_file,
    pd.DataFrame: pd_utils.dataframe_to_file,
    pl.DataFrame: pl_utils.dataframe_to_file,
    np.ndarray: np_utils.ndarray_to_file,
    RayDataset: ds_utils.dataset_to_file,
    MaterializedDataset: ds_utils.dataset_to_file,
}

TABLE_CLASS_TO_SLICER_FUNC: Dict[
    Type[Union[dcs.LocalTable, dcs.DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.slice_table,
    pd.DataFrame: pd_utils.slice_dataframe,
    pl.DataFrame: pl_utils.slice_table,
    np.ndarray: np_utils.slice_ndarray,
    RayDataset: ds_utils.slice_dataset,
    MaterializedDataset: ds_utils.slice_dataset,
}

TABLE_CLASS_TO_SIZE_FUNC: Dict[
    Type[Union[dcs.LocalTable, dcs.DistributedDataset]], Callable
] = {
    pa.Table: pa_utils.table_size,
    papq.ParquetFile: pa_utils.parquet_file_size,
    pd.DataFrame: pd_utils.dataframe_size,
    pl.DataFrame: pl_utils.dataframe_size,
    np.ndarray: np_utils.ndarray_size,
    RayDataset: ds_utils.dataset_size,
    MaterializedDataset: ds_utils.dataset_size,
}

TABLE_CLASS_TO_PYARROW_FUNC: Dict[
    Type[Union[dcs.LocalTable, dcs.DistributedDataset]], Callable
] = {
    pa.Table: lambda table, **kwargs: table,
    papq.ParquetFile: lambda table, **kwargs: table.read(**kwargs),
    pd.DataFrame: lambda table, **kwargs: pa.Table.from_pandas(table, **kwargs),
    pl.DataFrame: lambda table, **kwargs: pl.DataFrame.to_arrow(table, **kwargs),
    np.ndarray: lambda table, **kwargs: pa.Table.from_arrays(
        [pa.array(table[:, i]) for i in range(table.shape[1])]
    ),
}

TABLE_CLASS_TO_TABLE_TYPE: Dict[Type[dcs.LocalTable], str] = {
    pa.Table: TableType.PYARROW.value,
    papq.ParquetFile: TableType.PYARROW_PARQUET.value,
    pl.DataFrame: TableType.POLARS.value,
    pd.DataFrame: TableType.PANDAS.value,
    np.ndarray: TableType.NUMPY.value,
}

TABLE_TYPE_TO_DATASET_CREATE_FUNC: Dict[str, Callable] = {
    TableType.PYARROW.value: from_arrow,
    TableType.PYARROW_PARQUET.value: from_arrow,
    TableType.NUMPY.value: from_numpy,
    TableType.PANDAS.value: from_pandas,
}

TABLE_TYPE_TO_DATASET_CREATE_FUNC_REFS: Dict[str, Callable] = {
    TableType.PYARROW.value: from_arrow_refs,
    TableType.PYARROW_PARQUET.value: from_arrow_refs,
    TableType.NUMPY.value: from_numpy,
    TableType.PANDAS.value: from_pandas_refs,
}

DISTRIBUTED_DATASET_TYPE_TO_READER_FUNC: Dict[int, Callable] = {
    DistributedDatasetType.DAFT.value: daft_utils.s3_files_to_dataframe
}


class TableWriteMode(str, Enum):
    """
    Enum controlling how a given dataset will be written to a table.

    AUTO: CREATE if the table doesn't exist, APPEND if the table exists
    without merge keys, and MERGE if the table exists with merge keys.
    CREATE: Create the table if it doesn't exist, throw an error if it does.
    APPEND: Append to the table if it exists, throw an error if it doesn't.
    REPLACE: Replace existing table contents with the data to write.
    MERGE: Insert, update, or delete records matching a given predicate.
    Updates or inserts records based on the table's primary and sort keys by
    default.
    """

    AUTO = "auto"
    CREATE = "create"
    APPEND = "append"
    REPLACE = "replace"
    MERGE = "merge"


def get_table_length(
    table: Union[dcs.LocalTable, dcs.DistributedDataset, BlockAccessor]
) -> int:
    return len(table) if not isinstance(table, RayDataset) else table.count()


def get_table_size(table: Union[dcs.LocalTable, dcs.DistributedDataset]) -> int:
    table_size_func = TABLE_CLASS_TO_SIZE_FUNC.get(type(table))
    if table_size_func is None:
        msg = (
            f"No size function found for table type: {type(table)}.\n"
            f"Known table types: {TABLE_CLASS_TO_SIZE_FUNC.keys}"
        )
        raise ValueError(msg)
    return table_size_func(table)


def get_table_writer(table: Union[dcs.LocalTable, dcs.DistributedDataset]) -> Callable:
    table_writer_func = TABLE_CLASS_TO_WRITER_FUNC.get(type(table))
    if table_writer_func is None:
        msg = (
            f"No writer found for table type: {type(table)}.\n"
            f"Known table types: {TABLE_CLASS_TO_WRITER_FUNC.keys}"
        )
        raise ValueError(msg)
    return table_writer_func


def get_table_slicer(table: Union[dcs.LocalTable, dcs.DistributedDataset]) -> Callable:
    table_slicer_func = TABLE_CLASS_TO_SLICER_FUNC.get(type(table))
    if table_slicer_func is None:
        msg = (
            f"No slicer found for table type: {type(table)}.\n"
            f"Known table types: {TABLE_CLASS_TO_SLICER_FUNC.keys}"
        )
        raise ValueError(msg)
    return table_slicer_func


def write_sliced_table(
    table: Union[LocalTable, DistributedDataset],
    base_path: str,
    filesystem: Optional[pa.fs.FileSystem],
    max_records_per_entry: Optional[int],
    table_writer_fn: Callable,
    table_slicer_fn: Callable,
    table_writer_kwargs: Optional[Dict[str, Any]] = None,
    content_type: ContentType = ContentType.PARQUET,
    entry_params: Optional[EntryParams] = None,
    entry_type: Optional[EntryType] = EntryType.DATA,
) -> ManifestEntryList:

    # @retry decorator can't be pickled by Ray, so wrap upload in Retrying
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_delay(UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )

    manifest_entries = ManifestEntryList()
    table_record_count = get_table_length(table)

    if max_records_per_entry is None or not table_record_count:
        # write the whole table to a single file
        manifest_entries = retrying(
            write_table,
            table,
            f"{base_path}",  # cast any non-string arg to string
            filesystem,
            table_writer_fn,
            table_writer_kwargs,
            content_type,
            entry_params,
            entry_type,
        )
    else:
        # iteratively write table slices
        table_slices = table_slicer_fn(table, max_records_per_entry)
        for table_slice in table_slices:
            slice_entries = retrying(
                write_table,
                table_slice,
                f"{base_path}",  # cast any non-string arg to string
                filesystem,
                table_writer_fn,
                table_writer_kwargs,
                content_type,
                entry_params,
                entry_type,
            )
            manifest_entries.extend(slice_entries)
    return manifest_entries


def write_table(
    table: Union[LocalTable, DistributedDataset],
    base_path: str,
    filesystem: Optional[pa.fs.FileSystem],
    table_writer_fn: Callable,
    table_writer_kwargs: Optional[Dict[str, Any]],
    content_type: ContentType = ContentType.PARQUET,
    entry_params: Optional[EntryParams] = None,
    entry_type: Optional[EntryType] = EntryType.DATA,
) -> ManifestEntryList:
    """
    Writes the given table to 1 or more files and return
    manifest entries describing the uploaded files.
    """
    if table_writer_kwargs is None:
        table_writer_kwargs = {}

    wrapped_obj = (
        CapturedBlockWritePathsActor.remote()
        if isinstance(table, RayDataset)
        else CapturedBlockWritePathsBase()
    )
    capture_object = CapturedBlockWritePaths(wrapped_obj)
    block_write_path_provider = UuidBlockWritePathProvider(
        capture_object,
        base_path=base_path,
    )
    table_writer_fn(
        table,
        base_path,
        filesystem,
        block_write_path_provider,
        content_type.value,
        **table_writer_kwargs,
    )
    # TODO: Add a proper fix for block_refs and write_paths not persisting in Ray actors
    del block_write_path_provider
    blocks = capture_object.blocks()
    write_paths = capture_object.write_paths()
    metadata = get_block_metadata_list(table, write_paths, blocks)
    manifest_entries = ManifestEntryList()
    content_encoding = None
    if content_type in EXPLICIT_COMPRESSION_CONTENT_TYPES:
        # TODO(pdames): Support other user-specified encodings at write time.
        content_encoding = ContentEncoding.GZIP
    for block_idx, path in enumerate(write_paths):
        try:
            manifest_entry = ManifestEntry.from_path(
                path=path,
                filesystem=filesystem,
                record_count=metadata[block_idx].num_rows,
                source_content_length=metadata[block_idx].size_bytes,
                content_type=content_type.value,
                content_encoding=content_encoding,
                entry_type=entry_type,
                entry_params=entry_params,
            )
            manifest_entries.append(manifest_entry)
        except RETRYABLE_TRANSIENT_ERRORS as e:
            raise RetryableUploadTableError(
                f"Retry write for: {path} after receiving {type(e).__name__}",
            ) from e
        except BaseException as e:
            logger.warning(
                f"Upload has failed for {path} and content_type={content_type}. Error: {e}",
                exc_info=True,
            )
            raise NonRetryableUploadTableError(
                f"Upload has failed for {path} and content_type={content_type} because of {type(e).__name__}",
            ) from e
    return manifest_entries


@ray.remote
class CapturedBlockWritePathsActor:
    def __init__(self):
        self._wrapped = CapturedBlockWritePathsBase()

    def extend(self, write_paths: List[str], blocks: List[Block]) -> None:
        self._wrapped.extend(write_paths, blocks)

    def write_paths(self) -> List[str]:
        return self._wrapped.write_paths()

    def blocks(self) -> List[Block]:
        return self._wrapped.blocks()


class CapturedBlockWritePathsBase:
    def __init__(self):
        self._write_paths: List[str] = []
        self._blocks: List[Block] = []

    def extend(self, write_paths: List[str], blocks: List[Block]) -> None:
        try:
            iter(write_paths)
        except TypeError:
            pass
        else:
            self._write_paths.extend(write_paths)
        try:
            iter(blocks)
        except TypeError:
            pass
        else:
            self._blocks.extend(blocks)

    def write_paths(self) -> List[str]:
        return self._write_paths

    def blocks(self) -> List[Block]:
        return self._blocks


class CapturedBlockWritePaths:
    def __init__(self, wrapped=CapturedBlockWritePathsBase()):
        self._wrapped = wrapped

    def extend(self, write_paths: List[str], blocks: List[Block]) -> None:
        return (
            self._wrapped.extend(write_paths, blocks)
            if isinstance(self._wrapped, CapturedBlockWritePathsBase)
            else ray.get(self._wrapped.extend.remote(write_paths, blocks))
        )

    def write_paths(self) -> List[str]:
        return (
            self._wrapped.write_paths()
            if isinstance(self._wrapped, CapturedBlockWritePathsBase)
            else ray.get(self._wrapped.write_paths.remote())
        )

    def blocks(self) -> List[Block]:
        return (
            self._wrapped.blocks()
            if isinstance(self._wrapped, CapturedBlockWritePathsBase)
            else ray.get(self._wrapped.blocks.remote())
        )


class UuidBlockWritePathProvider(FilenameProvider):
    """Block write path provider implementation that writes each
    dataset block out to a file of the form: {base_path}/{uuid}
    """

    def __init__(
        self,
        capture_object: CapturedBlockWritePaths,
        base_path: Optional[str] = None,
    ):
        self.base_path = base_path
        self.write_paths: List[str] = []
        self.blocks: List[Block] = []
        self.capture_object = capture_object

    def __del__(self):
        if self.write_paths or self.blocks:
            self.capture_object.extend(
                self.write_paths,
                self.blocks,
            )

    def get_filename_for_block(
        self,
        block: Block,
        task_index: int,
        block_index: int,
    ) -> str:
        if self.base_path is None:
            raise ValueError(
                "Base path must be provided to UuidBlockWritePathProvider",
            )
        return self._get_write_path_for_block(
            base_path=self.base_path,
            block=block,
            block_index=block_index,
        )

    def _get_write_path_for_block(
        self,
        base_path: str,
        *,
        block: Optional[Block] = None,
        **kwargs,
    ) -> str:
        write_path = f"{base_path}/{str(uuid4())}"
        self.write_paths.append(write_path)
        if block is not None:
            self.blocks.append(block)
        return write_path

    def __call__(
        self,
        base_path: str,
        *,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        dataset_uuid: Optional[str] = None,
        block: Optional[Block] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        return self._get_write_path_for_block(
            base_path,
            filesystem=filesystem,
            dataset_uuid=dataset_uuid,
            block=block,
            block_index=block_index,
            file_format=file_format,
        )


def get_block_metadata_list(
    table: LocalTable,
    write_paths: List[str],
    blocks: List[Block],
) -> List[BlockMetadata]:
    block_meta_list: List[BlockMetadata] = []
    if not blocks:
        # this must be a local table - ensure it was written to only 1 file
        assert len(write_paths) == 1, (
            f"Expected table of type '{type(table)}' to be written to 1 "
            f"file, but found {len(write_paths)} files."
        )
        blocks = [table]
    for block in blocks:
        block_meta_list.append(get_block_metadata(block))
    return block_meta_list


def get_block_metadata(
    table: Union[LocalTable, DistributedDataset, BlockAccessor],
) -> BlockMetadata:
    table_size = None
    table_size_func = TABLE_CLASS_TO_SIZE_FUNC.get(type(table))
    if table_size_func:
        table_size = table_size_func(table)
    else:
        logger.warning(f"Unable to estimate '{type(table)}' table size.")
    if isinstance(table, BlockAccessor):
        table = table.to_block()
    return BlockMetadata(
        num_rows=get_table_length(table),
        size_bytes=table_size,
        schema=None,
        input_files=None,
        exec_stats=None,
    )


def block_metadata(block: Block) -> BlockMetadata:
    return BlockAccessor.for_block(block).get_metadata(
        input_files=None,
        exec_stats=None,
    )
