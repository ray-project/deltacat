import logging

from fsspec import AbstractFileSystem
from deltacat import logs
import deltacat.compute.converter.utils.iceberg_columns as sc
import daft
from deltacat.utils.daft import _get_s3_io_config
from daft import TimeUnit, DataFrame
import pyarrow as pa
from typing import Callable, Optional, List, Dict, Any
from deltacat.utils.pyarrow import sliced_string_cast
from deltacat.compute.converter.constants import IDENTIFIER_FIELD_DELIMITER
from deltacat.compute.converter.utils.s3u import upload_table_with_retry
from pyiceberg.manifest import DataFile
import pyarrow.compute as pc
from deltacat.types.media import ContentType
from deltacat.types.tables import (
    get_table_writer,
    get_table_slicer,
    write_sliced_table as types_write_sliced_table,
)
from deltacat.storage import LocalTable, DistributedDataset
from deltacat.utils.filesystem import append_protocol_prefix_by_type, FilesystemType
from typing import Union

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def download_data_table_and_append_iceberg_columns(
    file: DataFile,
    columns_to_download: List[str],
    additional_columns_to_append: Optional[List[str]] = [],
    s3_client_kwargs: Optional[Dict[str, Any]] = None,
) -> pa.Table:
    table = download_parquet_with_daft_hash_applied(
        identifier_columns=columns_to_download,
        file=file,
        s3_client_kwargs=s3_client_kwargs,
    )

    if sc._FILE_PATH_COLUMN_NAME in additional_columns_to_append:
        table = sc.append_file_path_column(table, file.file_path)
    if sc._ORDERED_RECORD_IDX_COLUMN_NAME in additional_columns_to_append:
        record_idx_iterator = iter(range(len(table)))
        table = sc.append_record_idx_col(table, record_idx_iterator)

    return table


def download_parquet_with_daft_hash_applied(
    identifier_columns: List[str],
    file: DataFile,
    s3_client_kwargs: Optional[Dict[str, Any]],
    **kwargs: Any,
) -> pa.Table:

    # TODO: Add correct read kwargs as in:
    #  https://github.com/ray-project/deltacat/blob/383855a4044e4dfe03cf36d7738359d512a517b4/deltacat/utils/daft.py#L97

    coerce_int96_timestamp_unit = TimeUnit.from_str(
        kwargs.get("coerce_int96_timestamp_unit", "ms")
    )

    # TODO: Use Daft SHA1 hash instead to minimize probably of data corruption
    io_config = _get_s3_io_config(s3_client_kwargs=s3_client_kwargs)
    df = daft_read_parquet(
        path=file.file_path,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )

    hash_column = concatenate_hashed_identifier_columns(
        df=df, identifier_columns=identifier_columns
    )

    table = pa.Table.from_arrays(
        [hash_column], names=[sc._IDENTIFIER_COLUMNS_HASH_COLUMN_NAME]
    )

    return table


def daft_read_parquet(
    path: str, io_config: Dict[str, Any], coerce_int96_timestamp_unit: TimeUnit
) -> DataFrame:
    df = daft.read_parquet(
        path=path,
        io_config=io_config,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )
    return df


def concatenate_hashed_identifier_columns(
    df: DataFrame, identifier_columns: List[str]
) -> pa.Array:
    pk_hash_columns = []
    previous_hash_column_length = None
    for i in range(len(identifier_columns)):
        pk_hash_column = df.select(daft.col(identifier_columns[i]).hash())
        pk_hash_column_arrow = pk_hash_column.to_arrow()

        # Assert that each hash column downloaded are same length to ensure we don't create mismatch between columns.
        if not previous_hash_column_length:
            previous_hash_column_length = len(pk_hash_column_arrow)
        else:
            assert previous_hash_column_length == len(pk_hash_column_arrow), (
                f"Identifier column Length mismatch: {identifier_columns[i]} has length {len(pk_hash_column_arrow)} "
                f"but expected {previous_hash_column_length}."
            )
            previous_hash_column_length = len(pk_hash_column_arrow)

        # Convert identifier from different datatypes to string here
        pk_hash_column_str = sliced_string_cast(
            pk_hash_column_arrow[identifier_columns[i]]
        )
        assert len(pk_hash_column_str) == previous_hash_column_length, (
            f"Casting column Length mismatch: {identifier_columns[i]} has length {len(pk_hash_column_str)} after casting, "
            f"before casting length: {previous_hash_column_length}."
        )

        pk_hash_columns.append(pk_hash_column_str)

    pk_hash_columns.append(IDENTIFIER_FIELD_DELIMITER)
    pk_hash_columns_concatenated = pc.binary_join_element_wise(
        *pk_hash_columns, null_handling="replace"
    )
    assert len(pk_hash_columns_concatenated) == previous_hash_column_length, (
        f"Concatenated column Length mismatch: Final concatenated identifier column has length {len(pk_hash_columns_concatenated)}, "
        f"before concatenating length: {previous_hash_column_length}."
    )

    return pk_hash_columns_concatenated


def write_sliced_table(
    table: Union[LocalTable, DistributedDataset],
    base_path: str,
    table_writer_kwargs: Optional[Dict[str, Any]],
    content_type: ContentType = ContentType.PARQUET,
    max_records_per_file: Optional[int] = 4000000,
    filesystem: Optional[Union[AbstractFileSystem, pa.fs.FileSystem]] = None,
    skip_manifest_write: bool = False,
    **kwargs,
) -> List[str]:
    """
    Writes the given table to 1 or more files and return the paths
    of the files written.

    Args:
        skip_manifest_write: If True, calls table_writer_fn directly and returns file paths as strings.
                           If False (default), creates manifest entries and extracts URIs.
    """
    if isinstance(filesystem, pa.fs.FileSystem):
        table_writer_fn = get_table_writer(table)
        table_slicer_fn = get_table_slicer(table)

        # Create a wrapper for the table writer that ensures directory creation
        def table_writer_with_dir_creation(
            dataframe: Any,
            base_path: str,
            filesystem: Optional[Union[AbstractFileSystem, pa.fs.FileSystem]],
            block_path_provider: Callable,
            content_type: str = ContentType.PARQUET.value,
            **kwargs,
        ):
            try:
                # Ensure base path directory exists
                if isinstance(base_path, str):
                    # Normalize the base path and ensure it's treated as a directory path
                    base_dir = base_path.rstrip("/")
                    filesystem.create_dir(base_dir, recursive=True)
            except Exception:
                # Directory might already exist or there might be permission issues
                # Let the original write attempt proceed
                pass
            return table_writer_fn(
                dataframe,
                base_path,
                filesystem,
                block_path_provider,
                content_type,
                **kwargs,
            )

        # TODO(pdames): Disable redundant file info fetch currently
        #   used to construct unused manifest entry metadata.
        result = types_write_sliced_table(
            table=table,
            base_path=base_path,
            filesystem=filesystem,
            max_records_per_entry=max_records_per_file,
            table_writer_fn=table_writer_with_dir_creation,
            table_slicer_fn=table_slicer_fn,
            table_writer_kwargs=table_writer_kwargs,
            content_type=content_type,
            skip_manifest_write=skip_manifest_write,
        )

        if skip_manifest_write:
            # Result is already a list of file paths
            paths = result
        else:
            # Result is manifest entries, extract URIs
            paths = [entry.uri for entry in result]

        filesystem_type = FilesystemType.from_filesystem(filesystem)
        paths = [
            append_protocol_prefix_by_type(path, filesystem_type) for path in paths
        ]

        return paths
    else:
        return upload_table_with_retry(
            table=table,
            s3_url_prefix=base_path,
            s3_table_writer_kwargs=table_writer_kwargs,
            content_type=content_type,
            max_records_per_file=max_records_per_file,
            filesystem=filesystem,
            **kwargs,
        )
