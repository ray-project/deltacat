import logging
from typing import Optional, List
from deltacat import logs
from deltacat.constants import NULL_SIZE_BYTES
from deltacat.compute.resource_requirements.parquet import (
    parquet_column_chunk_size_estimator,
)
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.types.partial_download import PartialParquetParameters
from deltacat.storage import (
    ManifestEntry,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _get_parquet_type_params_if_exist(
    entry: ManifestEntry,
) -> Optional[PartialParquetParameters]:
    if (
        entry.meta
        and entry.meta.content_type == ContentType.PARQUET
        and entry.meta.content_encoding == ContentEncoding.IDENTITY
        and entry.meta.content_type_parameters
    ):
        for type_params in entry.meta.content_type_parameters:
            if isinstance(type_params, PartialParquetParameters):
                return type_params
    return None


def _calculate_parquet_column_size(
    type_params: PartialParquetParameters,
    parquet_to_pyarrow_inflation: float,
    column: str,
    enable_intelligent_size_estimation: bool,
) -> float:

    memory_estimator = (
        parquet_column_chunk_size_estimator
        if enable_intelligent_size_estimation
        else lambda column_meta: column_meta.total_uncompressed_size
    )

    final_size = 0.0
    for rg in type_params.row_groups_to_download:
        columns_found = 0
        row_group_meta = type_params.pq_metadata.row_group(rg)
        for col in range(row_group_meta.num_columns):
            column_meta = row_group_meta.column(col)
            if column_meta.path_in_schema == column:
                columns_found += 1
                final_size += memory_estimator(column_meta=column_meta)
        if columns_found == 0:
            # This indicates a null column
            final_size += NULL_SIZE_BYTES * row_group_meta.num_rows
        elif columns_found > 1:
            raise ValueError(f"Duplicate column found: {column}")

    return final_size * parquet_to_pyarrow_inflation


def estimate_manifest_entry_size_bytes(
    entry: ManifestEntry,
    previous_inflation: float,
    parquet_to_pyarrow_inflation: float,
    force_use_previous_inflation: bool,
    enable_intelligent_size_estimation: bool,
    **kwargs,
) -> float:
    if entry.meta.source_content_length:
        logger.debug(f"Using source content length for entry={entry.uri}")
        return entry.meta.source_content_length

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if type_params and type_params.row_groups_to_download:
        if not force_use_previous_inflation:
            logger.debug(f"Using parquet meta for entry={entry.uri}")
            if enable_intelligent_size_estimation:
                column_names = [
                    type_params.pq_metadata.row_group(0).column(col).path_in_schema
                    for col in range(type_params.pq_metadata.num_columns)
                ]
                return estimate_manifest_entry_column_size_bytes(
                    entry=entry,
                    parquet_to_pyarrow_inflation=parquet_to_pyarrow_inflation,
                    columns=column_names,
                    enable_intelligent_size_estimation=enable_intelligent_size_estimation,
                )
            else:
                return type_params.in_memory_size_bytes * parquet_to_pyarrow_inflation
        else:
            logger.warning(
                f"Force using previous inflation for entry={entry.uri}. "
                "This could lead to overestimation of memory when "
                "enable_input_split=True"
            )

    logger.debug(f"Using inflation for entry={entry.uri}")
    return entry.meta.content_length * previous_inflation


def estimate_manifest_entry_num_rows(
    entry: ManifestEntry,
    average_record_size_bytes: float,
    previous_inflation: float,
    parquet_to_pyarrow_inflation: float,
    force_use_previous_inflation: bool,
    **kwargs,
) -> int:
    """
    Estimate number of records in the manifest entry file. It uses content type
    specific estimation logic if available, otherwise it falls back to using
    previous inflation and average record size.
    """
    if entry.meta.record_count:
        logger.debug(f"Using record count in meta for entry={entry.uri}")
        return entry.meta.record_count

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if type_params:
        if not force_use_previous_inflation:
            logger.debug(f"Using parquet meta for entry={entry.uri}")
            return type_params.num_rows
        else:
            logger.warning(
                f"Force using previous inflation for entry={entry.uri}. "
                "This could lead to overestimation of records when "
                "enable_input_split=True"
            )

    total_size_bytes = estimate_manifest_entry_size_bytes(
        entry=entry,
        previous_inflation=previous_inflation,
        parquet_to_pyarrow_inflation=parquet_to_pyarrow_inflation,
        force_use_previous_inflation=force_use_previous_inflation,
        enable_intelligent_size_estimation=False,
        **kwargs,
    )
    logger.debug(f"Using previous inflation for entry={entry.uri}")

    return int(total_size_bytes / average_record_size_bytes)


def estimate_manifest_entry_column_size_bytes(
    entry: ManifestEntry,
    parquet_to_pyarrow_inflation: float,
    enable_intelligent_size_estimation: bool,
    columns: Optional[List[str]] = None,
) -> Optional[float]:
    """
    Estimate the size of specified columns in the manifest entry file.
    This method only supports parquet. For other types, it returns None.
    """
    if not columns:
        return 0

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if type_params and type_params.pq_metadata:
        columns_size = 0.0
        for column_name in columns:
            columns_size += _calculate_parquet_column_size(
                type_params=type_params,
                column=column_name,
                parquet_to_pyarrow_inflation=parquet_to_pyarrow_inflation,
                enable_intelligent_size_estimation=enable_intelligent_size_estimation,
            )
        return columns_size

    return None
