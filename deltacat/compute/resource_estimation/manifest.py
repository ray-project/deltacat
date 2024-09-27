import logging
from typing import Optional, List
from deltacat import logs
from deltacat.constants import NULL_SIZE_BYTES
from deltacat.compute.resource_estimation.parquet import (
    parquet_column_chunk_size_estimator,
)
from deltacat.types.media import ContentEncoding, ContentType
from deltacat.types.partial_download import PartialParquetParameters
from deltacat.storage import (
    ManifestEntry,
)
from deltacat.compute.resource_estimation.model import (
    OperationType,
    EstimateResourcesParams,
    ResourceEstimationMethod,
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
            raise ValueError(f"Duplicate column found in parquet file: {column}")

    return final_size * parquet_to_pyarrow_inflation


def _estimate_manifest_entry_size_bytes_using_previous_inflation(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    **kwargs,
) -> Optional[float]:

    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Size can only be estimated for PYARROW_DOWNLOAD operation"
    assert (
        estimate_resources_params.previous_inflation is not None
    ), "Expected previous_inflation when resource estimation method is PREVIOUS_INFLATION"

    return entry.meta.content_length * estimate_resources_params.previous_inflation


def _estimate_manifest_entry_size_bytes_using_content_type_meta(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    **kwargs,
) -> Optional[float]:

    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Size can only be estimated for PYARROW_DOWNLOAD operation"

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if (
        not type_params
        or estimate_resources_params.parquet_to_pyarrow_inflation is None
    ):
        return None

    if not type_params.row_groups_to_download:
        return 0

    return (
        type_params.in_memory_size_bytes
        * estimate_resources_params.parquet_to_pyarrow_inflation
    )


def _estimate_manifest_entry_size_bytes_using_intelligent_estimation(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    **kwargs,
) -> Optional[float]:

    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Size can only be estimated for PYARROW_DOWNLOAD operation"

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if (
        not type_params
        or estimate_resources_params.parquet_to_pyarrow_inflation is None
    ):
        return None

    if not type_params.row_groups_to_download:
        return 0

    column_names = [
        type_params.pq_metadata.row_group(0).column(col).path_in_schema
        for col in range(type_params.pq_metadata.num_columns)
    ]
    return estimate_manifest_entry_column_size_bytes(
        entry=entry,
        operation_type=operation_type,
        columns=column_names,
        estimate_resources_params=estimate_resources_params,
    )


RESOURCE_ESTIMATION_METHOD_TO_SIZE_ESTIMATION_FUNCTIONS = {
    ResourceEstimationMethod.PREVIOUS_INFLATION: [
        _estimate_manifest_entry_size_bytes_using_previous_inflation
    ],
    ResourceEstimationMethod.CONTENT_TYPE_META: [
        _estimate_manifest_entry_size_bytes_using_content_type_meta
    ],
    ResourceEstimationMethod.INTELLIGENT_ESTIMATION: [
        _estimate_manifest_entry_size_bytes_using_intelligent_estimation
    ],
    ResourceEstimationMethod.DEFAULT: [
        _estimate_manifest_entry_size_bytes_using_content_type_meta,
        _estimate_manifest_entry_size_bytes_using_previous_inflation,
    ],
    ResourceEstimationMethod.DEFAULT_V2: [
        _estimate_manifest_entry_size_bytes_using_intelligent_estimation,
        _estimate_manifest_entry_size_bytes_using_previous_inflation,
    ],
}


def _estimate_manifest_entry_num_rows_using_previous_inflation(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    **kwargs,
) -> Optional[int]:
    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"
    assert (
        estimate_resources_params.previous_inflation is not None
    ), "Expected previous_inflation when resource estimation method is PREVIOUS_INFLATION"
    assert (
        estimate_resources_params.average_record_size_bytes is not None
    ), "Expected average_record_size_bytes when resource estimation method is PREVIOUS_INFLATION"

    total_size_bytes = estimate_manifest_entry_size_bytes(
        entry=entry,
        operation_type=operation_type,
        estimate_resources_params=estimate_resources_params,
        **kwargs,
    )

    return int(total_size_bytes / estimate_resources_params.average_record_size_bytes)


def _estimate_manifest_entry_num_rows_using_content_type_meta(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    **kwargs,
) -> Optional[int]:
    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if not type_params:
        return None

    return type_params.num_rows


def _estimate_manifest_entry_num_rows_using_intelligent_estimation(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    **kwargs,
) -> Optional[int]:
    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if not type_params:
        return None

    return type_params.num_rows


RESOURCE_ESTIMATION_METHOD_TO_NUM_ROWS_ESTIMATION_FUNCTIONS = {
    ResourceEstimationMethod.PREVIOUS_INFLATION: [
        _estimate_manifest_entry_num_rows_using_previous_inflation
    ],
    ResourceEstimationMethod.CONTENT_TYPE_META: [
        _estimate_manifest_entry_num_rows_using_content_type_meta
    ],
    ResourceEstimationMethod.INTELLIGENT_ESTIMATION: [
        _estimate_manifest_entry_num_rows_using_intelligent_estimation
    ],
    ResourceEstimationMethod.DEFAULT: [
        _estimate_manifest_entry_num_rows_using_content_type_meta,
        _estimate_manifest_entry_num_rows_using_previous_inflation,
    ],
    ResourceEstimationMethod.DEFAULT_V2: [
        _estimate_manifest_entry_num_rows_using_intelligent_estimation,
        _estimate_manifest_entry_num_rows_using_previous_inflation,
    ],
}


def estimate_manifest_entry_size_bytes(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams = None,
    **kwargs,
) -> Optional[float]:
    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Size can only be estimated for PYARROW_DOWNLOAD operation"

    if entry.meta.source_content_length:
        # No need to estimate size as source_content_length is already present
        return entry.meta.source_content_length

    if estimate_resources_params is None:
        estimate_resources_params = EstimateResourcesParams.of()

    functions = RESOURCE_ESTIMATION_METHOD_TO_SIZE_ESTIMATION_FUNCTIONS.get(
        estimate_resources_params.resource_estimation_method
    )

    if functions is None:
        raise ValueError(
            "Unsupported size estimation method"
            f": {estimate_resources_params.resource_estimation_method} for entry: {entry}"
        )

    for func in functions:
        size_bytes = func(
            entry=entry,
            operation_type=operation_type,
            estimate_resources_params=estimate_resources_params,
            **kwargs,
        )
        if size_bytes is not None:
            logger.debug(
                f"Estimated size for entry={entry.uri} is {size_bytes} using {func}"
            )
            return size_bytes

    return None


def estimate_manifest_entry_num_rows(
    entry: ManifestEntry,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams = None,
    **kwargs,
) -> Optional[int]:
    """
    Estimate number of records in the manifest entry file.
    """
    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"

    if entry.meta.record_count:
        # No need to estimate as record_count is already present
        return entry.meta.record_count

    if estimate_resources_params is None:
        estimate_resources_params = EstimateResourcesParams.of()

    functions = RESOURCE_ESTIMATION_METHOD_TO_NUM_ROWS_ESTIMATION_FUNCTIONS.get(
        estimate_resources_params.resource_estimation_method
    )

    if functions is None:
        raise ValueError(
            "Unsupported num rows estimation method"
            f": {estimate_resources_params.resource_estimation_method} for entry: {entry}"
        )

    for func in functions:
        num_rows = func(
            entry=entry,
            operation_type=operation_type,
            estimate_resources_params=estimate_resources_params,
            **kwargs,
        )
        if num_rows is not None:
            logger.debug(
                f"Estimated number of rows for entry={entry.uri} is {num_rows} using {func}"
            )
            return num_rows

    return None


def estimate_manifest_entry_column_size_bytes(
    entry: ManifestEntry,
    operation_type: OperationType,
    columns: Optional[List[str]] = None,
    estimate_resources_params: EstimateResourcesParams = None,
) -> Optional[float]:
    """
    Estimate the size of specified columns in the manifest entry file.
    This method only supports parquet. For other types, it returns None.
    """

    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Resources can only be estimated for PYARROW_DOWNLOAD operation"

    type_params = _get_parquet_type_params_if_exist(entry=entry)

    if (
        not type_params
        or not type_params.pq_metadata
        or not estimate_resources_params.parquet_to_pyarrow_inflation
    ):
        return None

    if not columns or not type_params.row_groups_to_download:
        return 0

    if estimate_resources_params is None:
        estimate_resources_params = EstimateResourcesParams.of()

    is_intelligent_estimation = (
        estimate_resources_params.resource_estimation_method
        == ResourceEstimationMethod.INTELLIGENT_ESTIMATION
        or estimate_resources_params.resource_estimation_method
        == ResourceEstimationMethod.DEFAULT_V2
    )

    columns_size = 0.0
    for column_name in columns:
        columns_size += _calculate_parquet_column_size(
            type_params=type_params,
            column=column_name,
            parquet_to_pyarrow_inflation=estimate_resources_params.parquet_to_pyarrow_inflation,
            enable_intelligent_size_estimation=is_intelligent_estimation,
        )
    return columns_size


def does_require_content_type_params(
    resource_estimation_method: ResourceEstimationMethod,
) -> bool:
    return (
        resource_estimation_method == ResourceEstimationMethod.DEFAULT_V2
        or resource_estimation_method == ResourceEstimationMethod.INTELLIGENT_ESTIMATION
    )
