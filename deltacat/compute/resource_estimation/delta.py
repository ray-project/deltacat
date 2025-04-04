import logging
from typing import Optional, Dict, Any
from deltacat import logs
from deltacat.storage import (
    Delta,
    interface as unimplemented_deltacat_storage,
)
from deltacat.compute.compactor_v2.utils.content_type_params import (
    append_content_type_params,
)
from deltacat.compute.resource_estimation.model import (
    OperationType,
    EstimateResourcesParams,
    ResourceEstimationMethod,
    EstimatedResources,
    Statistics,
)
from deltacat.compute.resource_estimation.manifest import (
    estimate_manifest_entry_size_bytes,
    estimate_manifest_entry_num_rows,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _estimate_resources_required_to_process_delta_using_previous_inflation(
    delta: Delta,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    deltacat_storage: unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Dict[str, Any],
    **kwargs,
) -> Optional[EstimatedResources]:

    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"
    assert (
        estimate_resources_params.previous_inflation
    ), "Previous inflation must be provided to estimate delta size"

    in_memory_size = (
        delta.meta.content_length * estimate_resources_params.previous_inflation
    )
    num_rows = 0
    if estimate_resources_params.average_record_size_bytes is not None:
        num_rows = int(
            in_memory_size / estimate_resources_params.average_record_size_bytes
        )

    return EstimatedResources.of(
        memory_bytes=in_memory_size,
        statistics=Statistics.of(
            in_memory_size_bytes=in_memory_size,
            record_count=num_rows,
            on_disk_size_bytes=delta.meta.content_length,
        ),
    )


def _estimate_resources_required_to_process_delta_using_type_params(
    delta: Delta,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    deltacat_storage: unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Dict[str, Any],
    **kwargs,
) -> Optional[EstimatedResources]:

    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"

    if estimate_resources_params.parquet_to_pyarrow_inflation is None:
        logger.debug(
            "Could not estimate using type params as "
            f"parquet_to_pyarrow_inflation is None for {delta.locator}"
        )
        return None

    if not delta.manifest:
        delta.manifest = deltacat_storage.get_delta_manifest(
            delta.locator,
            **deltacat_storage_kwargs,
        )

    if not delta.manifest or not delta.manifest.entries:
        return EstimatedResources.of(
            memory_bytes=0,
            statistics=Statistics.of(
                in_memory_size_bytes=0,
                record_count=0,
                on_disk_size_bytes=delta.meta.content_length,
            ),
        )
    file_reader_kwargs_provider = kwargs.get(
        "file_reader_kwargs_provider"
    ) or deltacat_storage_kwargs.get("file_reader_kwargs_provider")

    """
    NOTE: The file_reader_kwargs_provider parameter can be passed in two ways:
    1. Nested within deltacat_storage_kwargs during resource estimation
    2. As a top-level attribute of CompactPartitionsParams during compaction

    This creates an inconsistent parameter path between resource estimation and compaction flows.
    As a long-term solution, this should be unified to use a single consistent path (either always
    nested in deltacat_storage_kwargs or always as a top-level parameter).

    For now, this implementation handles the resource estimation case by:
    1. First checking for file_reader_kwargs_provider as a direct kwarg
    2. Falling back to deltacat_storage_kwargs if not found
    This approach maintains backward compatibility by not modifying the DELTA_RESOURCE_ESTIMATION_FUNCTIONS signatures.
    """
    appended = append_content_type_params(
        delta=delta,
        deltacat_storage=deltacat_storage,
        deltacat_storage_kwargs=deltacat_storage_kwargs,
        file_reader_kwargs_provider=file_reader_kwargs_provider,
    )

    if not appended:
        logger.debug(
            f"Could not append content type params for {delta.locator}, returning None"
        )
        return None

    in_memory_size = 0.0
    num_rows = 0

    for entry in delta.manifest.entries:
        cur_memory = estimate_manifest_entry_size_bytes(
            entry=entry,
            operation_type=operation_type,
            estimate_resources_params=estimate_resources_params,
            **kwargs,
        )
        cur_num_rows = estimate_manifest_entry_num_rows(
            entry=entry,
            operation_type=operation_type,
            estimate_resources_params=estimate_resources_params,
            **kwargs,
        )

        if cur_memory is None or cur_num_rows is None:
            return None

        in_memory_size += cur_memory
        num_rows += cur_num_rows

    return EstimatedResources.of(
        memory_bytes=in_memory_size,
        statistics=Statistics.of(
            in_memory_size_bytes=in_memory_size,
            record_count=num_rows,
            on_disk_size_bytes=delta.meta.content_length,
        ),
    )


def _estimate_resources_required_to_process_delta_using_file_sampling(
    delta: Delta,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams,
    deltacat_storage: unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Dict[str, Any],
    **kwargs,
) -> Optional[EstimatedResources]:

    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"

    if not delta.manifest:
        delta.manifest = deltacat_storage.get_delta_manifest(
            delta.locator,
            **deltacat_storage_kwargs,
        )

    if not delta.manifest or not delta.manifest.entries:
        return EstimatedResources.of(
            memory_bytes=0,
            statistics=Statistics.of(
                in_memory_size_bytes=0,
                record_count=0,
                on_disk_size_bytes=delta.meta.content_length,
            ),
        )

    if not estimate_resources_params.max_files_to_sample:
        # we cannot calculate if we cannot sample
        return None

    sampled_in_memory_size = 0.0
    sampled_on_disk_size = 0.0
    sampled_num_rows = 0

    for entry_index in range(
        min(estimate_resources_params.max_files_to_sample, len(delta.manifest.entries))
    ):
        tbl = deltacat_storage.download_delta_manifest_entry(
            delta,
            entry_index,
            **deltacat_storage_kwargs,
        )
        sampled_in_memory_size += tbl.nbytes
        sampled_on_disk_size += delta.manifest.entries[entry_index].meta.content_length
        sampled_num_rows += len(tbl)

    if not sampled_on_disk_size or not sampled_in_memory_size:
        return EstimatedResources.of(
            memory_bytes=0,
            statistics=Statistics.of(
                in_memory_size_bytes=0,
                record_count=0,
                on_disk_size_bytes=delta.meta.content_length,
            ),
        )

    sampled_inflation = sampled_in_memory_size / sampled_on_disk_size

    in_memory_size = sampled_inflation * delta.meta.content_length
    num_rows = int(in_memory_size / sampled_in_memory_size * sampled_num_rows)

    return EstimatedResources.of(
        memory_bytes=in_memory_size,
        statistics=Statistics.of(
            in_memory_size_bytes=in_memory_size,
            record_count=num_rows,
            on_disk_size_bytes=delta.meta.content_length,
        ),
    )


RESOURCE_ESTIMATION_METHOD_TO_DELTA_RESOURCE_ESTIMATION_FUNCTIONS = {
    ResourceEstimationMethod.PREVIOUS_INFLATION: [
        _estimate_resources_required_to_process_delta_using_previous_inflation
    ],
    ResourceEstimationMethod.CONTENT_TYPE_META: [
        _estimate_resources_required_to_process_delta_using_type_params
    ],
    ResourceEstimationMethod.INTELLIGENT_ESTIMATION: [
        _estimate_resources_required_to_process_delta_using_type_params,
    ],
    ResourceEstimationMethod.FILE_SAMPLING: [
        _estimate_resources_required_to_process_delta_using_file_sampling
    ],
    ResourceEstimationMethod.DEFAULT: [
        _estimate_resources_required_to_process_delta_using_previous_inflation,
    ],
    ResourceEstimationMethod.DEFAULT_V2: [
        _estimate_resources_required_to_process_delta_using_type_params,
        _estimate_resources_required_to_process_delta_using_file_sampling,
        _estimate_resources_required_to_process_delta_using_previous_inflation,
    ],
}


def estimate_resources_required_to_process_delta(
    delta: Delta,
    operation_type: OperationType,
    estimate_resources_params: EstimateResourcesParams = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Dict[str, Any] = {},
    **kwargs,
) -> Optional[EstimatedResources]:
    assert (
        operation_type == OperationType.PYARROW_DOWNLOAD
    ), "Number of rows can only be estimated for PYARROW_DOWNLOAD operation"

    if delta.meta.record_count and delta.meta.source_content_length:
        # No need to estimate
        return EstimatedResources.of(
            memory_bytes=delta.meta.source_content_length,
            statistics=Statistics.of(
                in_memory_size_bytes=delta.meta.source_content_length,
                record_count=delta.meta.record_count,
                on_disk_size_bytes=delta.meta.content_length,
            ),
        )

    if estimate_resources_params is None:
        estimate_resources_params = EstimateResourcesParams.of()

    functions = RESOURCE_ESTIMATION_METHOD_TO_DELTA_RESOURCE_ESTIMATION_FUNCTIONS.get(
        estimate_resources_params.resource_estimation_method
    )

    for func in functions:
        resources = func(
            delta=delta,
            operation_type=operation_type,
            estimate_resources_params=estimate_resources_params,
            deltacat_storage=deltacat_storage,
            deltacat_storage_kwargs=deltacat_storage_kwargs,
            **kwargs,
        )
        if resources is not None:
            logger.debug(
                f"Estimated resources for delta={delta.locator} is {resources} using {func}"
            )
            return resources

    return None
