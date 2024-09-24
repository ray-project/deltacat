import logging
import ray
import functools
from deltacat.compute.compactor_v2.constants import (
    TASK_MAX_PARALLELISM,
    MAX_PARQUET_METADATA_SIZE,
)
from deltacat.utils.ray_utils.concurrency import invoke_parallel
from deltacat import logs
from deltacat.storage import (
    Delta,
    ManifestEntry,
    interface as unimplemented_deltacat_storage,
)
from typing import Dict, Optional, Any
from deltacat.types.media import TableType
from deltacat.types.media import ContentType
from deltacat.types.partial_download import PartialParquetParameters
from deltacat.compute.compactor_v2.utils.task_options import (
    append_content_type_params_options_provider,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _contains_partial_parquet_parameters(entry: ManifestEntry) -> bool:
    return (
        entry.meta
        and entry.meta.content_type_parameters
        and any(
            isinstance(type_params, PartialParquetParameters)
            for type_params in entry.meta.content_type_parameters
        )
    )


@ray.remote
def _download_parquet_meta_for_manifest_entry(
    delta: Delta,
    entry_index: int,
    deltacat_storage: unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[Any, Any]] = {},
) -> Dict[str, Any]:
    pq_file = deltacat_storage.download_delta_manifest_entry(
        delta,
        entry_index=entry_index,
        table_type=TableType.PYARROW_PARQUET,
        **deltacat_storage_kwargs,
    )

    return {
        "entry_index": entry_index,
        "partial_parquet_params": PartialParquetParameters.of(
            pq_metadata=pq_file.metadata
        ),
    }


def append_content_type_params(
    delta: Delta,
    task_max_parallelism: int = TASK_MAX_PARALLELISM,
    max_parquet_meta_size_bytes: Optional[int] = MAX_PARQUET_METADATA_SIZE,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = {},
) -> None:

    if not delta.meta:
        logger.warning(f"Delta with locator {delta.locator} doesn't contain meta.")
        return

    entry_indices_to_download = []
    for entry_index, entry in enumerate(delta.manifest.entries):
        if (
            not _contains_partial_parquet_parameters(entry)
            and entry.meta
            and entry.meta.content_type == ContentType.PARQUET.value
        ):
            entry_indices_to_download.append(entry_index)

    if not entry_indices_to_download:
        logger.info(f"No parquet entries found for delta with locator {delta.locator}.")
        return

    options_provider = functools.partial(
        append_content_type_params_options_provider,
        max_parquet_meta_size_bytes=max_parquet_meta_size_bytes,
    )

    def input_provider(index, item) -> Dict:
        return {
            "deltacat_storage_kwargs": deltacat_storage_kwargs,
            "deltacat_storage": deltacat_storage,
            "delta": delta,
            "entry_index": item,
        }

    logger.info(
        f"Downloading parquet meta for {len(entry_indices_to_download)} manifest entries..."
    )
    pq_files_promise = invoke_parallel(
        entry_indices_to_download,
        ray_task=_download_parquet_meta_for_manifest_entry,
        max_parallelism=task_max_parallelism,
        options_provider=options_provider,
        kwargs_provider=input_provider,
    )

    partial_file_meta_list = ray.get(pq_files_promise)

    logger.info(
        f"Downloaded parquet meta for {len(entry_indices_to_download)} manifest entries"
    )

    assert len(partial_file_meta_list) == len(
        entry_indices_to_download
    ), f"Expected {len(entry_indices_to_download)} pq files, got {len(partial_file_meta_list)}"

    for index, entry_index in enumerate(entry_indices_to_download):
        assert (
            entry_index == partial_file_meta_list[index]["entry_index"]
        ), "entry_index must match with the associated parquet meta"
        entry = delta.manifest.entries[entry_index]
        if not entry.meta.content_type_parameters:
            entry.meta.content_type_parameters = []
        entry.meta.content_type_parameters.append(
            partial_file_meta_list[index]["partial_parquet_params"]
        )

    for entry_index, entry in enumerate(delta.manifest.entries):
        assert _contains_partial_parquet_parameters(
            entry
        ), "partial parquet params validation failed."
