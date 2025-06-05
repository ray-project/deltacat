import logging
import ray
import functools
from deltacat.compute.compactor_v2.constants import (
    TASK_MAX_PARALLELISM,
    MAX_PARQUET_METADATA_SIZE,
)
from deltacat.utils.common import ReadKwargsProvider
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
from deltacat.exceptions import RetryableError

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def append_content_type_params_options_provider(
    index: int, item: Any, max_parquet_meta_size_bytes: int, **kwargs
) -> Dict:
    task_opts = {
        "num_cpus": 0.01,
        "memory": max_parquet_meta_size_bytes,
        "scheduling_strategy": "DEFAULT",
    }

    task_opts["max_retries"] = 3
    task_opts["retry_exceptions"] = [RetryableError]

    return task_opts


def _contains_partial_parquet_parameters(entry: ManifestEntry) -> bool:
    return (
        entry.meta
        and entry.meta.content_type_parameters
        and any(
            isinstance(type_params, PartialParquetParameters)
            for type_params in entry.meta.content_type_parameters
        )
    )


APPEND_CONTENT_TYPE_PARAMS_CACHE = "append_content_type_params_cache"
# At this point, it's better to fetch all parquet than to cache and
# call actor which is not expected to support high throughput.
MINIMUM_ENTRIES_TO_CACHE = 10


@ray.remote
class AppendContentTypeParamsCache:
    """
    This actor caches the delta that contains content type meta.
    """

    def __init__(self):
        self.cache = {}

    def get(self, key):
        return self.cache.get(key)

    def put(self, key, value):
        self.cache[key] = value


@ray.remote
def _download_parquet_metadata_for_manifest_entry(
    delta: Delta,
    entry_index: int,
    deltacat_storage: unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[Any, Any]] = {},
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
) -> Dict[str, Any]:
    logger.info(
        f"Downloading the parquet metadata for Delta with locator {delta.locator} and entry_index: {entry_index}"
    )
    if "file_reader_kwargs_provider" in deltacat_storage_kwargs:
        logger.info(
            "'file_reader_kwargs_provider' is also present in deltacat_storage_kwargs. Removing to prevent multiple values for keyword argument"
        )
        deltacat_storage_kwargs.pop("file_reader_kwargs_provider")
    pq_file = deltacat_storage.download_delta_manifest_entry(
        delta,
        entry_index=entry_index,
        table_type=TableType.PYARROW_PARQUET,
        file_reader_kwargs_provider=file_reader_kwargs_provider,
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
    file_reader_kwargs_provider: Optional[ReadKwargsProvider] = None,
) -> bool:
    """
    This operation appends content type params into the delta entry. Note
    that this operation can be time consuming, hence we cache it in a Ray actor.
    """
    logger.info(
        f"Appending the content type params for Delta with locator {delta.locator}..."
    )

    if not delta.meta:
        logger.warning(f"Delta with locator {delta.locator} doesn't contain meta.")
        return False

    entry_indices_to_download = []
    for entry_index, entry in enumerate(delta.manifest.entries):
        if (
            not _contains_partial_parquet_parameters(entry)
            and entry.meta
            and entry.meta.content_type == ContentType.PARQUET.value
        ):
            entry_indices_to_download.append(entry_index)

    if not entry_indices_to_download:
        logger.info(
            f"No parquet type params to download for delta with locator {delta.locator}."
        )
        return False

    ray_namespace = ray.get_runtime_context().namespace
    logger.info(
        f"Got Ray namespace: {ray_namespace}. "
        "Note that caching only works with non-anonymous namespace."
        "To set a non-anonymous namespace, call ray.init(namespace='X')."
    )
    if len(entry_indices_to_download) >= MINIMUM_ENTRIES_TO_CACHE:
        logger.info(
            f"Checking if cache contains parquet meta in namespace {ray_namespace} for "
            f"delta locator {delta.locator} and digest {delta.locator.hexdigest()}..."
        )
        cache = AppendContentTypeParamsCache.options(
            name=APPEND_CONTENT_TYPE_PARAMS_CACHE,
            namespace=ray_namespace,
            get_if_exists=True,
        ).remote()

        logger.info(f"Got cache actor: {cache}")
        cached_value = ray.get(cache.get.remote(delta.locator.hexdigest()))
        if cached_value is not None:
            logger.info(
                "Using cached parquet meta for delta with locator"
                f" {delta.locator} and digest {delta.locator.hexdigest()}."
            )
            delta.manifest = cached_value.manifest
            return True
        logger.info(
            f"Cache doesn't contain parquet meta for delta with locator {delta.locator}."
        )

    options_provider = functools.partial(
        append_content_type_params_options_provider,
        max_parquet_meta_size_bytes=max_parquet_meta_size_bytes,
    )

    def input_provider(index, item) -> Dict:
        return {
            "file_reader_kwargs_provider": file_reader_kwargs_provider,
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
        ray_task=_download_parquet_metadata_for_manifest_entry,
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

    if len(entry_indices_to_download) >= MINIMUM_ENTRIES_TO_CACHE:
        cache = AppendContentTypeParamsCache.options(
            name=APPEND_CONTENT_TYPE_PARAMS_CACHE,
            namespace=ray_namespace,
            get_if_exists=True,
        ).remote()
        logger.info(f"Got cache actor when writing: {cache}")
        logger.info(
            f"Caching parquet meta for delta with locator {delta.locator} "
            f"and digest {delta.locator.hexdigest()}..."
        )
        ray.get(cache.put.remote(delta.locator.hexdigest(), delta))
        assert ray.get(cache.get.remote(delta.locator.hexdigest())) is not None

    return True
