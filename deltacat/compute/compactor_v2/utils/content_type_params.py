import logging
from deltacat import logs
from deltacat.storage import (
    Delta,
    interface as unimplemented_deltacat_storage,
)
from typing import Dict, Optional, Any
from deltacat.types.media import TableType, StorageType
from deltacat.types.media import ContentType
from deltacat.types.partial_download import PartialParquetParameters

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def append_content_type_params(
    delta: Delta,
    entry_index: Optional[int] = None,
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = {},
) -> None:

    if delta.meta.content_type != ContentType.PARQUET.value:
        logger.info(
            f"Delta with locator {delta.locator} is not a parquet delta, "
            "skipping appending content type parameters."
        )
        return

    manifest_entries = delta.manifest.entries
    ordered_pq_meta = []

    if entry_index is not None:
        manifest_entries = [delta.manifest.entries[entry_index]]

        pq_file = deltacat_storage.download_delta_manifest_entry(
            delta,
            entry_index=entry_index,
            table_type=TableType.PYARROW_PARQUET,
            **deltacat_storage_kwargs,
        )

        partial_file_meta = PartialParquetParameters.of(pq_metadata=pq_file.metadata)
        ordered_pq_meta.append(partial_file_meta)

    else:
        pq_files = deltacat_storage.download_delta(
            delta,
            table_type=TableType.PYARROW_PARQUET,
            storage_type=StorageType.LOCAL,
            **deltacat_storage_kwargs,
        )

        assert len(pq_files) == len(
            manifest_entries
        ), f"Expected {len(manifest_entries)} pq files, got {len(pq_files)}"

        ordered_pq_meta = [
            PartialParquetParameters.of(pq_metadata=pq_file.metadata)
            for pq_file in pq_files
        ]

    for entry_index, entry in enumerate(manifest_entries):
        if not entry.meta.content_type_parameters:
            entry.meta.content_type_parameters = []

        entry.meta.content_type_parameters.append(ordered_pq_meta[entry_index])
