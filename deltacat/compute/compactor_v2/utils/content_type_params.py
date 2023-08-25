from deltacat.storage import (
    Delta,
    interface as unimplemented_deltacat_storage,
)
from typing import Dict, Optional, Any
from deltacat.types.media import TableType
from deltacat.types.media import ContentType
from deltacat.types.partial_download import PartialParquetParameters


def append_content_type_params(
    delta: Delta,
    entry_index: int,
    deltacat_storage: unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[Dict[str, Any]] = {},
):

    entry = delta.manifest.entries[entry_index]

    if entry.meta.content_type == ContentType.PARQUET:
        # Set partial download content type parameters to allow
        # row group level rebatching for parquet.
        pq_file = deltacat_storage.download_delta_manifest_entry(
            delta.locator,
            entry_index=entry_index,
            table_type=TableType.PYARROW_PARQUET,
            **deltacat_storage_kwargs,
        )

        if not entry.meta.content_type_parameters:
            entry.meta.content_type_parameters = []

        entry.meta.content_type_parameters.append(
            PartialParquetParameters.of(pq_metadata=pq_file.metadata)
        )

    return entry
