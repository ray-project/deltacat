from typing import List, Optional
from deltacat.storage import DeltaType, LocalTable
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
)
import numpy as np

from deltacat.compute.compactor.model.table_object_store import (
    LocalTableStorageStrategy,
    LocalTableRayObjectStoreReferenceStorageStrategy,
)


class DeleteFileEnvelope(DeltaFileEnvelope):
    @staticmethod
    def of(
        stream_position: int,
        delta_type: DeltaType,
        table: LocalTable,
        delete_columns: List[str],
        file_index: int = None,
        is_src_delta: np.bool_ = True,
        file_record_count: Optional[int] = None,
        table_storage_strategy: [
            LocalTableStorageStrategy
        ] = LocalTableRayObjectStoreReferenceStorageStrategy(),
    ) -> DeltaFileEnvelope:
        """
        Static factory builder for a DeleteFileEnvelope. Subclasses from DeltaFileEnvelope
        `
        Args:
            stream_position: Stream position of a delta.
            delta_type: A delta type.
            table: The table object that represents the delta file.
            delete_columns: delete column_names needed for equality-based deletes,
            file_index: Manifest file index number of a delta.
            is_src_delta: True if this Delta File Locator is
                pointing to a file from the uncompacted source table, False if
                this Locator is pointing to a file in the compacted destination
                table.

        Returns:
            A delete file envelope.

        """
        delete_file_envelope = super().of(
            stream_position,
            delta_type,
            table,
            file_index,
            is_src_delta,
            file_record_count,
            table_storage_strategy,
        )
        delete_file_envelope["delete_columns"] = delete_columns
        return DeleteFileEnvelope(**delete_file_envelope)

    @property
    def delete_columns(self) -> List[str]:
        return self["delete_columns"]
