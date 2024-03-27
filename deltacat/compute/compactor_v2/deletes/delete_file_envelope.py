# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from typing import Any, List, Optional
from deltacat.storage import DeltaType, LocalTable
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
)
import numpy as np
import pyarrow as pa

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
        table_storage_strategy: LocalTableStorageStrategy = LocalTableRayObjectStoreReferenceStorageStrategy(),
    ) -> DeleteFileEnvelope:
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
            table_storage_strategy: The way the table object is stored in the delta file envelope. Defaults to LocalTableRayObjectStoreReferenceStorageStrategy
        Returns:
            A delete file envelope.

        """
        delete_file_envelope = DeltaFileEnvelope.of(
            stream_position,
            delta_type,
            table,
            file_index,
            is_src_delta,
            file_record_count,
            table_storage_strategy,
        )
        assert len(delete_columns) > 0, "At least 1 delete column is expected"
        delete_file_envelope["delete_columns"] = delete_columns
        if isinstance(table, pa.Table):
            delete_file_envelope["table_size_bytes"] = table.nbytes
        return DeleteFileEnvelope(**delete_file_envelope)

    @property
    def table_size_bytes(self) -> int:
        val = self.get("table_size_bytes")
        if val is not None:
            return val
        else:
            raise ValueError(
                f"Table type: {type(self.table)} not for supported for size method."
            )

    @property
    def delete_columns(self) -> List[str]:
        return self["delete_columns"]

    @property
    def table_reference(self) -> Optional[Any]:
        if self.table_storage_strategy is not None and isinstance(
            self.table_storage_strategy,
            LocalTableRayObjectStoreReferenceStorageStrategy,
        ):
            return self.table_storage_strategy.get_table_reference(self["table"])
        return None
