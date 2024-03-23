# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import numpy as np
import pyarrow as pa

from deltacat.storage import DeltaType, LocalTable
from deltacat.compute.compactor.model.table_object_store import (
    LocalTableStorageStrategy,
)

from typing import Optional

DeltaFileEnvelopeGroups = np.ndarray


class DeltaFileEnvelope(dict):
    @staticmethod
    def of(
        stream_position: int,
        delta_type: DeltaType,
        table: LocalTable,
        file_index: int = None,
        is_src_delta: np.bool_ = True,
        file_record_count: Optional[int] = None,
        table_storage_strategy: [LocalTableStorageStrategy] = None,
    ) -> DeltaFileEnvelope:
        """
        Static factory builder for a Delta File Envelope
        `
        Args:
            stream_position: Stream position of a delta.
            delta_type: A delta type.
            table: The table object that represents the delta file.
            file_index: Manifest file index number of a delta.
            is_src_delta: True if this Delta File Locator is
                pointing to a file from the uncompacted source table, False if
                this Locator is pointing to a file in the compacted destination
                table.
            table_storage_strategy: The way the table object is stored in the delta file envelope. If None just stores the table normally
        Returns:
            A delta file envelope.

        """
        if stream_position is None:
            raise ValueError("Missing delta file envelope stream position.")
        if delta_type is None:
            raise ValueError("Missing Delta file envelope delta type.")
        if table is None:
            raise ValueError("Missing Delta file envelope table.")
        delta_file_envelope = DeltaFileEnvelope()
        delta_file_envelope["streamPosition"] = stream_position
        delta_file_envelope["fileIndex"] = file_index
        delta_file_envelope["deltaType"] = delta_type.value
        if table_storage_strategy is None:
            delta_file_envelope["table"] = table
        else:
            delta_file_envelope["table"] = table_storage_strategy.store_table(table)
        delta_file_envelope["table_storage_strategy"] = table_storage_strategy
        delta_file_envelope["is_src_delta"] = is_src_delta
        delta_file_envelope["file_record_count"] = file_record_count
        return delta_file_envelope

    @property
    def stream_position(self) -> int:
        return self["streamPosition"]

    @property
    def file_index(self) -> int:
        return self["fileIndex"]

    @property
    def delta_type(self) -> DeltaType:
        return DeltaType(self["deltaType"])

    @property
    def table_storage_strategy(self) -> Optional[LocalTableStorageStrategy]:
        return self["table_storage_strategy"]

    @property
    def table(self) -> LocalTable:
        val = self.table_storage_strategy
        if val is not None:
            table_storage_strategy = val
            return table_storage_strategy.get_table(self["table"])
        return self["table"]

    @property
    def is_src_delta(self) -> np.bool_:
        return self["is_src_delta"]

    @property
    def file_record_count(self) -> int:
        return self["file_record_count"]

    @property
    def table_size_bytes(self) -> int:
        if isinstance(self.table, pa.Table):
            return self.table.nbytes
        else:
            raise ValueError(
                f"Table type: {type(self.table)} not for supported for size method."
            )

    @property
    def table_num_rows(self) -> int:
        return len(self.table)
