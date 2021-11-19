# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import DeltaType, LocalTable


class DeltaFileEnvelope(dict):
    @staticmethod
    def of(stream_position: int,
           file_index: int,
           delta_type: DeltaType,
           table: LocalTable) -> DeltaFileEnvelope:
        if stream_position is None:
            raise ValueError("Missing delta file envelope stream position.")
        if file_index is None:
            raise ValueError("Missing delta file envelope file index.")
        if delta_type is None:
            raise ValueError("Missing Delta file envelope delta type.")
        if table is None:
            raise ValueError("Missing Delta file envelope table.")
        return DeltaFileEnvelope({
            "stream_position": stream_position,
            "file_index": file_index,
            "delta_type": delta_type.value,
            "table": table,
        })

    @property
    def stream_position(self) -> int:
        return self["stream_position"]

    @property
    def file_index(self) -> int:
        return self["file_index"]

    @property
    def delta_type(self) -> DeltaType:
        return DeltaType(self["delta_type"])

    @property
    def table(self) -> LocalTable:
        return self["table"]
