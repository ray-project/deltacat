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
        delta_file_envelope = DeltaFileEnvelope()
        delta_file_envelope["streamPosition"] = stream_position
        delta_file_envelope["fileIndex"] = file_index
        delta_file_envelope["deltaType"] = delta_type.value
        delta_file_envelope["table"] = table
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
    def table(self) -> LocalTable:
        return self["table"]
