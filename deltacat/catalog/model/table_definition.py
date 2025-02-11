# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Any

from deltacat.storage import Stream, Table, TableVersion


class TableDefinition(dict):
    @staticmethod
    def of(
        table: Table,
        table_version: TableVersion,
        stream: Stream,
        native_object: Optional[Any] = None,
    ) -> TableDefinition:
        return TableDefinition(
            {
                "table": table,
                "tableVersion": table_version,
                "stream": stream,
                "nativeObject": native_object,
            }
        )

    @property
    def table(self) -> Table:
        return self["table"]

    @property
    def table_version(self) -> TableVersion:
        return self["tableVersion"]

    @property
    def stream(self) -> Stream:
        return self["stream"]

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")
