# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import Table, TableVersion, Stream


class TableDefinition(dict):
    @staticmethod
    def of(table: Table,
           table_version: TableVersion,
           stream: Stream) -> TableDefinition:
        return TableDefinition({
            "table": table,
            "tableVersion": table_version,
            "stream": stream,
        })

    @property
    def table(self) -> Table:
        return self["table"]

    @property
    def table_version(self) -> TableVersion:
        return self["tableVersion"]

    @property
    def stream(self) -> Stream:
        return self["stream"]
