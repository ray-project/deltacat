from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from deltacat.storage.model.expression import Expression


class RowFilter:
    ...


class ColumnFilter:
    ...


@dataclass
class PartitionFilter:
    expr: Expression

    @staticmethod
    def of(expr: Expression) -> PartitionFilter:
        return PartitionFilter(expr)


@dataclass
class Pushdown:
    """Represents pushdown predicates to be applied for DeltaCAT Tables"""

    row_filter: Optional[RowFilter]
    column_filter: Optional[ColumnFilter]
    partition_filter: Optional[PartitionFilter]
    limit: Optional[int]

    @staticmethod
    def of(
        row_filter: Optional[RowFilter],
        column_filter: Optional[ColumnFilter],
        partition_filter: Optional[PartitionFilter],
        limit: Optional[int],
    ) -> Pushdown:
        return Pushdown(
            row_filter=row_filter,
            column_filter=column_filter,
            partition_filter=partition_filter,
            limit=limit,
        )
