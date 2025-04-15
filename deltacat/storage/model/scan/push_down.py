class RowFilter:
    ...


class ColumnFilter:
    ...


class PartitionFilter:
    ...


class Pushdown:
    """Represents pushdown predicates to be applied for DeltaCAT Tables"""

    row_filter: RowFilter
    column_filter: ColumnFilter
    partition_filter: PartitionFilter
    limit: int
