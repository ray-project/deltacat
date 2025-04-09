class RowFilter:...

class ColumnFilter:...

class PartitionFilter:...

class Pushdown:
    row_filter: RowFilter
    column_filter: ColumnFilter
    partition_filter: PartitionFilter
    limit: int
