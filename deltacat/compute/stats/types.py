from enum import Enum


class StatsType(str, Enum):
    ROW_COUNT = "rowCount"
    PYARROW_TABLE_BYTES = "pyArrowTableBytes"
