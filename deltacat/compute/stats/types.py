from enum import Enum
from typing import Set


class StatsType(str, Enum):
    ROW_COUNT = "rowCount"
    PYARROW_TABLE_BYTES = "pyarrowTableBytes"


ALL_STATS_TYPES: Set[StatsType] = {field for field in StatsType}
