from deltacat.compute.stats.types import StatsType


class StatsResult(dict):
    @staticmethod
    def of(row_count: int, pyarrow_table_bytes: int):
        sr = StatsResult()
        sr[StatsType.ROW_COUNT.value] = row_count
        sr[StatsType.PYARROW_TABLE_BYTES.value] = pyarrow_table_bytes
        return sr

    @property
    def row_count(self) -> int:
        return self[StatsType.ROW_COUNT.value]

    @property
    def pyarrow_table_bytes(self) -> int:
        return self[StatsType.PYARROW_TABLE_BYTES.value]
