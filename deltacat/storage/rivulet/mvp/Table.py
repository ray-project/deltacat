from dataclasses import dataclass
from typing import List, Dict, Any
from typing import Iterable

from collections.abc import Mapping

from pyarrow import RecordBatch


@dataclass
class MvpRow(Mapping):
    data: Dict[str, Any]

    def __getitem__(self, key):
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def __contains__(self, key):
        return key in self.data

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    def get(self, key, default=None):
        return self.data.get(key, default)

    @staticmethod
    def generate_from_arrow(batch: RecordBatch):
        for row_idx in range(batch.num_rows):
            out = {}
            for column_idx, column in enumerate(batch.column_names):
                col = batch.column(column_idx)
                out.update({column: col[row_idx].as_py()})
                yield MvpRow(out)


@dataclass
class MvpTable(Iterable[Dict[str, Any]]):
    data: Dict[str, List[Any]]

    def __iter__(self):
        # Get the lengths of all columns (they should be the same)
        row_count = len(next(iter(self.data.values())))

        # Iterate over the rows
        for i in range(row_count):
            row_data = {
                field_name: field_arr[i] for field_name, field_arr in self.data.items()
            }
            yield row_data

    def to_rows_by_key(self, mk: str) -> Dict[str, "MvpRow"]:
        # Find the provided key field in the schema
        # build row data
        pk_col = self.data[mk]
        row_data: Dict[str, MvpRow] = {}
        for i, value in enumerate(pk_col):
            row_data[value] = MvpRow(
                {
                    field_name: field_arr[i]
                    for field_name, field_arr in self.data.items()
                }
            )
        return row_data

    def to_rows_list(self) -> List[Dict[str, Any]]:
        return [r for r in self]

    @classmethod
    def merge(cls, dataset1: "MvpTable", dataset2: "MvpTable", pk: str) -> "MvpTable":

        merged_data: Dict[str, List[Any]] = {}
        # Initialize merged_data with keys from both datasets
        for k in set(dataset1.data.keys()) | set(dataset2.data.keys()):
            merged_data[k] = []

        # Create dictionaries for quick lookup
        row_data_ds1: dict[str, MvpRow] = dataset1.to_rows_by_key(pk)
        row_data_ds2: dict[str, MvpRow] = dataset2.to_rows_by_key(pk)

        # Merge the datasets
        all_keys = set(row_data_ds1.keys()) | set(row_data_ds2.keys())
        for k in all_keys:
            row1: MvpRow = row_data_ds1.get(k, MvpRow({}))
            row2: MvpRow = row_data_ds2.get(k, MvpRow({}))
            merged_row = {**row1, **row2}
            for column, values in merged_data.items():
                values.append(merged_row.get(column))

        return cls(merged_data)
