from typing import List, Optional

import pyarrow as pa
from abc import ABC

from typing import Callable, Tuple
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
import pyarrow.compute as pc
import numpy as np


class EqualityDeleteStrategy(ABC):
    """ """

    _name = "EqualityDeleteStrategy"

    @property
    def name(self) -> str:
        """
        The name of the delete strategy.
        """
        self._name

    def _drop_rows(
        self,
        table: pa.Table,
        deletes_to_apply,
        delete_column_names: List[str],
        equality_predicate_operation: Optional[Callable] = pa.compute.and_,
    ) -> Tuple[pa.Table, int]:
        prev_boolean_mask = pa.array(np.ones(len(table), dtype=bool))
        # all 1s -> all True so wont discard any from the curr_boolean_mask
        for delete_column_name in delete_column_names:
            curr_boolean_mask = pc.is_in(
                table[delete_column_name],
                value_set=deletes_to_apply[delete_column_name],
            )
            result = equality_predicate_operation(prev_boolean_mask, curr_boolean_mask)
            prev_boolean_mask = result
        number_of_rows_before_dropping = len(table)
        table = table.filter(pc.invert(result))
        number_of_rows_after_dropping = len(table)
        return table, abs(
            number_of_rows_after_dropping - number_of_rows_before_dropping
        )

    def apply_deletes(
        self,
        table: Optional[pa.Table],
        delete_file_envelope: DeleteFileEnvelope,
        *args,
        **kwargs,
    ) -> Tuple[pa.Table, int]:
        """
        Apply delete operations on the given table using the provided delete file envelope.

        Args:
            table (Optional[pa.Table]): The pyArrow table to apply deletes on.
            delete_file_envelope (DeleteFileEnvelope): The delete file envelope containing delete parameters.

        Returns:
            Tuple[pa.Table, int]: A tuple containing the updated Arrow table after applying deletes,
                and the number of rows deleted.
        """
        if not table:
            return table, 0
        delete_columns = delete_file_envelope.delete_columns
        delete_table = delete_file_envelope.table
        table, number_of_rows_dropped = self._drop_rows(
            table, delete_table, delete_columns
        )
        return table, number_of_rows_dropped

    def apply_all_deletes(
        self,
        table: Optional[pa.Table],
        delete_file_envelopes: List[DeleteFileEnvelope],
        *args,
        **kwargs,
    ) -> Tuple[pa.Table, int]:
        """
        Apply delete operations on the given table using all provided delete file envelopes.

        Args:
            table (Optional[pa.Table]): The Arrow table to apply deletes on.
            delete_file_envelopes (List[DeleteFileEnvelope]): A list of delete file envelopes containing delete parameters.

        Returns:
            Tuple[pa.Table, int]: A tuple containing the updated Arrow table after applying all deletes,
                and the total number of rows deleted.
        """
        if not table:
            return table, 0
        total_dropped_rows = 0
        for delete_file_envelope in delete_file_envelopes:
            delete_columns = delete_file_envelope.delete_columns
            delete_table = delete_file_envelope.table
            table, number_of_rows_dropped = self._drop_rows(
                table, delete_table, delete_columns
            )
            total_dropped_rows += number_of_rows_dropped
        return table, total_dropped_rows
