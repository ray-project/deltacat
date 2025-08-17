from typing import List, Optional
import logging
import pyarrow as pa
from deltacat import logs

from typing import Callable, Tuple
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from deltacat.compute.compactor_v2.deletes.delete_strategy import (
    DeleteStrategy,
)
import pyarrow.compute as pc
import numpy as np


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class EqualityDeleteStrategy(DeleteStrategy):
    """
    A strategy for applying row-level deletes on tables during compaction based on equality conditions on one or more columns. It
    implements the "equality delete" approach, which marks a row as deleted by one or more column values like pk1=3 or col1="foo", col2="bar".

    Attributes:
        _name (str): The name of the delete strategy.

    Methods:
        name(self) -> str:
            Returns the name of the delete strategy.

        apply_deletes(self, table, delete_file_envelope, *args, **kwargs) -> Tuple[pa.Table, int]:
            Apply delete operations on the given table using the provided delete file envelope.

        apply_many_deletes(self, table, delete_file_envelopes, *args, **kwargs) -> Tuple[pa.Table, int]:
            Apply delete operations on the given table using all provided delete file envelopes.
    """

    _name = "EqualityDeleteStrategy"

    @property
    def name(self) -> str:
        """
        The name of the delete strategy.
        """
        return self._name

    def _drop_rows(
        self,
        table: pa.Table,
        delete_table: pa.Table,
        delete_column_names: List[str],
        equality_predicate_operation: Optional[Callable] = pa.compute.and_,
    ) -> Tuple[pa.Table, int]:
        """
        Drop rows from the given table based on the provided delete table and column names.

        Args:
            table (pa.Table): The input table to drop rows from.
            delete_table (pa.Table): The table containing the values to match for deletion.
            delete_column_names (List[str]): A list of column names to check for equality with the delete table.
            equality_predicate_operation (Optional[Callable], optional): The operation to combine equality predicates for multiple columns.
                Defaults to pa.compute.and_.

        Returns:
            Tuple[pa.Table, int]: A tuple containing the updated table after dropping rows,
                and the number of rows dropped.
        """
        if len(delete_column_names) < 1:
            return table, 0
        # all 1s -> all True so wont discard any from the curr_boolean_mask
        prev_boolean_mask = pa.array(np.ones(len(table), dtype=bool))
        # all 0s -> all False so if mask is never modified all rows will be kept
        curr_boolean_mask = pa.array(np.zeros(len(table), dtype=bool))
        for delete_column_name in delete_column_names:
            if delete_column_name not in table.column_names:
                logger.warning(
                    f"Column name {delete_column_name} not in table column names. Skipping dropping rows for this column."
                )
                continue
            curr_boolean_mask = pc.is_in(
                table[delete_column_name],
                value_set=delete_table[delete_column_name],
            )
            curr_boolean_mask = equality_predicate_operation(
                prev_boolean_mask, curr_boolean_mask
            )
            prev_boolean_mask = curr_boolean_mask
        number_of_rows_before_dropping = len(table)
        logger.debug(
            f"Number of table rows before dropping: {number_of_rows_before_dropping}."
        )
        table = table.filter(pc.invert(curr_boolean_mask))
        number_of_rows_after_dropping = len(table)
        logger.debug(
            f"Number of table rows after dropping: {number_of_rows_after_dropping}."
        )
        dropped_rows = number_of_rows_before_dropping - number_of_rows_after_dropping
        return table, dropped_rows

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
            table (Optional[pa.Table]): The pyarrow table to apply deletes on.
            delete_file_envelope (DeleteFileEnvelope): The delete file envelope containing delete parameters.

        Returns:
            Tuple[pa.Table, int]: A tuple containing the updated Arrow table after applying deletes,
                and the number of rows deleted.
        """
        if not table or not delete_file_envelope.table:
            logger.debug(
                f"No table passed or no delete file envelope delete table found. DeleteFileEnvelope: {delete_file_envelope}"
            )
            return table, 0
        delete_columns = delete_file_envelope.delete_columns
        delete_table = delete_file_envelope.table
        table, number_of_rows_dropped = self._drop_rows(
            table, delete_table, delete_columns
        )
        return table, number_of_rows_dropped

    def apply_many_deletes(
        self,
        table: Optional[pa.Table],
        delete_file_envelopes: List[DeleteFileEnvelope],
        *args,
        **kwargs,
    ) -> Tuple[pa.Table, int]:
        """
        Apply delete operations on the given table using all provided delete file envelopes.

        Args:
            table (Optional[pa.Table]): The pyarrow table to apply deletes on.
            delete_file_envelopes (List[DeleteFileEnvelope]): A list of delete file envelopes containing delete parameters.

        Returns:
            Tuple[pa.Table, int]: A tuple containing the updated pyarrow table after applying all deletes,
                and the total number of rows deleted.
        """
        # looking up table references are lighter than the actual table
        if not table:
            logger.debug("No table passed.")
            return table, 0
        total_dropped_rows = 0
        for delete_file_envelope in delete_file_envelopes:
            if delete_file_envelope.table_reference is None:
                continue
            table, number_of_rows_dropped = self.apply_deletes(
                table, delete_file_envelope
            )
            total_dropped_rows += number_of_rows_dropped
        return table, total_dropped_rows
