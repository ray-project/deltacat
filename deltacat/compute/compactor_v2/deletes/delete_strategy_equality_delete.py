from typing import List, Optional
import logging
import pyarrow as pa
import pyarrow.compute as pc
from deltacat import logs

from typing import Tuple
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)
from deltacat.compute.compactor_v2.deletes.delete_strategy import (
    DeleteStrategy,
)

_NULL_SENTINEL = "__DELTACAT_NULL_SENTINEL__"


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

    @staticmethod
    def _coerce_keys_for_null_safe_join(tbl: pa.Table, columns: List[str]) -> pa.Table:
        """Cast key columns to string and replace nulls with a sentinel
        so that PyArrow's hash-join treats null keys as equal."""
        for col in columns:
            arr = tbl.column(col)
            if arr.type == pa.null():
                arr = pa.nulls(len(arr), type=pa.string())
            elif arr.type != pa.string():
                arr = pc.cast(arr, pa.string())
            arr = pc.if_else(pc.is_null(arr), pa.scalar(_NULL_SENTINEL), arr)
            tbl = tbl.set_column(tbl.schema.get_field_index(col), col, arr)
        return tbl

    def _drop_rows(
        self,
        table: pa.Table,
        delete_table: pa.Table,
        delete_column_names: List[str],
    ) -> Tuple[pa.Table, int]:
        """
        Drop rows from the given table based on the provided delete table and column names.

        Uses a left anti-join to match rows across all key columns simultaneously,
        ensuring correct row-wise matching for composite (multi-column) delete keys.

        Null values in key columns are treated as equal (null == null) for
        deletion matching, which differs from default SQL join semantics.

        Args:
            table (pa.Table): The input table to drop rows from.
            delete_table (pa.Table): The table containing the values to match for deletion.
            delete_column_names (List[str]): A list of column names to check for equality with the delete table.

        Returns:
            Tuple[pa.Table, int]: A tuple containing the updated table after dropping rows,
                and the number of rows dropped.
        """
        if len(delete_column_names) < 1:
            return table, 0

        valid_columns = []
        for col in delete_column_names:
            if col not in table.column_names:
                logger.warning(
                    f"Column name {col} not in table column names. "
                    f"Skipping dropping rows for this column."
                )
                continue
            valid_columns.append(col)

        if not valid_columns:
            return table, 0

        number_of_rows_before_dropping = len(table)
        logger.debug(
            f"Number of table rows before dropping: {number_of_rows_before_dropping}."
        )

        has_nulls = any(
            table.schema.field(c).type == pa.null()
            or delete_table.schema.field(c).type == pa.null()
            or table.column(c).null_count > 0
            or delete_table.column(c).null_count > 0
            for c in valid_columns
        )

        if has_nulls:
            # Null-safe path: stringify keys with sentinel, then join on
            # row indices to preserve original column types and null values.
            idx_name = "__deltacat_row_idx__"
            table_keys = table.append_column(
                idx_name, pa.array(range(len(table)), type=pa.int64())
            ).select(valid_columns + [idx_name])
            table_keys = self._coerce_keys_for_null_safe_join(table_keys, valid_columns)

            delete_keys = delete_table.select(valid_columns)
            delete_keys = self._coerce_keys_for_null_safe_join(
                delete_keys, valid_columns
            )
            delete_keys = delete_keys.group_by(valid_columns).aggregate([])

            surviving = table_keys.join(
                delete_keys, keys=valid_columns, join_type="left anti"
            )
            table = table.take(surviving.column(idx_name))
        else:
            delete_keys = (
                delete_table.select(valid_columns).group_by(valid_columns).aggregate([])
            )
            table = table.join(delete_keys, keys=valid_columns, join_type="left anti")

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
