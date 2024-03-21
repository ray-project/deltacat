from typing import List, Optional

import pyarrow as pa
from abc import ABC

from typing import Tuple
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)


class EqualityDeleteStrategy(ABC):
    """ """

    _name = "EqualityDeleteStrategy"

    @property
    def name(self) -> str:
        """
        The name of the delete strategy.
        """
        self._name

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
        return table, 0

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
        return table, 0
