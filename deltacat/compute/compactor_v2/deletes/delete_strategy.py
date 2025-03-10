from typing import List, Optional

import pyarrow as pa
from abc import ABC, abstractmethod

from typing import Tuple
from deltacat.compute.compactor_v2.deletes.delete_file_envelope import (
    DeleteFileEnvelope,
)


class DeleteStrategy(ABC):
    """
    Encapsulates a strategy for applying row-level deletes on tables during compaction

    This abstract base class defines the interface for applying delete operations
    on intermediate in-memory pyarrow tables during compaction. Concrete subclasses must implement the `apply_deletes` and
    `apply_many_deletes` methods, as well as the `name` property.

    Example:
        >>> class MyDeleteStrategy(DeleteStrategy):
        ...     @property
        ...     def name(self) -> str:
        ...         return "MyDeleteStrategy"
        ...
        ...     def apply_deletes(self, table: Optional[pa.Table], delete_file_envelope: DeleteFileEnvelope) -> ReturnTuple[pa.Table, int]:
        ...         # Implement delete logic here
        ...         pass
        ...
        ...     def apply_many_deletes(self, table: Optional[pa.Table], delete_file_envelopes: List[DeleteFileEnvelope]) -> ReturnTuple[pa.Table, int]:
        ...         # Implement delete logic here
        ...         pass
    """

    @property
    def name(self) -> str:
        """
        The name of the delete strategy.
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
            table (Optional[pa.Table]): The Arrow table to apply deletes on.
            delete_file_envelopes (List[DeleteFileEnvelope]): A list of delete file envelopes containing delete parameters.

        Returns:
            Tuple[pa.Table, int]: A tuple containing the updated Arrow table after applying all deletes,
                and the total number of rows deleted.
        """
        pass
