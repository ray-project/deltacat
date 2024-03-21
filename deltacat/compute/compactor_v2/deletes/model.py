from deltacat.compute.compactor import (
    DeltaAnnotated,
)

from typing import List, Optional

from dataclasses import dataclass
import pyarrow as pa
from abc import ABC, abstractmethod
from deltacat.storage import DeltaType, LocalTable
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
)
import numpy as np

from typing import Tuple, Any, Dict
from deltacat.compute.compactor.model.table_object_store import (
    LocalTableStorageStrategy,
    LocalTableRayObjectStoreReferenceStorageStrategy,
)


class DeleteFileEnvelope(DeltaFileEnvelope):
    @staticmethod
    def of(
        stream_position: int,
        delta_type: DeltaType,
        table: LocalTable,
        delete_columns: List[str],
        file_index: int = None,
        is_src_delta: np.bool_ = True,
        file_record_count: Optional[int] = None,
        table_storage_strategy: [
            LocalTableStorageStrategy
        ] = LocalTableRayObjectStoreReferenceStorageStrategy(),
    ) -> DeltaFileEnvelope:
        """
        Static factory builder for a DeleteFileEnvelope. Subclasses from DeltaFileEnvelope
        `
        Args:
            stream_position: Stream position of a delta.
            delta_type: A delta type.
            table: The table object that represents the delta file.
            delete_columns: delete column_names needed for equality-based deletes,
            file_index: Manifest file index number of a delta.
            is_src_delta: True if this Delta File Locator is
                pointing to a file from the uncompacted source table, False if
                this Locator is pointing to a file in the compacted destination
                table.

        Returns:
            A delete file envelope.

        """
        delete_file_envelope = super().of(
            stream_position,
            delta_type,
            table,
            file_index,
            is_src_delta,
            file_record_count,
            table_storage_strategy,
        )
        delete_file_envelope["delete_columns"] = delete_columns
        return DeleteFileEnvelope(**delete_file_envelope)

    @property
    def delete_columns(self) -> List[str]:
        return self["delete_columns"]


@dataclass
class PrepareDeleteResult:
    """
    TODO: pfaraone
    """

    transformed_deltas: [List[DeltaAnnotated]]
    delete_file_envelopes: List[DeleteFileEnvelope]


class DeleteStrategy(ABC):
    """
    TODO: pfaraone
    """

    @property
    def name(self):
        pass

    @abstractmethod
    def prepare_deletes(
        self,
        params,
        input_deltas: List[DeltaAnnotated],
        *args,
        **kwargs,
    ) -> PrepareDeleteResult:
        pass

    @abstractmethod
    def match_deletes(
        self,
        index_identifier: int,
        sorted_df_envelopes: List[DeltaFileEnvelope],
        delete_file_envelopes: List[DeleteFileEnvelope],
        *args,
        **kwargs,
    ) -> Tuple[List[int], Dict[str, Any]]:
        pass

    @abstractmethod
    def rebatch_df_envelopes(
        self,
        index_identifier: int,
        df_envelopes: List[DeltaFileEnvelope],
        delete_locations: List[Any],
        *args,
        **kwargs,
    ) -> List[List[DeltaFileEnvelope]]:
        pass

    @abstractmethod
    def apply_deletes(
        self,
        index_identifier: int,
        table: Optional[pa.Table],
        delete_file_envelope: DeleteFileEnvelope,
        *args,
        **kwargs,
    ) -> Tuple[Any, int]:
        pass

    @abstractmethod
    def apply_all_deletes(
        self,
        index_identifier: int,
        delete_file_envelopes: List[DeleteFileEnvelope],
        *args,
        **kwargs,
    ):
        pass
