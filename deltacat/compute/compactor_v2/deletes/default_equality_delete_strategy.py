from deltacat.compute.compactor_v2.deletes.model import (
    DeleteStrategy,
    PrepareDeleteResult,
    DeleteFileEnvelope,
)
from deltacat.storage import (
    DeltaType,
)
import pyarrow.compute as pc
import logging
import numpy as np

from deltacat.utils.numpy import searchsorted_by_attr
import ray
from typing import Any, Callable, Optional, List, Dict, Tuple
from deltacat.types.media import StorageType
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from ray.types import ObjectRef
import pyarrow as pa
from deltacat.compute.compactor import (
    DeltaAnnotated,
)
from deltacat import logs
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
)


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class DefaultEqualityDeleteStrategy(DeleteStrategy):
    _name: str = "DefaultEqualityDeleteStrategy"

    @property
    def name(cls):
        return cls.name

    def _drop_rows(
        self,
        table: pa.Table,
        deletes_to_apply,
        delete_column_names: List[str],
        equality_predicate_operation: Optional[Callable] = pa.compute.and_,
    ) -> Tuple[Any, int]:
        # next = None
        for i, delete_column_name in enumerate(delete_column_names):
            boolean_mask = pc.is_in(
                table[delete_column_name],
                value_set=deletes_to_apply[delete_column_name],
            )
            table = table.filter(pc.invert(boolean_mask))
            return table, len(boolean_mask)

    def prepare_deletes(
        self,
        params: CompactPartitionParams,
        input_deltas: List[DeltaAnnotated],
        *args,
        **kwargs,
    ) -> PrepareDeleteResult:
        """
        Prepares delete operations for a compaction process.
        This function processes all the annotated deltas and consolidates consecutive DELETE deltas using a sliding window algorithm
        It creates a range dictionary of these consolidated delete operations of the earliest stream position to the Ray obj references to the delete table
        Additionally, non-DELETE deltas are accumulated in a separate list.

        Args:
            params (CompactPartitionParams): Parameters for the compaction process.
            uniform_deltas (List[DeltaAnnotated]): A list of DeltaAnnotated objects representing
                delete operations.

        Returns:
            Tuple[List[DeltaAnnotated], IntegerRangeDict]:
                - A list of Annotated Deltas excluding all non-delete operations.
                - A dictionary (IntegerRangeDict) containing consolidated delete operations, where the keys
                are the earliest stream positions of the consolidated delete operations, and the values
                are Ray object references to PyArrow Tables representing the consolidated delete tables.
                If there are no delete operations, this dictionary will be empty

        Raises:
            AssertionError: If a delete operation does not have the required properties defined.
        """
        if not input_deltas:
            return PrepareDeleteResult(input_deltas, [])
        assert all(
            input_deltas[i].stream_position <= input_deltas[i + 1].stream_position
            for i in range(len(input_deltas) - 1)
        ), "Uniform deltas must be in non-decreasing order by stream position"
        window_start, window_end = 0, 0
        delete_payloads: List[DeleteFileEnvelope] = []
        non_delete_deltas: List[DeltaAnnotated] = []
        while window_end < len(input_deltas):
            # skip over non-delete type deltas
            if (
                input_deltas[window_end].annotations[0].annotation_delta_type
                is not DeltaType.DELETE
            ):
                non_delete_deltas.append(input_deltas[window_end])
                window_start += 1
                window_end = window_start
                continue
            # extend out the delete window to include all consecutive deletes
            while (
                window_end < len(input_deltas)
                and input_deltas[window_end].annotations[0].annotation_delta_type
                is DeltaType.DELETE
            ):
                window_end += 1
            delete_deltas_sequence: List[DeltaAnnotated] = input_deltas[
                window_start:window_end
            ]
            deletes_at_this_stream_position: List[pa.Table] = []
            for delete_delta in delete_deltas_sequence:
                assert (
                    delete_delta.delete_parameters is not None
                ), "Delete type deltas are required to have delete parameters defined"
                delete_columns: Optional[
                    List[str]
                ] = delete_delta.delete_parameters.equality_column_names
                delete_dataset = params.deltacat_storage.download_delta(
                    delete_delta,
                    file_reader_kwargs_provider=params.read_kwargs_provider,
                    columns=delete_columns,
                    storage_type=StorageType.LOCAL,
                    max_parallelism=1,
                    **params.deltacat_storage_kwargs,
                )
                deletes_at_this_stream_position.extend(delete_dataset)
            consolidated_deletes: pa.Table = pa.concat_tables(
                deletes_at_this_stream_position
            )
            stream_position_of_earliest_delete_in_sequence: int = (
                delete_deltas_sequence[0].stream_position
            )
            obj_ref: ObjectRef = ray.put(consolidated_deletes)
            delete_envelope = DeleteFileEnvelope(
                stream_position_of_earliest_delete_in_sequence,
                obj_ref,
                delete_columns,
            )
            delete_payloads.append(delete_envelope)
            window_start = window_end
            # store all_deletes
        return PrepareDeleteResult(
            non_delete_deltas,
            delete_payloads,
        )

    def match_deletes(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        index_identifier: int,
        delete_stream_positions: List[int],
    ) -> Tuple[List[int], Dict[str, Any]]:
        delete_indices: List[int] = searchsorted_by_attr(
            "stream_position", df_envelopes, delete_stream_positions
        )
        upsert_stream_position_to_delete_table = {}
        for delete_stream_position_index, delete_pos_in_upsert in enumerate(
            delete_indices
        ):
            if delete_pos_in_upsert == 0:
                continue
            upsert_stream_pos = df_envelopes[delete_pos_in_upsert - 1].stream_position
            upsert_stream_position_to_delete_table[
                upsert_stream_pos
            ] = delete_stream_position_index
        return delete_indices, upsert_stream_position_to_delete_table

    def split_incrementals(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        index_identifier: int,
        delete_locations: List[Any],
    ):
        df_envelopes: List[List[DeltaFileEnvelope]] = [
            upsert_sequence.tolist()
            for upsert_sequence in np.split(df_envelopes, delete_locations)
            if upsert_sequence.tolist()
        ]
        return df_envelopes

    def apply_deletes(
        self,
        table,
        table_stream_pos,
        delete_envelopes: List[DeleteFileEnvelope],
        upsert_stream_position_to_delete_table: Dict[str, Any],
    ) -> Tuple[pa.Table, int]:
        logger.info(f"pdebug:: {locals()=}")
        delete_index = upsert_stream_position_to_delete_table[table_stream_pos]
        (
            _,
            delete_obj_ref,
            delete_column_names,
        ) = delete_envelopes[delete_index]
        deletes_to_apply = ray.get(delete_obj_ref)
        table, number_of_rows_dropped = self._drop_rows(
            table, deletes_to_apply, delete_column_names
        )
        return table, number_of_rows_dropped

    def apply_all_deletes(
        self,
        table,
        all_deletes: List[DeleteFileEnvelope],
    ):
        total_dropped_rows = 0
        for _, obj_ref, delete_column_names in all_deletes:
            delete_table = ray.get(obj_ref)
            table, number_of_rows_dropped = self._drop_rows(
                table, delete_table, delete_column_names
            )
            total_dropped_rows += number_of_rows_dropped
        return table, total_dropped_rows

        

