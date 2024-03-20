from deltacat.compute.compactor_v2.deletes.model import (
    PrepareDeleteResult,
    DeleteFileEnvelope,
    DeleteTableReferenceStorageStrategy,
)
from deltacat.compute.compactor_v2.deletes.model import DeleteStrategy
from deltacat.storage import (
    DeltaType,
)
from collections import defaultdict
import pyarrow.compute as pc
import logging
import numpy as np

from deltacat.utils.numpy import searchsorted_by_attr
from typing import Any, Callable, Optional, List, Dict, Tuple
from deltacat.types.media import StorageType
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
import pyarrow as pa
from deltacat.compute.compactor import (
    DeltaAnnotated,
)
from deltacat import logs
from deltacat.compute.compactor import (
    DeltaFileEnvelope,
)


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class EqualityDeleteStrategy(DeleteStrategy):
    _name: str = "EqualityDeleteStrategy"

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
        return table, number_of_rows_after_dropping - number_of_rows_before_dropping

    def _filter_out_non_delete_deltas(
        self, input_deltas: List[DeltaAnnotated]
    ) -> List[DeltaAnnotated]:
        non_delete_deltas = []
        for input_delta in input_deltas:
            if input_delta.type is not DeltaType.DELETE:
                non_delete_deltas.append(input_delta)
        return non_delete_deltas

    def _aggregate_all_delete_deltas(
        self, input_deltas: List[DeltaAnnotated]
    ) -> Dict[int, List[DeltaAnnotated]]:
        window_start, window_end = 0, 0
        delete_delta_sequence_spos_to_delete_delta = defaultdict(list)
        while window_end < len(input_deltas):
            if input_deltas[window_end].type is not DeltaType.DELETE:
                window_start += 1
                window_end = window_start
                continue
            while (
                window_end < len(input_deltas)
                and input_deltas[window_end].type is DeltaType.DELETE
                and input_deltas[window_end].delete_parameters
                == input_deltas[window_start].delete_parameters
            ):
                window_end += 1
            delete_deltas_sequence: List[DeltaAnnotated] = input_deltas[
                window_start:window_end
            ]
            stream_position_of_earliest_delete_in_sequence: int = (
                delete_deltas_sequence[0].stream_position
            )
            delete_delta_sequence_spos_to_delete_delta[
                stream_position_of_earliest_delete_in_sequence
            ].extend(delete_deltas_sequence)
            window_start = window_end
        return delete_delta_sequence_spos_to_delete_delta

    def _get_delete_file_envelopes(
        self,
        params: CompactPartitionParams,
        delete_spos_to_delete_deltas: Dict[int, List],
    ) -> List[DeleteFileEnvelope]:
        delete_file_envelopes = []
        for (
            stream_position,
            delete_delta_sequence,
        ) in delete_spos_to_delete_deltas.items():
            consecutive_delete_tables: List[pa.Table] = []
            for delete_delta in delete_delta_sequence:
                assert (
                    delete_delta.delete_parameters is not None
                ), "Delete type deltas are required to have delete parameters defined"
                delete_columns: Optional[
                    List[str]
                ] = delete_delta.delete_parameters.equality_column_names
                assert len(delete_columns) > 0, "At least 1 delete column is required"
                delete_dataset = params.deltacat_storage.download_delta(
                    delete_delta,
                    file_reader_kwargs_provider=params.read_kwargs_provider,
                    columns=delete_columns,
                    storage_type=StorageType.LOCAL,
                    max_parallelism=1,
                    **params.deltacat_storage_kwargs,
                )
                consecutive_delete_tables.extend(delete_dataset)
            consolidated_deletes: pa.Table = pa.concat_tables(consecutive_delete_tables)
            delete_envelope = DeleteFileEnvelope(
                stream_position,
                consolidated_deletes,
                delete_columns,
                DeleteTableReferenceStorageStrategy(),
            )
            delete_file_envelopes.append(delete_envelope)
        return delete_file_envelopes

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
        non_delete_deltas: List[DeltaAnnotated] = self._filter_out_non_delete_deltas(
            input_deltas
        )
        delete_spos_to_delete_deltas: Dict[
            int, List[DeltaAnnotated]
        ] = self._aggregate_all_delete_deltas(input_deltas)
        delete_file_envelopes: List[
            DeleteFileEnvelope
        ] = self._get_delete_file_envelopes(params, delete_spos_to_delete_deltas)
        return PrepareDeleteResult(
            non_delete_deltas,
            delete_file_envelopes,
        )

    def match_deletes(
        self,
        index_identifier: int,
        df_envelopes: List[DeltaFileEnvelope],
        delete_stream_positions: List[int],
    ) -> Tuple[List[int], Dict[str, Any]]:
        delete_indices: List[int] = searchsorted_by_attr(
            "stream_position", df_envelopes, delete_stream_positions
        )
        upsert_stream_position_to_delete_table = defaultdict(list)
        for i, delete_pos_in_upsert in enumerate(delete_indices):
            delete_stream_position_index = i
            if delete_pos_in_upsert == 0:
                continue
            upsert_stream_pos = df_envelopes[delete_pos_in_upsert - 1].stream_position
            upsert_stream_position_to_delete_table[upsert_stream_pos].append(
                delete_stream_position_index
            )
        return delete_indices, upsert_stream_position_to_delete_table

    def rebatch_df_envelopes(
        self,
        index_identifier: int,
        df_envelopes: List[DeltaFileEnvelope],
        delete_locations: List[Any],
    ) -> List[List[DeltaFileEnvelope]]:
        df_envelopes: List[List[DeltaFileEnvelope]] = [
            upsert_sequence.tolist()
            for upsert_sequence in np.split(df_envelopes, delete_locations)
            if upsert_sequence.tolist()
        ]
        return df_envelopes

    def apply_deletes(
        self,
        index_identifier,
        table,
        delete_envelope: DeleteFileEnvelope,
    ) -> Tuple[pa.Table, int]:
        delete_columns = delete_envelope.delete_columns
        delete_table = delete_envelope.delete_table
        table, number_of_rows_dropped = self._drop_rows(
            table, delete_table, delete_columns
        )
        return table, number_of_rows_dropped

    def apply_all_deletes(
        self,
        table,
        delete_envelope: List[DeleteFileEnvelope],
    ):
        total_dropped_rows = 0
        for delete_envelope in delete_envelope:
            delete_columns = delete_envelope.delete_columns
            delete_table = delete_envelope.delete_table
            table, number_of_rows_dropped = self._drop_rows(
                table, delete_table, delete_columns
            )
            total_dropped_rows += number_of_rows_dropped
        return table, total_dropped_rows
