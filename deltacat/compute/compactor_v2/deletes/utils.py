from deltacat.compute.compactor_v2.deletes.model import (
    PrepareDeleteResult,
    DeleteFileEnvelope,
)
from deltacat.storage import (
    DeltaType,
)
from collections import defaultdict
import logging

from typing import Optional, List, Dict
from deltacat.types.media import StorageType
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
import pyarrow as pa
from deltacat.storage import (
    Delta,
)
from deltacat import logs


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _filter_out_non_delete_deltas(input_deltas: List[Delta]) -> List[Delta]:
    non_delete_deltas = []
    for input_delta in input_deltas:
        if input_delta.type is not DeltaType.DELETE:
            non_delete_deltas.append(input_delta)
    return non_delete_deltas


def _aggregate_all_delete_deltas(input_deltas: List[Delta]) -> Dict[int, List[Delta]]:
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
        delete_deltas_sequence: List[Delta] = input_deltas[window_start:window_end]
        stream_position_of_earliest_delete_in_sequence: int = delete_deltas_sequence[
            0
        ].stream_position
        delete_delta_sequence_spos_to_delete_delta[
            stream_position_of_earliest_delete_in_sequence
        ].extend(delete_deltas_sequence)
        window_start = window_end
    return delete_delta_sequence_spos_to_delete_delta


def _get_delete_file_envelopes(
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
        delete_table: pa.Table = pa.concat_tables(consecutive_delete_tables)
        delete_file_envelope: DeleteFileEnvelope = DeleteFileEnvelope.of(
            stream_position,
            delta_type=DeltaType.DELETE,
            table=delete_table,
            delete_columns=delete_columns,
        )
        delete_file_envelopes.append(delete_file_envelope)
    return delete_file_envelopes


def prepare_deletes(
    params: CompactPartitionParams,
    input_deltas: List[Delta],
    *args,
    **kwargs,
) -> PrepareDeleteResult:
    """
    Prepares delete operations for a compaction process.
    This function processes all the annotated deltas and consolidates consecutive DELETE deltas.
    It creates a range dictionary of these consolidated delete operations of the earliest stream position to the Ray obj references to the delete table
    Additionally, non-DELETE deltas are accumulated in a separate list.

    Args:
        params (CompactPartitionParams): Parameters for the compaction process.
        uniform_deltas (List[Delta]): A list of Delta objects representing
            delete operations.

    Returns:
        Tuple[List[Delta], IntegerRangeDict]:
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
    non_delete_deltas: List[Delta] = _filter_out_non_delete_deltas(input_deltas)
    delete_spos_to_delete_deltas: Dict[int, List[Delta]] = _aggregate_all_delete_deltas(
        input_deltas
    )
    delete_file_envelopes: List[DeleteFileEnvelope] = _get_delete_file_envelopes(
        params, delete_spos_to_delete_deltas
    )
    return PrepareDeleteResult(
        non_delete_deltas,
        delete_file_envelopes,
    )
