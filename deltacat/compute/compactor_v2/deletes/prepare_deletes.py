from deltacat.storage import (
    DeltaType,
)
from deltacat.utils.rangedictionary import IntegerRangeDict
from typing import Optional, List, Dict, Tuple
from deltacat.types.media import StorageType
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
import pyarrow as pa
from deltacat.compute.compactor import (
    DeltaAnnotated,
)


def prepare_deletes(
    params: CompactPartitionParams, uniform_deltas: List[DeltaAnnotated]
) -> Tuple[List[DeltaAnnotated], IntegerRangeDict]:
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

    if not uniform_deltas:
        return uniform_deltas, None
    assert all(
        uniform_deltas[i].stream_position <= uniform_deltas[i + 1].stream_position
        for i in range(len(uniform_deltas) - 1)
    ), "Uniform deltas must be in non-decreasing order by stream position"
    deletes_obj_ref_by_stream_position = IntegerRangeDict()
    window_start, window_end = 0, 0
    non_delete_deltas = []
    while window_end < len(uniform_deltas):
        # skip over non-delete type deltas
        if (
            uniform_deltas[window_end].annotations[0].annotation_delta_type
            is not DeltaType.DELETE
        ):
            non_delete_deltas.append(uniform_deltas[window_end])
            window_start += 1
            window_end = window_start
            continue
        # extend out the delete window to include all consecutive deletes
        while (
            window_end < len(uniform_deltas)
            and uniform_deltas[window_end].annotations[0].annotation_delta_type
            is DeltaType.DELETE
        ):
            window_end += 1
        delete_deltas_sequence: List[DeltaAnnotated] = uniform_deltas[
            window_start:window_end
        ]
        deletes_at_this_stream_position: List[pa.Table] = []
        for delete_delta in delete_deltas_sequence:
            assert (
                delete_delta.properties is not None
            ), "Delete type deltas are required to have properties defined"
            properties: Optional[Dict[str, str]] = delete_delta.properties
            assert (
                properties.get("DELETE_COLUMNS") is not None
            ), "Delete type deltas are required to have a delete column list defined"
            delete_columns: Optional[List[str]] = properties.get("DELETE_COLUMNS")
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
        stream_position_of_earliest_delete_in_sequence: int = delete_deltas_sequence[
            0
        ].stream_position
        deletes_obj_ref_by_stream_position[
            stream_position_of_earliest_delete_in_sequence
        ] = params.object_store.put(consolidated_deletes)
        window_start = window_end
        # store all_deletes
    deletes_obj_ref_by_stream_position.rebalance()
    return non_delete_deltas, deletes_obj_ref_by_stream_position