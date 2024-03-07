from deltacat.storage import (
    DeltaType,
)
from deltacat.utils.rangedictionary import IntegerRangeDict
from typing import Optional, List, Dict
from deltacat.types.media import StorageType
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
import ray
import pyarrow as pa
from deltacat.compute.compactor import (
    DeltaAnnotated,
)


def prepare_deletes(
    params: CompactPartitionParams,
    uniform_deltas: List[DeltaAnnotated],
    deletes_obj_ref_by_stream_position: IntegerRangeDict,
) -> IntegerRangeDict:
    """
    Prepares delete operations for a compaction process.
    This function processes all the annotated deltas and consolidates consecutive delete deltas using a sliding window algorithm
    It creates a range dictionary of these consolidate delete operations of the earliest stream position of consolidated deletes to the Ray obj references to the delete table

    Args:
        params (CompactPartitionParams): Parameters for the compaction process.
        uniform_deltas (List[DeltaAnnotated]): A list of DeltaAnnotated objects representing
            delete operations.
        deletes_to_apply_obj_ref_by_stream_position (IntegerRangeDict): A dictionary to store
            the consolidated delete operations, keyed by stream position.

    Returns:
        IntegerRangeDict: A dictionary containing the consolidated delete operations, with
            stream positions as keys and Ray object references to PyArrow Tables as values.

    Raises:
        AssertionError: If a delete operation does not have the required properties defined.
    """
    if not uniform_deltas:
        return deletes_obj_ref_by_stream_position
    window_start, window_end = 0, 0
    while window_end < len(uniform_deltas):
        # skip over non-delete type deltas
        if (
            uniform_deltas[window_end].annotations[0].annotation_delta_type
            is not DeltaType.DELETE
        ):
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
                **params.deltacat_storage_kwargs,
            )
            deletes_at_this_stream_position.extend(delete_dataset)
        consolidated_deletes: pa.Table = pa.concat_tables(
            deletes_at_this_stream_position
        )
        stream_position_of_earliest_delete_in_sequence: int = uniform_deltas[
            window_start
        ].stream_position
        deletes_obj_ref_by_stream_position[
            stream_position_of_earliest_delete_in_sequence
        ] = ray.put(consolidated_deletes)
        window_start = window_end
    return deletes_obj_ref_by_stream_position
