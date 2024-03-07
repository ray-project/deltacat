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
    deletes_to_apply_obj_ref_by_stream_position: IntegerRangeDict,
) -> IntegerRangeDict:
    """ """
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
        while (
            window_end < len(uniform_deltas)
            and uniform_deltas[window_end].annotations[0].annotation_delta_type
            is DeltaType.DELETE
        ):
            window_end += 1
        delete_deltas_sequence = uniform_deltas[window_start:window_end]
        deletes_at_this_stream_position = []
        for delete_delta in delete_deltas_sequence:
            assert (
                delete_delta.properties is not None
            ), "Delete type deltas are required to have properties defined are required for deletes"
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
        consolidated_deletes = pa.concat_tables(deletes_at_this_stream_position)
        deletes_to_apply_obj_ref_by_stream_position[
            uniform_deltas[window_start].stream_position
        ] = ray.put(consolidated_deletes)
        window_start = window_end
        window_end = window_start
    return deletes_to_apply_obj_ref_by_stream_position
