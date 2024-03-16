from deltacat.compute.compactor_v2.deletes.model import (
    DeleteStrategy,
    PrepareDeleteResult,
    DeleteEnvelope,
)
from deltacat.storage import (
    DeltaType,
)

from deltacat.utils.numpy import searchsorted_by_attr
import ray
from typing import Any, Optional, List, Dict, Tuple
from deltacat.types.media import StorageType
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from ray.types import ObjectRef
import pyarrow as pa
from deltacat.compute.compactor import (
    DeltaAnnotated,
)

from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)


class DefaultEqualityDeleteStrategy(DeleteStrategy):
    _name: str = "DefaultEqualityDeleteStrategy"

    @property
    def name(cls):
        return cls.name

    def prepare_deletes(
        self,
        params: CompactPartitionParams,
        input_deltas: List[DeltaAnnotated],
        *args,
        **kwargs
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
        delete_payloads: List[DeleteEnvelope] = []
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
            delete_payloads.append(
                DeletePayload(
                    stream_position_of_earliest_delete_in_sequence,
                    obj_ref,
                    delete_columns,
                )
            )
            window_start = window_end
            # store all_deletes
        return PrepareDeleteResult(
            non_delete_deltas,
            delete_payloads,
        )

    def get_deletes_indices(
        self,
        df_envelopes: List[DeltaFileEnvelope],
        deletes: List[DeleteEnvelope],
        *args,
        **kwargs
    ) -> Tuple[List[int], Dict[int, Any]]:
        delete_stream_positions: List[int] = [
            delete.stream_position for delete in deletes
        ]
        delete_indices: List[int] = searchsorted_by_attr(
            "stream_position", df_envelopes, delete_stream_positions
        )
        spos_to_delete = {}
        for delete_stream_position_index, delete_pos_in_upsert in enumerate(
            delete_indices
        ):
            if delete_pos_in_upsert == 0:
                continue
            upsert_stream_pos = df_envelopes[delete_pos_in_upsert - 1].stream_position
            spos_to_delete[upsert_stream_pos] = delete_stream_position_index
        return delete_indices, spos_to_delete
