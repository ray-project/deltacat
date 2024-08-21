import time
from typing import List, Optional, Tuple

from deltacat.compute.compactor import (
    DeltaAnnotated,
    DeltaFileEnvelope,
)
from deltacat.storage import (
    Delta,
)
from deltacat.storage.model.delta import DeltaType
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import StorageType
from deltacat.utils.common import ReadKwargsProvider
from deltacat import logs

import pyarrow as pa
import logging


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def contains_delete_deltas(deltas: List[Delta]) -> bool:
    for delta in deltas:
        if delta.type is DeltaType.DELETE:
            return True
    return False


def read_delta_file_envelopes(
    annotated_delta: DeltaAnnotated,
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
) -> Tuple[Optional[List[DeltaFileEnvelope]], int, int]:
    tables = deltacat_storage.download_delta(
        annotated_delta,
        max_parallelism=1,
        file_reader_kwargs_provider=read_kwargs_provider,
        storage_type=StorageType.LOCAL,
        **deltacat_storage_kwargs,
    )
    annotations = annotated_delta.annotations
    assert len(tables) == len(annotations), (
        f"Unexpected Error: Length of downloaded delta manifest tables "
        f"({len(tables)}) doesn't match the length of delta manifest "
        f"annotations ({len(annotations)})."
    )
    if not tables:
        return None, 0, 0

    delta_stream_position = annotations[0].annotation_stream_position
    delta_type = annotations[0].annotation_delta_type

    for annotation in annotations:
        assert annotation.annotation_stream_position == delta_stream_position, (
            f"Annotation stream position does not match - {annotation.annotation_stream_position} "
            f"!= {delta_stream_position}"
        )
        assert annotation.annotation_delta_type == delta_type, (
            f"Annotation delta type does not match - {annotation.annotation_delta_type} "
            f"!= {delta_type}"
        )

    delta_file_envelopes = []
    table = pa.concat_tables(tables)
    total_record_count = len(table)
    total_size_bytes = int(table.nbytes)

    delta_file = DeltaFileEnvelope.of(
        stream_position=delta_stream_position,
        delta_type=delta_type,
        table=table,
    )
    delta_file_envelopes.append(delta_file)
    return delta_file_envelopes, total_record_count, total_size_bytes


def get_local_delta_file_envelopes(
    uniform_deltas: List[DeltaAnnotated],
    read_kwargs_provider: Optional[ReadKwargsProvider],
    deltacat_storage=unimplemented_deltacat_storage,
    deltacat_storage_kwargs: Optional[dict] = None,
) -> Tuple[List[DeltaFileEnvelope], int]:
    local_dfe_list = []
    input_records_count = 0
    logger.info(f"Getting {len(uniform_deltas)} DFE Tasks.")
    dfe_start = time.monotonic()
    for annotated_delta in uniform_deltas:
        (
            delta_file_envelopes,
            total_record_count,
            total_size_bytes,
        ) = read_delta_file_envelopes(
            annotated_delta,
            read_kwargs_provider,
            deltacat_storage,
            deltacat_storage_kwargs,
        )
        if delta_file_envelopes:
            local_dfe_list.extend(delta_file_envelopes)
            input_records_count += total_record_count
    dfe_end = time.monotonic()
    logger.info(f"Retrieved {len(local_dfe_list)} DFE Tasks in {dfe_end - dfe_start}s.")
    return local_dfe_list, input_records_count
