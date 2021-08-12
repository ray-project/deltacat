import pyarrow as pa
import pandas as pd
import numpy as np
from deltacat.storage.model.types import DeltaType
from typing import Any, Dict, Union


def of(
        stream_position: int,
        file_index: int,
        delta_type: DeltaType,
        table: Union[pa.Table, pd.DataFrame, np.ndarray]) -> Dict[str, Any]:

    if stream_position is None:
        raise ValueError("Delta file envelope stream position must be defined.")
    if file_index is None:
        raise ValueError("Delta file envelope file index must be defined.")
    if delta_type is None:
        raise ValueError("Delta file envelope delta type must be defined.")

    return {
        "stream_position": stream_position,
        "file_index": file_index,
        "delta_type": delta_type.value,
        "table": table,
    }


def get_stream_position(delta_file_envelope: Dict[str, Any]) -> int:
    return delta_file_envelope["stream_position"]


def get_file_index(delta_file_envelope: Dict[str, Any]) -> int:
    return delta_file_envelope["file_index"]


def get_delta_type(delta_file_envelope: Dict[str, Any]) -> DeltaType:
    return DeltaType(delta_file_envelope["delta_type"])


def get_table(delta_file_envelope: Dict[str, Any]) \
        -> Union[pa.Table, pd.DataFrame, np.ndarray]:

    return delta_file_envelope["table"]
