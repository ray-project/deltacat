import logging
from deltacat import logs
import pyarrow.compute as pc
import pyarrow as pa
import numpy as np
from deltacat.utils.performance import timed_invocation
from deltacat.compute.compactor.utils import system_columns as sc

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _create_chunked_index_array(array: pa.Array) -> pa.Array:
    """
    Creates an chunked array where each chunk is of same size in the input array.
    """
    chunk_lengths = [
        len(array.chunk(chunk_index)) for chunk_index in range(len(array.chunks))
    ]

    # if all chunk lengths are equal, numpy assumes correct shape
    # which make addition operation append elements instead of adding.
    result = np.empty((len(chunk_lengths),), dtype="object")

    for index, cl in enumerate(chunk_lengths):
        result[index] = np.arange(cl, dtype="int32")

    chunk_lengths = ([0] + chunk_lengths)[:-1]
    result = pa.chunked_array(result + np.cumsum(chunk_lengths), type=pa.int32())
    return result


def drop_duplicates(table: pa.Table, on: str) -> pa.Table:
    """
    It is important to not combine the chunks for performance reasons.
    """

    if on not in table.column_names:
        return table

    index_array, array_latency = timed_invocation(
        _create_chunked_index_array, table[on]
    )

    logger.info(
        "Created a chunked index array of length "
        f" {len(index_array)} in {array_latency}s"
    )

    table = table.set_column(
        table.shape[1], sc._ORDERED_RECORD_IDX_COLUMN_NAME, index_array
    )
    selector = table.group_by([on]).aggregate(
        [(sc._ORDERED_RECORD_IDX_COLUMN_NAME, "max")]
    )

    table = table.filter(
        pc.is_in(
            table[sc._ORDERED_RECORD_IDX_COLUMN_NAME],
            value_set=selector[f"{sc._ORDERED_RECORD_IDX_COLUMN_NAME}_max"],
        )
    )

    table = table.drop([sc._ORDERED_RECORD_IDX_COLUMN_NAME])

    return table
