import numpy as np
from deltacat.utils.common import sha1_digest
from typing import Tuple


def of(
        is_source_delta: np.bool_,
        stream_position: np.int64,
        file_index: np.int32) -> \
        Tuple[np.bool_, np.int64, np.int32]:
    """
    Create a Delta File Locator tuple that can be used to uniquely identify and
    retrieve a file from any compaction job run input Delta.

    Args:
        is_source_delta (np.bool_): True if this Delta File Locator is pointing
        to a file from the uncompacted source table, False if this Locator is
        pointing to a file in the compacted destination table.

        stream_position (np.int64): Stream position of the Delta that contains
        this file.

        file_index (np.int32): Index of the file in the Delta Manifest.

    Returns:
        bool: The return value. True for success, False otherwise.
    """
    return (
        is_source_delta,
        stream_position,
        file_index,
    )


def is_source_delta(df_locator: Tuple[np.bool_, np.int64, np.int32]) \
        -> np.bool_:
    return df_locator[0]


def get_stream_position(df_locator: Tuple[np.bool_, np.int64, np.int32]) \
        -> np.int64:
    return df_locator[1]


def get_file_index(df_locator: Tuple[np.bool_, np.int64, np.int32]) \
        -> np.int32:
    return df_locator[2]


def digest(df_locator: Tuple[np.bool_, np.int64, np.int32]) -> bytes:
    """
    Return a digest of the given Delta File Locator that can be used for
    equality checks (i.e. two delta file locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    file_id_str = f"{df_locator[0]}|{df_locator[1]}|{df_locator[2]}"
    return sha1_digest(bytes(file_id_str, "utf-8"))
