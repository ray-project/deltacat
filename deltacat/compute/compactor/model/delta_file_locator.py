import numpy as np
from deltacat.utils.common import sha1_digest, sha1_hexdigest
from typing import Tuple


def of(
        is_src_delta: np.bool_,
        stream_position: np.int64,
        file_index: np.int32) -> \
        Tuple[np.bool_, np.int64, np.int32]:
    """
    Create a Delta File Locator tuple that can be used to uniquely identify and
    retrieve a file from any compaction job run input Delta.

    Args:
        is_src_delta (np.bool_): True if this Delta File Locator is pointing
        to a file from the uncompacted source table, False if this Locator is
        pointing to a file in the compacted destination table.

        stream_position (np.int64): Stream position of the Delta that contains
        this file.

        file_index (np.int32): Index of the file in the Delta Manifest.

    Returns:
        delta_file_locator (Tuple[np.bool_, np.int64, np.int32]): The Delta
        File Locator tuple as (is_source_delta, stream_position, file_index).
    """
    return (
        is_src_delta,
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


def canonical_string(df_locator: Tuple[np.bool_, np.int64, np.int32]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    return f"{df_locator[0]}|{df_locator[1]}|{df_locator[2]}"


def digest(df_locator: Tuple[np.bool_, np.int64, np.int32]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(bytes(canonical_string(df_locator), "utf-8"))


def hexdigest(df_locator: Tuple[np.bool_, np.int64, np.int32]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(canonical_string(df_locator).encode("utf-8"))
