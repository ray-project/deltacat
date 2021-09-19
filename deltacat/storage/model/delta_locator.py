from deltacat.utils.common import sha1_digest, sha1_hexdigest
from deltacat.storage.model import partition_locator as pl
from typing import Any, Dict, List, Optional


def of(
        partition_locator: Optional[Dict[str, Any]],
        stream_position: Optional[int]) -> Dict[str, Any]:
    """
    Creates a partition delta locator. Stream Position, if provided, should be
    greater than that of any prior delta in the partition.
    """
    return {
        "partitionLocator": partition_locator,
        "streamPosition": stream_position,
    }


def get_partition_locator(delta_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return delta_locator.get("partitionLocator")


def set_partition_locator(
        delta_locator: Dict[str, Any],
        partition_locator: Optional[Dict[str, Any]]) -> None:

    delta_locator["partitionLocator"] = partition_locator


def get_stream_position(delta_locator: Dict[str, Any]) -> Optional[int]:
    return delta_locator.get("streamPosition")


def set_stream_position(
        delta_locator: Dict[str, Any],
        stream_position: Optional[int]) -> None:

    delta_locator["streamPosition"] = stream_position


def get_namespace_locator(delta_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_namespace_locator(partition_locator)
    return None


def get_table_locator(delta_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_table_locator(partition_locator)
    return None


def get_table_version_locator(delta_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_table_version_locator(partition_locator)
    return None


def get_stream_locator(delta_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_stream_locator(partition_locator)
    return None


def get_partition_values(delta_locator: Dict[str, Any]) \
        -> Optional[List[Any]]:

    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_partition_values(partition_locator)
    return None


def get_partition_id(delta_locator: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_partition_id(partition_locator)
    return None


def get_stream_id(delta_locator: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_stream_id(partition_locator)
    return None


def get_storage_type(delta_locator: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_storage_type(partition_locator)
    return None


def get_namespace(delta_locator: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_namespace(partition_locator)
    return None


def get_table_name(delta_locator: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_table_name(partition_locator)
    return None


def get_table_version(delta_locator: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(delta_locator)
    if partition_locator:
        return pl.get_table_version(partition_locator)
    return None


def canonical_string(delta_locator: Dict[str, Any]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    pl_hexdigest = pl.hexdigest(get_partition_locator(delta_locator))
    stream_position = get_stream_position(delta_locator)
    return f"{pl_hexdigest}|{stream_position}"


def digest(delta_locator: Dict[str, Any]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(canonical_string(delta_locator).encode("utf-8"))


def hexdigest(delta_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(canonical_string(delta_locator).encode("utf-8"))
