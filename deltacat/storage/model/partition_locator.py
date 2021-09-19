from typing import Any, Dict, List, Optional
from deltacat.utils.common import sha1_digest, sha1_hexdigest
from deltacat.storage.model import stream_locator as sl


def of(
        stream_locator: Optional[Dict[str, Any]],
        partition_values: Optional[List[Any]],
        partition_id: Optional[str]) -> Dict[str, Any]:
    """
    Creates a stream partition locator. Partition ID is
    case-sensitive. Partition Value types must ensure that
    `str(partition_value1) == str(partition_value2)` always implies
    `partition_value1 == partition_value2` (i.e. if two string representations
    of partition values are equal, than the two partition values are equal).
    """
    return {
        "streamLocator": stream_locator,
        "partitionValues": partition_values,
        "partitionId": partition_id,
    }


def get_stream_locator(partition_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:
    return partition_locator.get("streamLocator")


def set_stream_locator(
        partition_locator: Dict[str, Any],
        stream_locator: Optional[Dict[str, Any]]) -> None:

    partition_locator["streamLocator"] = stream_locator


def get_partition_values(partition_locator: Dict[str, Any]) \
        -> Optional[List[Any]]:

    return partition_locator.get("partitionValues")


def set_partition_values(
        partition_locator: Dict[str, Any],
        partition_values: Optional[List[Any]]) -> None:

    partition_locator["partitionValues"] = partition_values


def get_partition_id(partition_locator: Dict[str, Any]) -> Optional[str]:
    return partition_locator.get("partitionId")


def set_partition_id(
        partition_locator: Dict[str, Any],
        partition_id: Optional[str]) -> None:

    partition_locator["partitionId"] = partition_id


def get_namespace_locator(partition_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_namespace_locator(stream_locator)
    return None


def get_table_locator(partition_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_table_locator(stream_locator)
    return None


def get_table_version_locator(partition_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_table_version_locator(stream_locator)
    return None


def get_stream_id(partition_locator: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_stream_id(stream_locator)
    return None


def get_storage_type(partition_locator: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_storage_type(stream_locator)
    return None


def get_namespace(partition_locator: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_namespace(stream_locator)
    return None


def get_table_name(partition_locator: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_table_name(stream_locator)
    return None


def get_table_version(partition_locator: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(partition_locator)
    if stream_locator:
        return sl.get_table_version(stream_locator)
    return None


def canonical_string(partition_locator: Dict[str, Any]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    sl_hexdigest = sl.hexdigest(get_stream_locator(partition_locator))
    partition_vals = str(get_partition_values(partition_locator))
    partition_id = get_partition_id(partition_locator)
    return f"{sl_hexdigest}|{partition_vals}|{partition_id}"


def digest(partition_locator: Dict[str, Any]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(canonical_string(partition_locator).encode("utf-8"))


def hexdigest(partition_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(canonical_string(partition_locator).encode("utf-8"))
