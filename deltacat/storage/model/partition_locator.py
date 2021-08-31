from typing import Any, Dict, List, Optional
from deltacat.utils.common import sha1_hexdigest
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
        stream_locator: Optional[Dict[str, Any]]):

    partition_locator["streamLocator"] = stream_locator


def get_partition_values(partition_locator: Dict[str, Any]) \
        -> Optional[List[Any]]:

    return partition_locator.get("partitionValues")


def set_partition_values(
        partition_locator: Dict[str, Any],
        partition_values: Optional[List[Any]]):

    partition_locator["partitionValues"] = partition_values


def get_partition_id(partition_locator: Dict[str, Any]) -> Optional[str]:
    return partition_locator.get("partitionId")


def set_partition_id(
        partition_locator: Dict[str, Any],
        partition_id: Optional[str]):

    partition_locator["partitionId"] = partition_id


def hexdigest(partition_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given Partition Locator suitable for use in
    equality (i.e. two Partition Locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    sl_hexdigest = sl.hexdigest(get_stream_locator(partition_locator))
    partition_vals = str(get_partition_values(partition_locator))
    partition_id = get_partition_id(partition_locator)
    partition_locator_str = f"{sl_hexdigest}|{partition_vals}|{partition_id}"
    return sha1_hexdigest(partition_locator_str.encode("utf-8"))
