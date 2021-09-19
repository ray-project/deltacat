from deltacat.storage.model import stream_locator as sl
from typing import Any, Dict, List, Optional


def of(
        stream_locator: Optional[Dict[str, Any]],
        partition_keys: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:

    return {
        "streamLocator": stream_locator,
        "partitionKeys": partition_keys,
    }


def get_stream_locator(partition_staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return partition_staging_area.get("streamLocator")


def set_stream_locator(
        partition_staging_area: Dict[str, Any],
        stream_locator: Optional[Dict[str, Any]]) -> None:

    partition_staging_area["streamLocator"] = stream_locator


def get_partition_keys(staging_area: Dict[str, Any]) \
        -> Optional[List[Dict[str, Any]]]:

    return staging_area.get("partitionKeys")


def set_partition_keys(
        partition_staging_area: Dict[str, Any],
        partition_keys: Optional[List[Dict[str, Any]]]) -> None:

    partition_staging_area["partitionKeys"] = partition_keys


def get_namespace_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    stream_locator = get_stream_locator(staging_area)
    if stream_locator:
        return sl.get_namespace_locator(stream_locator)
    return None


def get_table_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    stream_locator = get_stream_locator(staging_area)
    if stream_locator:
        return sl.get_table_locator(stream_locator)
    return None


def get_table_version_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    stream_locator = get_stream_locator(staging_area)
    if stream_locator:
        return sl.get_table_version_locator(stream_locator)
    return None


def get_stream_id(staging_area: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(staging_area)
    if stream_locator:
        return sl.get_stream_id(stream_locator)
    return None


def get_namespace(staging_area: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(staging_area)
    if stream_locator:
        return sl.get_namespace(stream_locator)
    return None


def get_table_name(staging_area: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(staging_area)
    if stream_locator:
        return sl.get_table_name(stream_locator)
    return None


def get_table_version(staging_area: Dict[str, Any]) -> Optional[str]:
    stream_locator = get_stream_locator(staging_area)
    if stream_locator:
        return sl.get_table_version(stream_locator)
    return None


def validate_partition_values(
        staging_area: Dict[str, Any],
        partition_values: Optional[List[Any]]):

    # TODO (pdames): ensure value data types match key data types
    partition_keys = get_partition_keys(staging_area)
    num_keys = len(partition_keys) if partition_keys else 0
    num_values = len(partition_values) if partition_values else 0
    if num_values != num_keys:
        raise ValueError(f"Found {num_values} partition values but only "
                         f"{num_keys} keys in staging area: {staging_area}")
