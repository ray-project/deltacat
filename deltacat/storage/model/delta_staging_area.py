import pyarrow as pa
from deltacat.storage.model import partition_locator as pl
from deltacat.types.media import ContentType
from typing import Any, Dict, List, Optional, Union


def of(
        partition_locator: Optional[Dict[str, Any]],
        schema: Optional[Union[pa.Schema, str, bytes]],
        supported_content_types: Optional[List[ContentType]],
        previous_stream_position: Optional[int] = None,
        previous_partition_id: Optional[str] = None) -> Dict[str, Any]:

    return {
        "partitionLocator": partition_locator,
        "schema": schema,
        "contentTypes": supported_content_types,
        "previousStreamPosition": previous_stream_position,
        "previousPartitionId": previous_partition_id,
    }


def get_partition_locator(delta_staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return delta_staging_area.get("partitionLocator")


def set_partition_locator(
        delta_staging_area: Dict[str, Any],
        partition_locator: Optional[Dict[str, Any]]) -> None:

    delta_staging_area["partitionLocator"] = partition_locator


def get_schema(delta_staging_area: Dict[str, Any]) \
        -> Optional[Union[pa.Schema, str, bytes]]:

    return delta_staging_area.get("schema")


def set_schema(
        delta_staging_area: Dict[str, Any],
        schema: Optional[Union[pa.Schema, str, bytes]]) -> None:

    delta_staging_area["schema"] = schema


def get_supported_content_types(delta_staging_area: Dict[str, Any]) \
        -> Optional[List[ContentType]]:

    content_types = delta_staging_area.get("contentTypes")
    return None if content_types is None else \
        [None if _ is None else ContentType(_) for _ in content_types]


def set_supported_content_types(
        delta_staging_area: Dict[str, Any],
        supported_content_types: Optional[List[ContentType]]) -> None:

    delta_staging_area["contentTypes"] = supported_content_types


def is_supported_content_type(
        delta_staging_area: Dict[str, Any],
        content_type: ContentType):

    supported_content_types = get_supported_content_types(delta_staging_area)
    return (not supported_content_types) or \
           (content_type in supported_content_types)


def get_partition_id(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_partition_id(partition_locator)
    return None


def set_partition_id(
        staging_area: Dict[str, Any],
        partition_id: str) -> None:

    pl.set_partition_id(get_partition_locator(staging_area), partition_id)


def get_previous_stream_position(staging_area: Dict[str, Any]) -> Optional[int]:

    return staging_area.get("previousStreamPosition")


def set_previous_stream_position(
        staging_area: Dict[str, Any],
        previous_stream_position: Optional[int]) -> None:

    staging_area["previousStreamPosition"] = previous_stream_position


def get_previous_partition_id(staging_area: Dict[str, Any]) -> Optional[str]:
    return staging_area.get("previousPartitionId")


def set_previous_partition_id(
        staging_area: Dict[str, Any],
        previous_partition_id: Optional[str]) -> None:

    staging_area["previousPartitionId"] = previous_partition_id


def get_stream_id(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_stream_id(partition_locator)
    return None


def get_partition_values(staging_area: Dict[str, Any]) -> Optional[List[Any]]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_partition_values(partition_locator)


def get_namespace_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_namespace_locator(partition_locator)
    return None


def get_table_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_table_locator(partition_locator)
    return None


def get_table_version_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_table_version_locator(partition_locator)
    return None


def get_stream_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_stream_locator(partition_locator)
    return None


def get_storage_type(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_storage_type(partition_locator)
    return None


def get_namespace(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_namespace(partition_locator)
    return None


def get_table_name(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_table_name(partition_locator)
    return None


def get_table_version(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_table_version(partition_locator)
    return None
