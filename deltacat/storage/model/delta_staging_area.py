from deltacat.storage.model import stream_locator as sl, partition_locator as pl
from deltacat.types.media import ContentType
from typing import Any, Dict, List, Optional


def of(
        partition_locator: Optional[Dict[str, Any]],
        schema: Optional[str],
        supported_content_types: Optional[List[ContentType]]) -> Dict[str, Any]:

    return {
        "partitionLocator": partition_locator,
        "schema": schema,
        "supportedContentTypes": supported_content_types,
    }


def get_partition_locator(delta_staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return delta_staging_area.get("partitionLocator")


def set_partition_locator(
        delta_staging_area: Dict[str, Any],
        partition_locator: Optional[Dict[str, Any]]):

    delta_staging_area["partitionLocator"] = partition_locator


def get_supported_content_types(delta_staging_area: Dict[str, Any]) \
        -> Optional[List[ContentType]]:

    return delta_staging_area.get("supportedContentTypes")


def set_supported_content_types(
        delta_staging_area: Dict[str, Any],
        supported_content_types: Optional[List[ContentType]]):

    delta_staging_area["supportedContentTypes"] = supported_content_types


def is_supported_content_type(
        delta_staging_area: Dict[str, Any],
        content_type: ContentType):

    supported_content_types = get_supported_content_types(delta_staging_area)
    return (not supported_content_types) or \
           (content_type.value in supported_content_types)


def get_partition_id(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_partition_id(partition_locator)
    return None


def set_partition_id(
        staging_area: Dict[str, Any],
        partition_id: str):

    pl.set_partition_id(get_partition_locator(staging_area), partition_id)


def get_stream_id(staging_area: Dict[str, Any]) -> Optional[str]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        stream_locator = pl.get_stream_locator(partition_locator)
        if stream_locator:
            return sl.get_stream_id(stream_locator)
    return None


def get_partition_values(staging_area: Dict[str, Any]) -> Optional[List[Any]]:
    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_partition_values(partition_locator)


def get_stream_locator(staging_area: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    partition_locator = get_partition_locator(staging_area)
    if partition_locator:
        return pl.get_stream_locator(partition_locator)


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
