import pyarrow as pa
from deltacat.types.media import ContentType
from deltacat.storage.model import table_version_locator as tvl
from typing import Any, Dict, List, Optional, Union


def of(
        table_version_locator: Optional[Dict[str, Any]],
        schema: Optional[Union[pa.Schema, str, bytes]],
        partition_keys: Optional[List[Dict[str, Any]]] = None,
        primary_key_column_names: Optional[List[str]] = None,
        table_version_description: Optional[str] = None,
        table_version_properties: Optional[Dict[str, str]] = None,
        supported_content_types: Optional[List[ContentType]] = None) \
        -> Dict[str, Any]:

    return {
        "tableVersionLocator": table_version_locator,
        "schema": schema,
        "partitionKeys": partition_keys,
        "primaryKeys": primary_key_column_names,
        "description": table_version_description,
        "properties": table_version_properties,
        "contentTypes": supported_content_types,
    }


def get_table_version_locator(table_version: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:
    return table_version.get("tableVersionLocator")


def set_table_version_locator(
        table_version: Dict[str, Any],
        table_version_locator: Optional[Dict[str, Any]]) -> None:

    table_version["tableVersionLocator"] = table_version_locator


def get_schema(table_version: Dict[str, Any]) -> \
        Optional[Union[pa.Schema, str, bytes]]:

    return table_version.get("schema")


def set_schema(
        table_version: Dict[str, Any],
        schema: Optional[Union[pa.Schema, str, bytes]]) -> None:

    table_version["schema"] = schema


def get_partition_keys(table_version: Dict[str, Any]) -> \
        Optional[List[Dict[str, Any]]]:

    return table_version.get("partitionKeys")


def set_partition_keys(
        table_version: Dict[str, Any],
        partition_keys: Optional[List[Dict[str, Any]]]) -> None:

    table_version["partitionKeys"] = partition_keys


def get_primary_keys(table_version: Dict[str, Any]) -> \
        Optional[List[str]]:

    return table_version.get("primaryKeys")


def set_primary_keys(
        table_version: Dict[str, Any],
        primary_keys: Optional[List[str]]) -> None:

    table_version["primaryKeys"] = primary_keys


def get_description(table_version: Dict[str, Any]) -> Optional[str]:
    return table_version.get("description")


def set_description(
        table_version: Dict[str, Any],
        description: Optional[str]) -> None:

    table_version["description"] = description


def get_properties(table_version: Dict[str, Any]) -> Optional[Dict[str, str]]:
    return table_version.get("properties")


def set_properties(
        table_version: Dict[str, Any],
        properties: Optional[Dict[str, str]]) -> None:

    table_version["properties"] = properties


def get_content_types(table_version: Dict[str, Any]) -> \
        Optional[List[ContentType]]:

    content_types = table_version.get("contentTypes")
    return None if content_types is None else \
        [None if _ is None else ContentType(_) for _ in content_types]


def set_content_types(
        table_version: Dict[str, Any],
        content_types: Optional[List[ContentType]]) -> None:

    table_version["contentTypes"] = content_types


def is_supported_content_type(
        delta_staging_area: Dict[str, Any],
        content_type: ContentType):

    supported_content_types = get_content_types(delta_staging_area)
    return (not supported_content_types) or \
           (content_type in supported_content_types)


def get_namespace_locator(table_version: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    table_version_locator = get_table_version_locator(table_version)
    if table_version_locator:
        return tvl.get_namespace_locator(table_version_locator)
    return None


def get_table_locator(table_version: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    table_version_locator = get_table_version_locator(table_version)
    if table_version_locator:
        return tvl.get_table_locator(table_version_locator)
    return None


def get_namespace(table_version: Dict[str, Any]) -> Optional[str]:
    table_version_locator = get_table_version_locator(table_version)
    if table_version_locator:
        return tvl.get_namespace(table_version_locator)
    return None


def get_table_name(table_version: Dict[str, Any]) -> Optional[str]:
    table_version_locator = get_table_version_locator(table_version)
    if table_version_locator:
        return tvl.get_table_name(table_version_locator)
    return None


def get_table_version(table_version: Dict[str, Any]) -> Optional[str]:
    table_version_locator = get_table_version_locator(table_version)
    if table_version_locator:
        return tvl.get_table_version(table_version_locator)
    return None
