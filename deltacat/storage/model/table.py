from deltacat.storage.model import table_locator as tl
from typing import Any, Dict, Optional


def of(
        table_locator: Optional[Dict[str, Any]],
        table_permissions: Optional[Dict[str, Any]] = None,
        table_description: Optional[str] = None,
        table_properties: Optional[Dict[str, str]] = None) -> Dict[str, Any]:

    return {
        "tableLocator": table_locator,
        "permissions": table_permissions,
        "description": table_description,
        "properties": table_properties,
    }


def get_table_locator(table: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return table.get("tableLocator")


def set_table_locator(
        table: Dict[str, Any],
        table_locator: Optional[Dict[str, Any]]) -> None:

    table["tableLocator"] = table_locator


def get_permissions(table: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return table.get("permissions")


def set_permissions(
        namespace: Dict[str, Any],
        permissions: Optional[Dict[str, Any]]) -> None:

    namespace["permissions"] = permissions


def get_description(table: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return table.get("description")


def set_description(
        table: Dict[str, Any],
        description: Optional[str]) -> None:

    table["description"] = description


def get_properties(table: Dict[str, Any]) -> Optional[Dict[str, str]]:
    return table.get("properties")


def set_properties(
        table: Dict[str, Any],
        properties: Optional[Dict[str, str]]) -> None:

    table["properties"] = properties


def get_namespace_locator(table: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    table_locator = get_table_locator(table)
    if table_locator:
        return tl.get_namespace_locator(table_locator)
    return None


def get_namespace(table: Dict[str, Any]) -> Optional[str]:
    table_locator = get_table_locator(table)
    if table_locator:
        return tl.get_namespace(table_locator)
    return None


def get_table_name(table: Dict[str, Any]) -> Optional[str]:
    table_locator = get_table_locator(table)
    if table_locator:
        return tl.get_table_name(table_locator)
    return None
