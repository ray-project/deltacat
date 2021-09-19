from deltacat.utils.common import sha1_digest, sha1_hexdigest
from deltacat.storage.model import namespace_locator as nl
from typing import Any, Dict, Optional


def of(
        namespace_locator: Optional[Dict[str, Any]],
        table_name: Optional[str]) -> Dict[str, Any]:

    return {
        "namespaceLocator": namespace_locator,
        "tableName": table_name
    }


def get_namespace_locator(table_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return table_locator.get("namespaceLocator")


def set_namespace_locator(
        table_locator: Dict[str, Any],
        namespace_locator: Optional[Dict[str, Any]]) -> None:

    table_locator["namespaceLocator"] = namespace_locator


def get_table_name(table_locator: Dict[str, Any]) -> Optional[str]:
    return table_locator.get("tableName")


def set_table_name(
        table_locator: Dict[str, Any],
        table_name: Optional[str]) -> None:

    table_locator["tableName"] = table_name


def get_namespace(table_locator: Dict[str, Any]) -> Optional[str]:
    namespace_locator = get_namespace_locator(table_locator)
    if namespace_locator:
        return nl.get_namespace(namespace_locator)
    return None


def canonical_string(table_locator: Dict[str, Any]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    nl_hexdigest = nl.hexdigest(get_namespace_locator(table_locator))
    table_name = get_table_name(table_locator)
    return f"{nl_hexdigest}|{table_name}"


def digest(table_locator: Dict[str, Any]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(canonical_string(table_locator).encode("utf-8"))


def hexdigest(table_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(canonical_string(table_locator).encode("utf-8"))
