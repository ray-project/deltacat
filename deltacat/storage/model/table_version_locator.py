from deltacat.utils.common import sha1_digest, sha1_hexdigest
from deltacat.storage.model import table_locator as tl
from typing import Any, Dict, Optional

def of(
        table_locator: Optional[Dict[str, Any]],
        table_version: Optional[str]) -> Dict[str, Any]:

    return {
        "tableLocator": table_locator,
        "tableVersion": table_version,
    }


def get_table_locator(table_version_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return table_version_locator.get("tableLocator")


def set_table_locator(
        table_version_locator: Dict[str, Any],
        table_locator: Optional[Dict[str, Any]]) -> None:

    table_version_locator["tableLocator"] = table_locator


def get_table_version(table_version_locator: Dict[str, Any]) -> Optional[str]:
    return table_version_locator.get("tableVersion")


def set_table_version(
        table_version_locator: Dict[str, Any],
        table_version: Optional[str]) -> None:

    table_version_locator["tableVersion"] = table_version


def get_namespace_locator(table_version_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    table_locator = get_table_locator(table_version_locator)
    if table_locator:
        return tl.get_namespace_locator(table_locator)
    return None


def get_namespace(table_version_locator: Dict[str, Any]) -> Optional[str]:
    table_locator = get_table_locator(table_version_locator)
    if table_locator:
        return tl.get_namespace(table_locator)
    return None


def get_table_name(table_version_locator: Dict[str, Any]) -> Optional[str]:
    table_locator = get_table_locator(table_version_locator)
    if table_locator:
        return tl.get_table_name(table_locator)
    return None


def canonical_string(table_version_locator: Dict[str, Any]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    tl_hexdigest = tl.hexdigest(get_table_locator(table_version_locator))
    table_version = get_table_version(table_version_locator)
    return f"{tl_hexdigest}|{table_version}"


def digest(table_version_locator: Dict[str, Any]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(canonical_string(table_version_locator).encode("utf-8"))


def hexdigest(table_version_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(
        canonical_string(table_version_locator).encode("utf-8")
    )
