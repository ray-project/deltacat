from deltacat.storage.model import table_version_locator as tvl
from deltacat.utils.common import sha1_digest, sha1_hexdigest
from typing import Any, Dict, Optional


def of(
        table_version_locator: Optional[Dict[str, Any]],
        stream_id: Optional[str],
        storage_type: Optional[str]) -> Dict[str, Any]:

    """
    Creates a table version Stream Locator. All input parameters are
    case-sensitive.
    """
    return {
        "tableVersionLocator": table_version_locator,
        "streamId": stream_id,
        "storageType": storage_type,
    }


def get_table_version_locator(stream_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    return stream_locator.get("tableVersionLocator")


def set_table_version_locator(
        stream_locator: Dict[str, Any],
        table_version_locator: Optional[Dict[str, Any]]) -> None:

    stream_locator["tableVersionLocator"] = table_version_locator


def get_stream_id(stream_locator: Dict[str, Any]) -> Optional[str]:
    return stream_locator.get("streamId")


def set_stream_id(
        stream_locator: Dict[str, Any],
        stream_id: Optional[str]) -> None:

    stream_locator["streamId"] = stream_id


def get_storage_type(stream_locator: Dict[str, Any]) -> Optional[str]:
    return stream_locator.get("storageType")


def set_storage_type(
        stream_locator: Dict[str, Any],
        storage_type: Optional[str]) -> None:

    stream_locator["storageType"] = storage_type


def get_namespace_locator(stream_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    table_version_locator = get_table_version_locator(stream_locator)
    if table_version_locator:
        return tvl.get_namespace_locator(table_version_locator)
    return None


def get_table_locator(stream_locator: Dict[str, Any]) \
        -> Optional[Dict[str, Any]]:

    table_version_locator = get_table_version_locator(stream_locator)
    if table_version_locator:
        return tvl.get_table_locator(table_version_locator)
    return None


def get_namespace(stream_locator: Dict[str, Any]) -> Optional[str]:
    table_version_locator = get_table_version_locator(stream_locator)
    if table_version_locator:
        return tvl.get_namespace(table_version_locator)
    return None


def get_table_name(stream_locator: Dict[str, Any]) -> Optional[str]:
    table_version_locator = get_table_version_locator(stream_locator)
    if table_version_locator:
        return tvl.get_table_name(table_version_locator)
    return None


def get_table_version(stream_locator: Dict[str, Any]) -> Optional[str]:
    table_version_locator = get_table_version_locator(stream_locator)
    if table_version_locator:
        return tvl.get_table_version(table_version_locator)
    return None


def canonical_string(stream_locator: Dict[str, Any]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    tvl_hexdigest = tvl.hexdigest(get_table_version_locator(stream_locator))
    stream_id = get_stream_id(stream_locator)
    storage_type = get_storage_type(stream_locator)
    return f"{tvl_hexdigest}|{stream_id}|{storage_type}"


def digest(stream_locator: Dict[str, Any]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(canonical_string(stream_locator).encode("utf-8"))


def hexdigest(stream_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(canonical_string(stream_locator).encode("utf-8"))
