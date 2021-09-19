from deltacat.utils.common import sha1_digest, sha1_hexdigest
from typing import Any, Dict, Optional


def of(namespace: Optional[str]) -> Dict[str, Any]:

    return {
        "namespace": namespace,
    }


def get_namespace(namespace_locator: Dict[str, Any]) -> Optional[str]:
    return namespace_locator.get("namespace")


def set_namespace(namespace_locator: Dict[str, Any], namespace: Optional[str]) \
        -> None:
    namespace_locator["namespace"] = namespace


def canonical_string(namespace_locator: Dict[str, Any]) -> str:
    """
    Returns a unique string for the given locator that can be used
    for equality checks (i.e. two locators are equal if they have
    the same canonical string).
    """
    return get_namespace(namespace_locator)


def digest(namespace_locator: Dict[str, Any]) -> bytes:
    """
    Return a digest of the given locator that can be used for
    equality checks (i.e. two locators are equal if they have the
    same digest) and uniform random hash distribution.
    """
    return sha1_digest(canonical_string(namespace_locator).encode("utf-8"))


def hexdigest(namespace_locator: Dict[str, Any]) -> str:
    """
    Returns a hexdigest of the given locator suitable
    for use in equality (i.e. two locators are equal if they have the same
    hexdigest) and inclusion in URLs.
    """
    return sha1_hexdigest(canonical_string(namespace_locator).encode("utf-8"))
