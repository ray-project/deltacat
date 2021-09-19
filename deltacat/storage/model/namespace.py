from deltacat.storage.model import namespace_locator as nl
from typing import Any, Dict, Optional


def of(
        namespace_locator: Optional[Dict[str, Any]],
        permissions: Optional[Dict[str, Any]]) -> Dict[str, Any]:

    return {
        "namespaceLocator": namespace_locator,
        "permissions": permissions,
    }


def get_namespace_locator(namespace: Dict[str, Any]) -> Optional[str]:
    return namespace.get("namespaceLocator")


def get_namespace(namespace: Dict[str, Any]) -> Optional[str]:
    namespace_locator = get_namespace_locator(namespace)
    if namespace_locator:
        return nl.get_namespace(namespace_locator)
    return None


def get_permissions(namespace: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return namespace.get("permissions")


def set_permissions(
        namespace: Dict[str, Any],
        permissions: Optional[Dict[str, Any]]) -> None:

    namespace["permissions"] = permissions
