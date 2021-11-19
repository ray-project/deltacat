# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional

from deltacat.storage import Locator


class Namespace(dict):
    @staticmethod
    def of(namespace_locator: Optional[NamespaceLocator],
           permissions: Optional[Dict[str, Any]]) -> Namespace:
        return Namespace({
            "namespaceLocator": namespace_locator,
            "permissions": permissions,
        })

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        return self.get("namespaceLocator")

    @property
    def namespace(self) -> Optional[str]:
        namespace_locator = self.namespace_locator
        if namespace_locator:
            return namespace_locator.namespace
        return None

    @property
    def permissions(self) -> Optional[Dict[str, Any]]:
        return self.get("permissions")

    @permissions.setter
    def permissions(self, permissions: Optional[Dict[str, Any]]) -> None:
        self["permissions"] = permissions


class NamespaceLocator(Locator, dict):
    @staticmethod
    def of(namespace: Optional[str]) -> NamespaceLocator:
        return NamespaceLocator({
            "namespace": namespace,
        })

    @property
    def namespace(self) -> Optional[str]:
        return self.get("namespace")

    @namespace.setter
    def namespace(self, namespace: Optional[str]) -> None:
        self["namespace"] = namespace

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        return self.namespace
