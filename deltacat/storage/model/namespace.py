# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional

from deltacat.storage.model.locator import Locator


class Namespace(dict):
    @staticmethod
    def of(locator: Optional[NamespaceLocator],
           permissions: Optional[Dict[str, Any]]) -> Namespace:
        namespace = Namespace()
        namespace.locator = locator
        namespace.permissions = permissions
        return namespace

    @property
    def locator(self) -> Optional[NamespaceLocator]:
        val: Dict[str, Any] = self.get("namespaceLocator")
        if val is not None and not isinstance(val, NamespaceLocator):
            self.locator = val = NamespaceLocator(val)
        return val

    @locator.setter
    def locator(
            self,
            namespace_locator: Optional[NamespaceLocator]) -> None:
        self["namespaceLocator"] = namespace_locator

    @property
    def namespace(self) -> Optional[str]:
        namespace_locator = self.locator
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
        namespace_locator = NamespaceLocator()
        namespace_locator.namespace = namespace
        return namespace_locator

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
