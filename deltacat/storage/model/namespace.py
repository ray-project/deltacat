# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional

from deltacat.storage.model.metafile import Metafile
from deltacat.storage.model.locator import Locator

NamespaceProperties = Dict[str, Any]


class Namespace(Metafile):
    @staticmethod
    def of(
        locator: Optional[NamespaceLocator],
        properties: Optional[NamespaceProperties] = None,
    ) -> Namespace:
        namespace = Namespace()
        namespace.locator = locator
        namespace.properties = properties
        return namespace

    @property
    def locator(self) -> Optional[NamespaceLocator]:
        val: Dict[str, Any] = self.get("namespaceLocator")
        if val is not None and not isinstance(val, NamespaceLocator):
            self.locator = val = NamespaceLocator(val)
        return val

    @locator.setter
    def locator(self, namespace_locator: Optional[NamespaceLocator]) -> None:
        self["namespaceLocator"] = namespace_locator

    @property
    def namespace(self) -> Optional[str]:
        namespace_locator = self.locator
        if namespace_locator:
            return namespace_locator.namespace
        return None

    @property
    def properties(self) -> Optional[NamespaceProperties]:
        return self.get("properties")

    @properties.setter
    def properties(self, properties: Optional[NamespaceProperties]) -> None:
        self["properties"] = properties


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
