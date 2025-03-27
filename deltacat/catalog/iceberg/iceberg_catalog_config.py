from __future__ import annotations
from typing import Any, Dict
from pyiceberg.catalog import CatalogType


class IcebergCatalogConfig(Dict[str, Any]):
    """
    Configuration properties for Iceberg catalog implementation.

    This class holds the PyIceberg Catalog instance needed for interaction with
    Iceberg tables and metadata.

    Attributes:
        catalog: The PyIceberg Catalog instance
    """

    def __init__(self, *args, type: CatalogType, properties: Dict[str, Any]):
        """
        Initialize an IcebergCatalogConfig with a PyIceberg Catalog.

        Args:
            catalog: PyIceberg Catalog instance. If None, other kwargs are passed to initialize a catalog.
            **kwargs: Additional arguments passed to create or configure the catalog
        """
        super().__init__()
        self["type"] = type
        self["properties"] = properties

    @property
    def type(self) -> CatalogType:
        """
        The PyIceberg Catalog instance.
        """
        return self["type"]

    @property
    def properties(self) -> Dict[str, Any]:
        """
        The PyIceberg Catalog instance.
        """
        return self["properties"]
