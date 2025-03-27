from __future__ import annotations
from typing import Any, Dict

from attr import dataclass
from pyiceberg.catalog import CatalogType


@dataclass
class IcebergCatalogConfig:
    """
    Configuration properties for Iceberg catalog implementation.

    This class holds the PyIceberg Catalog instance needed for interaction with
    Iceberg tables and metadata.

    This configuration is passed through to PyIceberg by invoking load_catalog.
    The Properties provided must match properties accepted by PyIceberg for each catalog type
    See: :func:`deltacat.catalog.iceberg.initialize`

    Attributes:
        type: The PyIceberg Catalog instance
        properties: Dict of properties passed to pyiceberg load_catalog
    """

    type: CatalogType
    properties: Dict[str, Any]
