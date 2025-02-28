from __future__ import annotations
import os
from typing import Optional

import pyarrow

from deltacat.utils.filesystem import resolve_path_and_filesystem

"""
Property catalog is configured globally (at the interpreter level)

Ray has limitations around serialized class size. For this reason, larger files like catalog impl and
storage impl need to be a flat list of functions rather than a stateful class initialized with properties

These classes will fetch the globally configured CatalogProperties, OR allow injection of a custom 
CatalogProperties in kwargs
"""
CATALOG_PROPERTIES: CatalogProperties = None
_INITIALIZED = False

def initialize_properties(root: Optional[str] = None,
               *args,
               force: bool = False,
               **kwargs) -> CatalogProperties:
    """
    Initialize a Catalog state, if not already initialized.

    If environment variables are present, will check the following environment variables to configure catalog:
        DELTACAT_ROOT: maps to "root" parameter

    Environment variables will be overridden if explicit parameters are provided

    Args:
        root: filesystem URI for catalog root
        force: if True, will re-initialize even if global catalog exists. If False, will return global catalog
    """
    global _INITIALIZED, CATALOG_PROPERTIES

    if _INITIALIZED and not force:
        return CATALOG_PROPERTIES

    # Check environment variables
    env_root = os.environ.get("DELTACAT_ROOT")

    # Environment variables are overridden by explicit parameters
    if root is None and env_root is not None:
        root = env_root

    # Initialize the catalog properties
    CATALOG_PROPERTIES = CatalogProperties(
        root=root,
        **kwargs
    )

    _INITIALIZED = True
    return CATALOG_PROPERTIES


def get_catalog_properties(**kwargs) -> CatalogProperties:
    """
    Helper function to get the appropriate CatalogProperties instance.

    If 'catalog_properties' is provided in kwargs, it will be used.
    Otherwise, it will use the global catalog, initializing it if necessary.

    Args:
        **kwargs: Keyword arguments that might contain 'properties'

    Returns:
        CatalogProperties: The catalog properties to use
    """
    properties = kwargs.get("catalog_properties")
    if properties is not None and isinstance(properties, CatalogProperties):
        return properties

    # Use the global catalog, initializing if necessary
    if not _INITIALIZED:
        initialize_properties()

    return CATALOG_PROPERTIES

class CatalogProperties:
    """
    This holds all configuration for a DeltaCAT catalog.
    A catalog implementation that stores and retrieves properties for tables and namespaces.

    Attributes:
        root (str): URI string The root path where catalog metadata and data files are stored. If none provided,
          will be initialized as .deltacat/ relative to current working directory

        filesystem (pyarrow.fs.FileSystem): pyarrow filesystem implementation used for
            accessing files. If not provided, will be inferred via root
    """
    DEFAULT_ROOT = ".deltacat"

    def __init__(
            self,
            root: Optional[str] = None,
            *args,
            filesystem: Optional[pyarrow.fs.FileSystem] = None,
            **kwargs
    ):
        """
        Initialize a CatalogProperties instance.

        Args:
            root (str, optional): Root path for the catalog storage. If None, will be resolved later.
            filesystem (pyarrow.fs.FileSystem, optional): FileSystem implementation to use.
                If None, will be resolved based on the root path.
        """
        if root is None:
            self._root = os.path.join(os.getcwd(), ".deltacat")

        resolved_root, resolved_filesystem = resolve_path_and_filesystem(
            path=root,
            filesystem=filesystem,
        )
        self._root = resolved_root
        self._filesystem = resolved_filesystem

    @property
    def root(self) -> str:
        return self._root

    @property
    def filesystem(self) -> Optional[pyarrow.fs.FileSystem]:
        return self._filesystem

