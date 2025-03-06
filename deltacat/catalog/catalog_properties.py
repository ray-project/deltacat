from __future__ import annotations
from typing import Any, Dict, Optional, Tuple

import pyarrow
from deltacat.constants import DELTACAT_CATALOG_PROPERTY_ROOT
from deltacat.utils.filesystem import resolve_path_and_filesystem

"""
Property catalog is configured globally (at the interpreter level)

Ray has limitations around serialized class size. For this reason, larger files like catalog impl and
storage impl need to be a flat list of functions rather than a stateful class initialized with properties.
For more details see - README-development.md

These classes will fetch the globally configured CatalogProperties, OR allow injection of a custom
CatalogProperties in kwargs

Example: injecting custom CatalogProperties
    catalog.namespace_exists("my_namespace", catalog_properties=CatalogProperties(root="..."))

Example: explicitly initializing global CatalogProperties
    from deltacat.catalog import initialize_properties
    initialize_properties(root="...")
    catalog.namespace_exists("mynamespace")

By default, catalog properties are initialized automatically, and fall back to defaults/env variables.
Example: using env variables
  os.environ["DELTACAT_ROOT"]="..."
  catalog.namespace_exists("mynamespace")
"""
CATALOG_PROPERTIES: CatalogProperties = None
_INITIALIZED = False


def initialize_properties(
    root: Optional[str] = None, *args, force: bool = False, **kwargs
) -> CatalogProperties:
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
    # This is set or defaulted in constants.py
    env_root = DELTACAT_CATALOG_PROPERTY_ROOT
    if env_root is None:
        raise ValueError(
            "Expected environment variable DELTACAT_ROOT to be set or defaulted"
        )

    # Environment variables are overridden by explicit parameters
    if root is None:
        root = env_root

    # Initialize the catalog properties
    CATALOG_PROPERTIES = CatalogProperties(root=root, **kwargs)

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
    elif properties is not None and not isinstance(properties, CatalogProperties):
        raise ValueError(
            "Expected kwarg catalog_properties to be instance of CatalogProperties"
        )

    # Use the global catalog, initializing if necessary
    if not _INITIALIZED:
        initialize_properties()

    return CATALOG_PROPERTIES


class CatalogProperties:
    """
    This holds all configuration for a DeltaCAT catalog.

    CatalogProperties can be configured at the interpreter level by calling initialize_properties, or provided with
    the kwarg catalog_properties. We expect functions to plumb through kwargs throughout, so only when a property needs to be fetched does a function
    need to retrieve the property catalog. Property catalog must be retrieved through get_property_catalog, which will
    hierarchically check kwargs then the global value.

    Specific properties are configurable via env variable.

    Be aware that parallel code (e.g. parallel tests) may overwrite the catalog properties defined global at the interpreter level
    In this case, you must explicitly provide the kwarg catalog_properties rather than declare it globally with initialize_catalog_properties

    Attributes:
        root (str): URI string The root path where catalog metadata and data files are stored. If none provided,
          will be initialized as .deltacat/ relative to current working directory

        filesystem (pyarrow.fs.FileSystem): pyarrow filesystem implementation used for
            accessing files. If not provided, will be inferred via root
    """

    def __init__(
        self,
        root: str,
        *args,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        **kwargs,
    ):
        """
        Initialize a CatalogProperties instance.

        Args:
            root (str, optional): Root path for the catalog storage. If None, will be resolved later.
            filesystem (pyarrow.fs.FileSystem, optional): FileSystem implementation to use.
                If None, will be resolved based on the root path.
        """
        resolved_root, resolved_filesystem = resolve_path_and_filesystem(
            path=root,
            filesystem=filesystem,
        )
        self._root = resolved_root
        self._filesystem = resolved_filesystem
        
        self._dataset_cache: Dict[Tuple[str, str], Any] = {}

    @property
    def root(self) -> str:
        return self._root

    @property
    def filesystem(self) -> Optional[pyarrow.fs.FileSystem]:
        return self._filesystem
    
    def cache_dataset(self, dataset, namespace: str) -> None:
        """
        Cache a dataset for faster retrieval.
        
        Args:
            dataset: The dataset to cache
            namespace: The namespace the dataset belongs to
        """
        self._dataset_cache[(namespace, dataset.dataset_name)] = dataset
    
    def get_cached_dataset(self, namespace: str, table_name: str) -> Optional[Any]:
        """
        Retrieve a cached dataset if available.
        
        Args:
            namespace: The namespace
            table_name: The table name
            
        Returns:
            The cached dataset or None if not found
        """
        return self._dataset_cache.get((namespace, table_name))
    
    def remove_dataset_from_cache(self, namespace: str, table_name: str) -> None:
        """
        Remove a specific dataset from the cache.
        
        Args:
            namespace: The namespace
            table_name: The table name
        """
        if (namespace, table_name) in self._dataset_cache:
            del self._dataset_cache[(namespace, table_name)]
    
    def remove_namespace_from_cache(self, namespace: str) -> None:
        """
        Remove all datasets for a specific namespace from the cache.
        
        Args:
            namespace: The namespace to remove
        """
        keys_to_remove = []
        for (ns, table), _ in self._dataset_cache.items():
            if ns == namespace:
                keys_to_remove.append((ns, table))
        
        for key in keys_to_remove:
            del self._dataset_cache[key]
    
    def rename_dataset_in_cache(self, namespace: str, old_table_name: str, new_table_name: str) -> None:
        """
        Rename a dataset in the cache.
        
        Args:
            namespace: The namespace 
            old_table_name: The current table name
            new_table_name: The new table name
        """
        if (namespace, old_table_name) in self._dataset_cache:
            dataset = self._dataset_cache[(namespace, old_table_name)]
            # Update the dataset's name property if available
            if hasattr(dataset, 'dataset_name'):
                dataset.dataset_name = new_table_name
            # Store with new key
            self._dataset_cache[(namespace, new_table_name)] = dataset
            # Remove old entry
            del self._dataset_cache[(namespace, old_table_name)]
    
    def rename_namespace_in_cache(self, old_namespace: str, new_namespace: str) -> None:
        """
        Rename a namespace in the cache, updating all associated datasets.
        
        Args:
            old_namespace: The current namespace name
            new_namespace: The new namespace name
        """
        datasets_to_rename = []
        
        # Find all datasets in the cache that belong to this namespace
        for (ns, table_name), dataset in self._dataset_cache.items():
            if ns == old_namespace:
                datasets_to_rename.append((table_name, dataset))
        
        # Remove old entries and add with new namespace
        for table_name, dataset in datasets_to_rename:
            del self._dataset_cache[(old_namespace, table_name)]
            self._dataset_cache[(new_namespace, table_name)] = dataset
    
    def clear_dataset_cache(self) -> None:
        """Clear the entire dataset cache"""
        self._dataset_cache.clear()
        
    def get_dataset_cache_size(self) -> int:
        """
        Get the number of datasets in the cache.
        
        Returns:
            int: The number of cached datasets
        """
        return len(self._dataset_cache)
