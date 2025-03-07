from __future__ import annotations
from typing import Optional

import pyarrow
from deltacat.constants import DELTACAT_ROOT

from deltacat.utils.filesystem import resolve_path_and_filesystem

"""
Global (i.e., interpreter-level) DeltaCAT catalog property config attributes.

These will be set to catalog property system environment variables by
default, unless overridden by custom catalog properties provided to catalog or 
storage APIs directly.

Example: injecting custom CatalogProperties
.. code-block:: python
    catalog.namespace_exists(
        "my_namespace",
        catalog=CatalogProperties(root="..."),
    )

Example: explicitly initializing global CatalogProperties
.. code-block:: python
    from deltacat.catalog import initialize_properties
    initialize_properties(root="...")
    catalog.namespace_exists("mynamespace")

Example: using env variables
  os.environ["DELTACAT_ROOT"]="..."
  catalog.namespace_exists("mynamespace")
"""
CATALOG_PROPERTIES: CatalogProperties = None
_INITIALIZED = False


def initialize_properties(
    root: Optional[str] = None,
    *args,
    force: bool = False,
    **kwargs,
) -> CatalogProperties:
    """
    Initializes interpreter-global catalog properties.

    Checks the following environment variables to configure catalog:
        DELTACAT_ROOT: default value for the "root" parameter if unspecified

    Args:
        root: filesystem URI for catalog root (overrides the `DELTACAT_ROOT`
        system environment variable)
        force: if True, will re-initialize the interpreter-global catalog. If
        False, will return any previously initialized catalog .
    """
    global _INITIALIZED, CATALOG_PROPERTIES

    if _INITIALIZED and not force:
        return CATALOG_PROPERTIES

    # Check environment variables
    # This is set or defaulted in constants.py
    env_root = DELTACAT_ROOT
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


def get_catalog_properties(
    *args,
    catalog: Optional[CatalogProperties] = None,
    **kwargs,
) -> CatalogProperties:
    """
    Helper function to get the appropriate CatalogProperties instance.

    If 'catalog_properties' is provided it will be used. Otherwise, catalog
    properties will be read from system environment variables.

    Args:
        catalog: Catalog property overrides to use instead of
        system environment variables.

    Returns:
        CatalogProperties: Instantiated catalog properties.
    """
    properties = catalog
    if properties is not None and isinstance(properties, CatalogProperties):
        return properties
    elif properties is not None and not isinstance(properties, CatalogProperties):
        raise ValueError(
            f"Expected catalog properties of type {CatalogProperties.__name__} "
            f"but found {type(properties)}."
        )

    # Use the global catalog, initializing if necessary
    if not _INITIALIZED:
        initialize_properties()

    return CATALOG_PROPERTIES


class CatalogProperties:
    """
    DeltaCAT catalog properties used to deterministically resolve a durable
    DeltaCAT catalog instance. Properties are set from system environment
    variables unless explicit overrides are provided during initialization.

    Can be configured at the interpreter level by calling initialize_properties,
    or provided directly to catalog/store APIs. Catalog and storage APIs rely
    on the property catalog to retrieve durable state about the catalog they're
    working against.

    Since interpreter-global catalog properties are not multi-process or thread
    safe, parallel code (e.g. parallel tests) they should only be used to
    simplify local, single-process development. For all other use-cases,
    properties must be passed explicitly into all catalog and storage APIs.

    Attributes:
        root (str): URI string The root path where catalog metadata and data
            files are stored. Set to {current-working-dir}/.deltacat if None.

        filesystem: The filesystem implementation that should be used for
            reading/writing files. If None, a filesystem will be inferred from
            the catalog root path.
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
            root: A single directory path that serves as the catalog root dir.
            filesystem: The filesystem implementation that should be used for
                reading these files. If None, a filesystem will be inferred.
                If not None, the provided filesystem will still be validated
                against the provided path to ensure compatibility.
        """
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

    def __str__(self):
        return (
            f"{self.__class__.__name__}(root={self.root}, filesystem={self.filesystem})"
        )

    def __repr__(self):
        return self.__str__()
