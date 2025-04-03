from __future__ import annotations
from typing import Optional, Any

import pyarrow
from deltacat.constants import DELTACAT_ROOT

from deltacat.utils.filesystem import resolve_path_and_filesystem


def get_catalog_properties(
    *args,
    catalog: Optional[CatalogProperties] = None,
    inner: Optional[CatalogProperties] = None,
    **kwargs,
) -> CatalogProperties:
    """
    Helper function to fetch CatalogProperties instance. You are meant to call this by providing your functions
    kwargs, OR to directly pass through CatalogProperty configuration keys like "root" in kwargs.

    This will look for a CatalogProperty value in the kwargs "catalog" or "inner". If these are found, it returns
    the CatalogProperty value under that kwarg. Otherwise, it will pass through kwargs to the CatalogProperties
    constructor.
    """
    properties = catalog if catalog is not None else inner
    if properties is not None and isinstance(properties, CatalogProperties):
        return properties
    elif properties is not None and not isinstance(properties, CatalogProperties):
        raise ValueError(
            f"Expected catalog properties of type {CatalogProperties.__name__} "
            f"but found {type(properties)}."
        )
    else:
        return CatalogProperties(**kwargs)


class CatalogProperties:
    """
    DeltaCAT catalog properties used to deterministically resolve a durable
    DeltaCAT catalog instance. Properties are set from system environment
    variables unless explicit overrides are provided during initialization.

    Catalog and storage APIs rely on the property catalog to retrieve durable state about the catalog they're
    working against.

    Attributes:
        root (str): URI string The root path where catalog metadata and data
            files are stored. Root is determined (in prededence order) by:
            1. check kwargs for "root"
            2. check env variable "DELTACAT_ROOT"
            3. default to ${cwd}/.deltacat

        filesystem: The filesystem implementation that should be used for
            reading/writing files. If None, a filesystem will be inferred from
            the catalog root path.

        storage: Storage class implementation (overrides default filesystem storage impl)
    """

    def __init__(
        self,
        root: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        storage=None,
        *args,
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
        # set root, using precedence rules described in pydoc
        if root is None:
            # Check environment variables
            # This is set or defaulted in constants.py
            root = DELTACAT_ROOT
            if root is None:
                raise ValueError(
                    "Expected environment variable DELTACAT_ROOT to be set or defaulted"
                )

        resolved_root, resolved_filesystem = resolve_path_and_filesystem(
            path=root,
            filesystem=filesystem,
        )
        self._root = resolved_root
        self._filesystem = resolved_filesystem
        self._storage = storage

    @property
    def root(self) -> str:
        return self._root

    @property
    def filesystem(self) -> Optional[pyarrow.fs.FileSystem]:
        return self._filesystem

    @property
    def storage(self) -> Optional[Any]:
        """
        Return overridden storage impl, if any
        """
        return self._storage

    def __str__(self):
        return (
            f"{self.__class__.__name__}(root={self.root}, filesystem={self.filesystem})"
        )

    def __repr__(self):
        return self.__str__()
