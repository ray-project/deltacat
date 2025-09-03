from __future__ import annotations

from typing import Optional, Any
import urllib.parse

import os

import pyarrow
from deltacat.constants import DELTACAT_ROOT

from deltacat.utils.filesystem import resolve_path_and_filesystem


def get_catalog_properties(
    *,
    catalog: Optional[CatalogProperties] = None,
    inner: Optional[CatalogProperties] = None,
    **kwargs,
) -> CatalogProperties:
    """
    Helper function to fetch CatalogProperties instance.

    This will look first look for CatalogProperties in either "catalog"
    or "inner" and otherwise passes all keyword arguments to the
    CatalogProperties constructor.
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

    Catalog and storage APIs rely on the property catalog to retrieve durable
    state about the catalog they're working against.

    Attributes:
        root: The root path for catalog metadata and data storage. Resolved by
            searching for the root path in the following order:
            1. "root" constructor input argument
            2. "DELTACAT_ROOT" system environment variable
            3. default to "./.deltacat/"

        filesystem: The filesystem implementation that should be used for
            reading/writing files. If None, a filesystem will be inferred from
            the catalog root path.

        storage: Storage class implementation (overrides default filesystem
            storage impl)
    """

    def __init__(
        self,
        root: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        storage=None,
    ):
        """
        Initialize a CatalogProperties instance.

        Args:
            root: Catalog root directory path. Uses the "DELTACAT_ROOT"
                system environment variable if not set, and defaults to
                "./.deltacat/" if this environment variable is not set.
            filesystem: The filesystem implementation that should be used for
                reading these files. If None, a filesystem will be inferred.
                If provided, this will be validated for compatibility with the
                catalog root path.
            storage: DeltaCAT storage implementation override.
        """
        # set root, using precedence rules described in pydoc
        if root is None:
            # Check environment variables
            root = DELTACAT_ROOT
            if not root:
                # Default to "./.deltacat/"
                root = os.path.join(os.getcwd(), ".deltacat")

        # Store the original root with its scheme for reconstruction later
        self._original_root = root
        self._original_scheme = urllib.parse.urlparse(root).scheme

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

    def reconstruct_full_path(self, path: str) -> str:
        """
        Reconstruct a full path with the original scheme for external readers.

        This addresses GitHub issue #567 by ensuring that cloud storage URIs
        include the relevant scheme prefix (e.g., s3://) that some file readers
        require regardless of the filesystem being used to read the file
        (e.g., Daft).

        Args:
            path: A path relative to the catalog root or absolute path

        Returns:
            Full path with appropriate scheme prefix for external readers
        """
        # If the path already has a scheme, return it as-is
        if urllib.parse.urlparse(path).scheme:
            return path

        # If we don't have an original scheme (local filesystem), return as-is
        if not self._original_scheme:
            return path

        # Reconstruct the full path with the original scheme
        # Handle both absolute and relative paths
        if path.startswith("/"):
            # Absolute path - this shouldn't happen normally but handle it
            return f"{self._original_scheme}:/{path}"
        else:
            # Relative path - prepend the s3:// scheme
            return f"{self._original_scheme}://{path}"

    def __str__(self):
        return (
            f"{self.__class__.__name__}(root={self.root}, filesystem={self.filesystem})"
        )

    def __repr__(self):
        return self.__str__()
