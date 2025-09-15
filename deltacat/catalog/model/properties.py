from __future__ import annotations

from typing import Optional, Any, Union, Dict
import urllib.parse
import time
import posixpath
import logging

import os
import deltacat as dc

import pyarrow
from deltacat.constants import (
    DELTACAT_ROOT,
    CATALOG_VERSION_DIR_NAME,
)

from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    list_directory,
    write_file,
)

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


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


class CatalogVersion(dict):
    """
    DeltaCAT catalog version.
    """

    @staticmethod
    def of(
        version: str,
        starting_from: int,
    ) -> CatalogVersion:
        catalog_version = CatalogVersion()
        catalog_version["version"] = version
        catalog_version["starting_from"] = starting_from
        return catalog_version

    @staticmethod
    def current() -> CatalogVersion:
        return CatalogVersion.of(dc.__version__, time.time_ns())

    @staticmethod
    def from_filename(filename: str) -> CatalogVersion:
        # filename is written as f"{version}.{starting_from}"
        parts = filename.split(".")
        if len(parts) < 2:
            raise ValueError(f"{filename} is not a valid catalog version filename")
        version = ".".join(parts[:-1])  # version is all but the last part
        starting_from = int(parts[-1])  # starting_from is the last part
        return CatalogVersion.of(version, starting_from)

    def to_filename(self) -> str:
        return f"{self.version}.{self.starting_from}"

    @property
    def version(self) -> str:
        return self["version"]

    @property
    def starting_from(self) -> int:
        return self["starting_from"]


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

        version: The current catalog version resolved from the catalog root "version" directory.
            Returns None if no catalog version file exists.

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
        self._version = None

        # Try to read the catalog version file (if it exists)
        version_dir_path = posixpath.join(self._root, CATALOG_VERSION_DIR_NAME)
        version_files_and_sizes = list_directory(
            version_dir_path, self._filesystem, ignore_missing_path=True
        )
        # Extract only the filenames from the version_files
        version_files = [
            posixpath.basename(version_file[0])
            for version_file in version_files_and_sizes
        ]
        # Construct the current catalog version
        current_version = CatalogVersion.current()
        # Check if the catalog has already been initialized with this version
        for version_file in version_files:
            try:
                catalog_version = CatalogVersion.from_filename(version_file)
                if catalog_version.version == current_version.version:
                    self._version = catalog_version
                    break
            except ValueError:
                # Skip files that don't match the expected version filename format
                logger.warning(
                    f"Skipping version file '{version_file}' that doesn't match the expected version filename format"
                )
                continue
        if self._version is None:
            # Try to write the current version file
            version_file_path = posixpath.join(
                self._root, CATALOG_VERSION_DIR_NAME, current_version.to_filename()
            )
            try:
                write_file(
                    version_file_path, current_version.to_filename(), self._filesystem
                )
                self._version = current_version
            except Exception as e:
                # log a warning and continue (user may not have write permissions)
                logger.warning(
                    f"Failed to write current version file '{current_version.to_filename()}': {e}"
                )

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

    @property
    def version(self) -> Optional[CatalogVersion]:
        """
        Return the current catalog version resolved from the catalog root "version" directory.
        Returns None if no catalog version file exists.
        """
        return self._version

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

    def to_serializable(self) -> Dict[str, Any]:
        """
        Convert this CatalogProperties instance into a serializable dictionary,
        suitable for dumping to YAML or JSON.

        Returns:
            dict of primitive config values.
        """
        return {
            "root": getattr(self, "root", None),
            # Non-serializable fields -> we serialize as None for now.
            "filesystem": None,
            "storage": None,
        }

    @staticmethod
    def ensure_serializable(
        obj: Union["CatalogProperties", Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Helper: normalize a CatalogProperties or dict-like (e.g. mock configs)
        into a serializable dictionary. This allows tests/mocks to supply dicts
        instead of CatalogProperties.
        """
        if isinstance(obj, CatalogProperties):
            return obj.to_serializable()
        elif isinstance(obj, dict):
            # Assume dict is already serializable
            return obj
        else:
            raise TypeError(
                f"Unsupported type for serialization: {type(obj)}. "
                f"Expected CatalogProperties or dict."
            )

    @staticmethod
    def from_serializable(config: Union[Dict[str, Any], None]) -> "CatalogProperties":
        """
        Deserialize a config (from YAML, dict, etc.) into a CatalogProperties.

        This method can handle:
          1. A full dict-of-properties (root, filesystem, storage)
          2. A 'primitive-only' dict (Case 1 from YAML) — still allowed

        Args:
            config: A mapping of properties.

        Returns:
            A CatalogProperties instance.

        Raises:
            ValueError: For invalid input types or malformed configs.
        """
        if config is None:
            raise ValueError("Cannot construct CatalogProperties from None")

        if not isinstance(config, dict):
            raise ValueError(
                f"Expected dict for CatalogProperties deserialization, "
                f"got {type(config)}"
            )

        # Validation: all values must be serializable primitives or lists
        if all(
            isinstance(v, (str, int, float, bool, type(None), list))
            for v in config.values()
        ):
            # Accept it directly
            return CatalogProperties(**config)

        # Otherwise, assume it’s a property mapping { key -> value }
        expected_keys = {"root", "filesystem", "storage"}
        for key in config:
            if key not in expected_keys:
                raise ValueError(
                    f"Unrecognized CatalogProperties key '{key}'. "
                    f"Expected one of {expected_keys}"
                )

        return CatalogProperties(**config)

    def __str__(self):
        return (
            f"{self.__class__.__name__}(root={self.root}, filesystem={self.filesystem})"
        )

    def __repr__(self):
        return self.__str__()
