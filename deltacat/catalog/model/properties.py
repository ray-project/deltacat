from __future__ import annotations

from typing import Optional, Any, Dict
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

from deltacat.exceptions import NamespaceAlreadyExistsError

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
        Initialize a CatalogProperties instance. For initializers that have write permissions,
        also runs lightweight, one-time bootstrapping operations against the given catalog
        root (e.g., default table namespace creation).

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
                self._root,
                CATALOG_VERSION_DIR_NAME,
                current_version.to_filename(),
            )
            try:
                write_file(
                    version_file_path,
                    b"",  # empty file
                    self._filesystem,
                )
                self._version = current_version
            except Exception as e:
                # log a warning and continue (user may not have write permissions)
                logger.warning(
                    f"Failed to write current version file '{current_version.to_filename()}': {e}"
                )

        # Migrate any unpartitioned transaction files to partitioned
        try:
            from deltacat.experimental.compatibility.backfill_transaction_partitions import (
                backfill_transaction_partitions,
            )

            backfill_transaction_partitions(self, show_progress=True)
        except Exception as e:
            # CRITICAL: Transaction migration failure must fail catalog initialization
            # to prevent database corruption from mixed partitioned/unpartitioned transactions
            raise RuntimeError(
                f"Failed to migrate unpartitioned transaction files to partitioned structure: {e}. "
                f"Catalog initialization aborted to prevent database corruption."
            ) from e

        # Try to create the default namespace
        try:
            from deltacat.catalog.main.impl import (
                create_namespace,
                default_namespace,
            )

            default_namespace = default_namespace()
            create_namespace(default_namespace, inner=self)
        except NamespaceAlreadyExistsError:
            logger.info(f"Default namespace {default_namespace} already exists.")
        except Exception as e:
            # Some other error occurred - log a warning and continue.
            # (e.g., the default namespace may not exist, but we may not have write permissions to create it)
            logger.warning(f"Failed to create default namespace: {e}")

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

        # If we don't have an original root, return as-is
        if not self._original_root:
            logger.warning(
                f"No original catalog root found, returning path '{path}' as-is"
            )
            return path

        # Reconstruct the full path with the original scheme
        # Handle both absolute and relative paths
        if posixpath.isabs(path):
            # Absolute path - this shouldn't happen normally but handle it
            if self._original_scheme:
                return f"{self._original_scheme}:/{path}"
            else:
                return path
        else:
            # Relative path - prepend the original root (e.g., s3://deltacat/root)
            return posixpath.join(self._original_root, path)

    def to_serializable(self) -> Dict[str, Any]:
        """
        Convert this CatalogProperties instance into a serializable dictionary,
        suitable for dumping to YAML or JSON.

        Returns:
            A dictionary of properties that can be used to reconstruct this
            CatalogProperties instance via CatalogProperties.from_serializable().
        """
        return {"root": self.root}

    @staticmethod
    def from_serializable(config: Dict[str, Any]) -> CatalogProperties:
        """
        Deserialize a config dictionary into CatalogProperties.

        Args:
            config: A dictionary of properties from CatalogProperties.to_serializable().

        Returns:
            A CatalogProperties instance.
        """
        return CatalogProperties(**config)

    def __str__(self):
        return (
            f"{self.__class__.__name__}(root={self.root}, filesystem={self.filesystem})"
        )

    def __repr__(self):
        return self.__str__()
