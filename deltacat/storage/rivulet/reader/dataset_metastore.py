import logging
import posixpath
from typing import Generator, Optional

import pyarrow
import pyarrow.fs

from deltacat.storage import Delta
from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.rivulet.fs.file_provider import FileProvider
from deltacat.utils.filesystem import resolve_path_and_filesystem
from deltacat.storage.rivulet.metastore.json_sst import JsonSstReader
from deltacat.storage.rivulet.metastore.delta import (
    ManifestIO,
    DeltaContext,
    RivuletDelta,
    DeltacatManifestIO,
)
from deltacat.storage.rivulet.metastore.sst import SSTReader, SSTable
from deltacat.utils.metafile_locator import _find_table_path
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class ManifestAccessor:
    """Accessor for retrieving a manifest's SSTable entities."""

    def __init__(
        self, delta: RivuletDelta, file_provider: FileProvider, sst_reader: SSTReader
    ):
        self.manifest: RivuletDelta = delta
        self.file_provider: FileProvider = file_provider
        self._sst_reader = sst_reader

    @property
    def context(self) -> DeltaContext:
        return self.manifest.context

    def generate_sstables(self) -> Generator[SSTable, None, None]:
        """
        Generate the SortedString Tables from this Manifest

        :return a generator of SSTables for this manifest
        """
        for sst_uri in self.manifest.sst_files:
            sst_file = self.file_provider.provide_input_file(sst_uri)
            yield self._sst_reader.read(sst_file)


class DatasetMetastore:
    """
    Metastore implementation for manifests stored on a filesystem

    TODO this will be replaced with Deltacat Storage interface - https://github.com/ray-project/deltacat/issues/477
    """

    def __init__(
        self,
        # URI at which we expect to find deltas
        delta_root_uri: str,
        file_provider: FileProvider,
        locator: PartitionLocator,
        *,
        manifest_io: ManifestIO = None,
        sst_reader: SSTReader = None,
    ):
        self._min_key = None
        self._max_key = None
        self.delta_root_uri = delta_root_uri
        self.file_provider = file_provider
        self.manifest_io = manifest_io or DeltacatManifestIO(delta_root_uri, locator)
        self.sst_reader = sst_reader or JsonSstReader()
        self.locator = locator

    def _get_delta(
        self, delta_dir: str, filesystem: pyarrow.fs.FileSystem
    ) -> Optional[RivuletDelta]:
        """
        Find the latest revision in a delta directory.

        param: delta_dir: The directory containing the revisions.
        param: filesystem: The filesystem to search for the revisions.
        returns: The latest revision as a RivuletDelta.
        """
        rev_directory = posixpath.join(delta_dir, "rev")
        revisions = filesystem.get_file_info(
            pyarrow.fs.FileSelector(rev_directory, allow_not_found=True)
        )

        if not revisions:
            logger.warning(f"No revision files found in {rev_directory}")
            return None

        # Take lexicographical max to find the latest revision
        latest_revision = max(revisions, key=lambda f: f.path)

        return (
            RivuletDelta.of(Delta.read(latest_revision.path))
            if latest_revision
            else None
        )

    def generate_manifests(self) -> Generator[ManifestAccessor, None, None]:
        """
        Generate all manifests within the Metastore
        NOTE: this will be replaced by deltacat storage API.

        TODO: Generate partition path using Deltacat Storage interface.

        param: delta_root_uri: The URI at which we expect to find deltas.
        returns: a generator of ManifestAccessors for all manifests in the dataset.
        """

        root_path, filesystem = resolve_path_and_filesystem(self.delta_root_uri)

        partition_path = posixpath.join(
            _find_table_path(root_path, filesystem),
            self.locator.table_version,
            self.locator.stream_id,
            self.locator.partition_id,
        )

        partition_info = filesystem.get_file_info(partition_path)

        if partition_info.type != pyarrow.fs.FileType.Directory:
            logger.debug(f"Partition directory {partition_path} not found. Skipping.")
            return

        # Locate "rev" directory inside the partition
        rev_directory = posixpath.join(partition_path, "rev")
        rev_info = filesystem.get_file_info(rev_directory)

        if rev_info.type != pyarrow.fs.FileType.Directory:
            logger.debug(f"Revision directory {rev_directory} not found. Skipping.")
            return

        # Fetch all delta directories inside the partition
        delta_dirs = filesystem.get_file_info(
            pyarrow.fs.FileSelector(
                partition_path, allow_not_found=True, recursive=False
            )
        )

        delta_dirs = [
            delta
            for delta in delta_dirs
            if delta.type == pyarrow.fs.FileType.Directory and delta.base_name.isdigit()
        ]

        for delta_dir in delta_dirs:
            rivulet_delta = self._get_delta(delta_dir.path, filesystem)
            if rivulet_delta:
                yield ManifestAccessor(
                    rivulet_delta, self.file_provider, self.sst_reader
                )

    def get_min_max_keys(self):
        """
        Compute and cache the minimum and maximum keys in the dataset.

        returns: a tuple of the minimum and maximum keys in the dataset
        """
        if self._min_key is not None and self._max_key is not None:
            return (self._min_key, self._max_key)

        min_key = None
        max_key = None
        for manifest_accessor in self.generate_manifests():
            for sstable in manifest_accessor.generate_sstables():
                if min_key is None or sstable.min_key < min_key:
                    min_key = sstable.min_key
                if max_key is None or sstable.max_key > max_key:
                    max_key = sstable.max_key

        self._min_key = min_key
        self._max_key = max_key
        return (min_key, max_key)
