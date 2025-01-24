import string
from importlib.metadata import files
from typing import Generator
import os

import pyarrow.fs as fs

from deltacat.storage import Delta
from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.file_system import FileSystem
from deltacat.storage.rivulet.fs.fs_utils import construct_filesystem
from deltacat.storage.rivulet.metastore.json_sst import JsonSstReader
from deltacat.storage.rivulet.metastore.delta import (
    ManifestIO,
    DeltaContext,
    RivuletDelta, DeltacatManifestIO,
)
from deltacat.storage.rivulet.metastore.sst import SSTReader, SSTable


class ManifestAccessor:
    """Accessor for retrieving a manifest's SSTable entities."""

    def __init__(
            self, manifest: RivuletDelta, file_store: FileStore, sst_reader: SSTReader
    ):
        self.manifest: RivuletDelta = manifest
        self._file_store: file_store = file_store
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
            sst_file = self._file_store.new_input_file(sst_uri)
            yield self._sst_reader.read(sst_file)


class DatasetMetastore:
    """
    Metastore implementation for manifests stored on a filesystem

    Right now - this stores all SSTs as Deltas directly under

    TODO this metastore needs to be merged with deltacat storage interface (does not exist yet)
    """

    def __init__(
            self,
            # URI at which we expect to find deltas
            delta_root_uri: str,
            # TODO should replace with pyarrow FS interface
            file_store: FileStore,
            *,
            manifest_io: ManifestIO = None,
            sst_reader: SSTReader = None,
    ):
        self.delta_root_uri = delta_root_uri
        self.file_store: FileStore = file_store
        self.manifest_io = manifest_io or DeltacatManifestIO()
        self.sst_reader = sst_reader or JsonSstReader()

    def _get_delta(self,
                   delta_dir: str,
                   filesystem: FileSystem) -> RivuletDelta:
        """
        Given a DeltaCat delta directory, find latest delta file

        NOTE: this will be replaced by deltacat store API.
        Current implementation does not respect open/closed transactions, it just
            looks for the latest revision
        """
        rev_directory = os.path.join(delta_dir, "rev")
        revisions = filesystem.get_file_info(fs.FileSelector(rev_directory))
        # Take lexicographical max
        latest_revision = None
        for revision in revisions:
            latest_revision = revision if not latest_revision else max(latest_revision, revision.path)
        return RivuletDelta.of(Delta.read(latest_revision.path))

    def generate_manifests(self) -> Generator[ManifestAccessor, None, None]:
        """
        Generate all manifests within the Metastore

        :return: a generator of accessors into the Manifests
        """

        root_path, filesystem = construct_filesystem(self.delta_root_uri)
        root_children = filesystem.get_file_info(fs.FileSelector(root_path))
        delta_directories = [child for child in root_children if
                             not child.is_file and not child.path.endswith("txn")]

        for delta_directory in delta_directories:
            rivulet_delta = self._get_delta(delta_directory.path, filesystem)
            yield ManifestAccessor(rivulet_delta, self.file_store, self.sst_reader)


            # for uri in self.location_provider.generate_manifest_uris():
            #    manifest = self.manifest_io.read(uri)
            #    yield ManifestAccessor(manifest, self.file_store, self.sst_reader)

