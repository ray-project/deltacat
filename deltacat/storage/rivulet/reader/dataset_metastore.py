from typing import Generator

from deltacat.storage.rivulet.fs.file_provider import FileProvider
from deltacat.storage.rivulet.metastore.json_sst import JsonSstReader
from deltacat.storage.rivulet.metastore.manifest import (
    ManifestIO,
    JsonManifestIO,
    ManifestContext,
    Manifest,
)
from deltacat.storage.rivulet.metastore.sst import SSTReader, SSTable


class ManifestAccessor:
    """Accessor for retrieving a manifest's SSTable entities."""

    def __init__(
        self, manifest: Manifest, file_provider: FileProvider, sst_reader: SSTReader
    ):
        self.manifest: Manifest = manifest
        self.file_provider: FileProvider = file_provider
        self._sst_reader = sst_reader

    @property
    def context(self) -> ManifestContext:
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
    """Metastore implementation for manifests stored on a filesystem"""

    def __init__(
        self,
        file_provider: FileProvider,
        manifest_io: ManifestIO = None,
        sst_reader: SSTReader = None,
    ):
        self._min_key = None
        self._max_key = None
        self.file_provider = file_provider
        self.manifest_io = manifest_io or JsonManifestIO()
        self.sst_reader = sst_reader or JsonSstReader()

    def generate_manifests(self) -> Generator[ManifestAccessor, None, None]:
        """
        Generate all manifests within the Metastore

        :return: a generator of accessors into the Manifests
        """
        for uri in self.file_provider.generate_manifest_uris():
            manifest = self.manifest_io.read(uri)
            yield ManifestAccessor(manifest, self.file_provider, self.sst_reader)

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
