from __future__ import annotations

from typing import Protocol, NamedTuple, List
import time

from deltacat.storage import (
    ManifestMeta,
    EntryType,
    DeltaLocator,
    Delta,
    DeltaType,
    Transaction,
    TransactionType,
    TransactionOperation,
    TransactionOperationType,
)
from deltacat.storage.model.manifest import Manifest, ManifestEntryList, ManifestEntry
from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.model.transaction import TransactionOperationList

from deltacat.storage.model.types import StreamFormat
from deltacat.storage.rivulet import Schema

StreamPosition = int
"""The stream position for creating a consistent ordering of manifests."""
TreeLevel = int
"""The level of the manifest in the LSM-tree."""


class DeltaContext(NamedTuple):
    """Minimal amount of manifest context that may need to be circulated independently or alongside individual files"""

    # Schema needed to understand which field group was added when writing manifest
    # TODO in the future we should use something like a field group id and keep schema in dataset-level metadata
    schema: Schema
    stream_position: StreamPosition
    level: TreeLevel


class RivuletDelta(dict):
    """
    Temporary class during merging of deltacat/rivulet metadata formats

    This class currently serves two purposes:
    1. Avoid big bang refactor in which consumers of RivuletDelta have to update their code to consume deltacat Delta/Manifest
    2. Provide more time to figure out how to represent SST files / schema / etc within deltacat constructs

    """

    context: DeltaContext

    @staticmethod
    def of(delta: Delta) -> RivuletDelta:
        riv_delta = RivuletDelta()
        riv_delta["dcDelta"] = delta
        schema = Schema.from_dict(delta.get("schema"))
        riv_delta["DeltaContext"] = DeltaContext(
            schema, delta.stream_position, delta.get("level")
        )

        return riv_delta

    @property
    def dcDelta(self) -> Delta:
        return self.get("dcDelta")

    @property
    def sst_files(self) -> List[str]:
        if "sst_files" not in self.keys():
            self["sst_files"] = [m.uri for m in self.dcDelta.manifest.entries]
        return self["sst_files"]

    @sst_files.setter
    def sst_files(self, files: List[str]):
        self["sst_files"] = files

    @property
    def context(self) -> DeltaContext:
        return self["DeltaContext"]

    @context.setter
    def context(self, mc: DeltaContext):
        self["DeltaContext"] = mc


class ManifestIO(Protocol):
    """
    Minimal interface for reading and writing manifest files
    """

    def write(
        self,
        sst_files: List[str],
        schema: Schema,
        level: TreeLevel,
    ) -> str:
        ...

    def read(self, file: str) -> RivuletDelta:
        ...


class DeltacatManifestIO(ManifestIO):
    """
    Writes manifest data, but by writing to a Deltacat metastore using Deltacat delta/manifest classes.
    """

    def __init__(self, root: str, locator: PartitionLocator):
        self.root = root
        self.locator = locator

    def write(
        self,
        sst_files: List[str],
        schema: Schema,
        level: TreeLevel,
    ) -> str:
        entry_list = ManifestEntryList()
        """
        Currently, we use the "data files" manifest entry field for SST files
        This is a bit of a hack - we should consider how to better model SST files
        (e.g.: add Manifest entry of type "SST") and decide whether we also need to record data files separately
         even though they're referenced by SST
        Ticket: https://github.com/ray-project/deltacat/issues/469
        """
        for sst_uri in sst_files:
            entry_list.append(
                ManifestEntry.of(
                    url=sst_uri,
                    # TODO have rivulet writer populate these values
                    # see: https://github.com/ray-project/deltacat/issues/476
                    meta=ManifestMeta.of(
                        record_count=None,  # or known
                        content_length=None,
                        content_type=None,
                        content_encoding=None,
                        entry_type=EntryType.DATA,
                    ),
                )
            )
        dc_manifest = Manifest.of(entries=entry_list)

        # Create delta and transaction which writes manifest to root
        # TODO replace this with higher level storage interface for deltacat
        delta_locator = DeltaLocator.at(
            namespace=self.locator.namespace,
            table_name=self.locator.table_name,
            table_version=self.locator.table_version,
            partition_id=self.locator.partition_id,
            partition_values=self.locator.partition_values,
            stream_id=self.locator.stream_id,
            stream_format=StreamFormat.DELTACAT,
            # Using microsecond precision timestamp as stream position
            # TODO consider having storage interface auto assign stream position
            stream_position=time.time_ns(),
        )

        delta = Delta.of(
            locator=delta_locator,
            delta_type=DeltaType.APPEND,
            meta=None,
            properties={},
            manifest=dc_manifest,
        )
        # TODO later support multiple schemas (https://github.com/ray-project/deltacat/issues/468)
        delta["schema"] = schema.to_dict()
        # TODO consider if level should be added as first class key to delta or
        # kept as specific to storage interface
        delta["level"] = level

        tx_results = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=TransactionOperationList.of(
                [
                    TransactionOperation.of(
                        operation_type=TransactionOperationType.CREATE,
                        dest_metafile=delta,
                    )
                ]
            ),
        ).commit(self.root)
        paths = tx_results[0]
        assert (
            len(paths) == 1
        ), "expected delta commit transaction to write exactly 1 metafile"
        return paths[0]

    def read(self, file: str):
        delta = Delta.read(file)
        return RivuletDelta.of(delta)
