from dataclasses import dataclass
from typing import Protocol, Set, NamedTuple
import json
import time

from deltacat.storage import ManifestMeta, EntryType, DeltaLocator, Delta, DeltaType, Transaction, TransactionType, \
    TransactionOperation, TransactionOperationType
from deltacat.storage.model.manifest import Manifest as DCManifest, ManifestEntryList, ManifestEntry
from deltacat.storage.model.metafile import TransactionOperationList

from deltacat.storage.rivulet.fs.input_file import InputFile
from deltacat.storage.rivulet.fs.output_file import OutputFile
from deltacat.storage.rivulet import Schema

StreamPosition = str
"""The stream position for creating a consistent ordering of manifests."""
TreeLevel = int
"""The level of the manifest in the LSM-tree."""


class ManifestContext(NamedTuple):
    """Minimal amount of manifest context that may need to be circulated independently or alongside individual files"""

    # Schema needed to understand which field group was added when writing manifest
    # TODO in the future we should use something like a field group id and keep schema in dataset-level metadata
    schema: Schema
    stream_position: StreamPosition
    level: TreeLevel


@dataclass(frozen=True)
class Manifest:
    """
    Minimal manifest of rivulet data

    Future improvements
    1. We may use deltacat for manifest spec
    2. Manifest may have a lot more metadata, such as:
        Key ranges across SSTs
        other data statistics like record counts
        references to schema id or partition spec id
        stream position or write timestamp
        support for other snapshot types (besides append)
    """
    sst_files: Set[str]
    context: ManifestContext

    def __hash__(self):
        # Generate hash using the name and a frozenset of the items
        return hash(
            (frozenset(self.sst_files), self.context)
        )


class ManifestIO(Protocol):
    """
    Minimal interface for reading and writing manifest files
    """

    def write(
            self,
            file: OutputFile,
            sst_files: Set[str],
            schema: Schema,
            level: TreeLevel,
    ):
        ...

    def read(self, file: InputFile) -> Manifest:
        ...


class DeltacatManifestIO(ManifestIO):
    """
    Writes manifest data, but by writing to a Deltacat metastore using Deltacat delta/manifest classes
    """

    def __init__(self, root: str):
        self.root = root

    def write(
            self,
            file: OutputFile,
            sst_files: Set[str],
            schema: Schema,
            level: TreeLevel,
    ):
        # TODO write interface shoudl populate better metadata

        # Build the Deltacat Manifest entries:
        entry_list = ManifestEntryList()
        # data_files as "DATA" entry type
        for sst_uri in sst_files:
            entry_list.append(ManifestEntry.of(
                url=sst_uri,
                # TODO have rivulet writer populate these values
                meta=ManifestMeta.of(
                    record_count=None,  # or known
                    content_length=None,
                    content_type=None,
                    content_encoding=None,
                    entry_type=EntryType.DATA,
                )
            ))

        dc_manifest = DCManifest.of(entries=entry_list)

        # TODO (discussion) - is it acceptable to just write keys to a manifest?
        # Or do we need to formalize this
        dc_manifest["level"] = level
        dc_manifest["schema"] = schema.to_dict()

        # Create delta and transaction which writes manifest to root
        # Note that deltacat storage currently lets you write deltas not to any parent stream
        # TODO replace this with higher level storage interface for deltacat

        delta_locator = DeltaLocator.at(
            namespace=None,
            table_name=None,
            table_version=None,
            stream_id=None,
            stream_format=None,
            partition_values=None,
            partition_id=None,
            # Using microsecond precision timestamp as stream position
            # TODO discuss how to assign stream position
            stream_position=time.time_ns())

        delta = Delta.of(
            locator=delta_locator,
            delta_type=DeltaType.APPEND,
            meta=None,
            properties={},
            manifest=dc_manifest)

        Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=TransactionOperationList.of([TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=delta,
            )])
        ).commit(self.root)

    def read(self, file: InputFile):
        with file.open() as f:
            data = json.loads(f.read())
        sst_files = data["sst_files"]
        schema = Schema.from_dict(data["schema"])
        stream_position = file.location  # TODO: use the actual stream position
        level = data.get(
            "level", 0
        )  # TODO remove default after fixing assets in ZipperReadExample.zip
        return Manifest(
            set(sst_files),
            ManifestContext(schema, stream_position, level),
        )


class JsonManifestIO(ManifestIO):
    """
    IO for reading and writing manifest in json format

    TODO improve this by allowing it to buffer intermediate state before writing
    """

    def write(
            self,
            file: OutputFile,
            sst_files: Set[str],
            schema: Schema,
            level: TreeLevel,
    ):
        with file.create() as f:
            f.write(
                json.dumps(
                    {
                        "sst_files": list(sst_files),
                        "level": level,
                        "schema": schema.to_dict(),
                    }
                ).encode()
            )

    def read(self, file: InputFile):
        with file.open() as f:
            data = json.loads(f.read())
        sst_files = data["sst_files"]
        schema = Schema.from_dict(data["schema"])
        stream_position = file.location  # TODO: use the actual stream position
        level = data.get(
            "level", 0
        )  # TODO remove default after fixing assets in ZipperReadExample.zip
        return Manifest(
            set(sst_files),
            ManifestContext(schema, stream_position, level),
        )
