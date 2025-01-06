from dataclasses import dataclass
from typing import Protocol, Set, NamedTuple
import json

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

    data_files: Set[str]
    sst_files: Set[str]
    context: ManifestContext

    def __hash__(self):
        # Generate hash using the name and a frozenset of the items
        return hash(
            (frozenset(self.data_files), frozenset(self.sst_files), self.context)
        )


class ManifestIO(Protocol):
    """
    Minimal interface for reading and writing manifest files
    """

    def write(
        self,
        file: OutputFile,
        data_files: Set[str],
        sst_files: Set[str],
        schema: Schema,
        level: TreeLevel,
    ):
        ...

    def read(self, file: InputFile) -> Manifest:
        ...


class JsonManifestIO(ManifestIO):
    """
    IO for reading and writing manifest in json format

    TODO improve this by allowing it to buffer intermediate state before writing
    """

    def write(
        self,
        file: OutputFile,
        data_files: Set[str],
        sst_files: Set[str],
        schema: Schema,
        level: TreeLevel,
    ):
        with file.create() as f:
            f.write(
                json.dumps(
                    {
                        "data_files": list(data_files),
                        "sst_files": list(sst_files),
                        "level": level,
                        "schema": schema.to_dict()
                    }
                ).encode()
            )

    def read(self, file: InputFile):
        with file.open() as f:
            data = json.loads(f.read())
        data_files = data["data_files"]
        sst_files = data["sst_files"]
        schema = Schema.from_dict(data["schema"])
        stream_position = file.location  # TODO: use the actual stream position
        level = data.get(
            "level", 0
        )  # TODO remove default after fixing assets in ZipperReadExample.zip
        return Manifest(
            set(data_files),
            set(sst_files),
            ManifestContext(schema, stream_position, level),
        )
